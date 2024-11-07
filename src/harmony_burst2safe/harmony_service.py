import argparse
import tempfile
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile

import asf_search
import harmony_service_lib
import pystac
from burst2safe.local2safe import local2safe
from shapely.geometry import Polygon, box, mapping
from shapely.ops import unary_union


@dataclass
class Burst:
    id: str
    flight_direction: str
    orbit: int
    slc: str
    swath: str
    polarization: str
    index: int
    datetime: datetime
    footprint: Polygon
    bbox: Iterable[int]
    data_url: str
    metadata_url: str


def nested_defaultdict():
    return defaultdict(nested_defaultdict)


def parse_search_result(search_result):
    slc, swath, polarization, index = search_result.properties['url'][:-5].split('/')[3:]
    footprint = search_result.umm['SpatialExtent']['HorizontalSpatialDomain']['Geometry']['GPolygons'][0]['Boundary']
    points = [(x['Longitude'], x['Latitude']) for x in footprint['Points']]
    polygon = Polygon(points)
    burst = Burst(
        id=search_result.properties['fileID'],
        flight_direction=search_result.properties['flightDirection'].upper(),
        orbit=int(search_result.properties['orbit']),
        slc=slc.upper(),
        swath=swath.upper(),
        polarization=polarization.upper(),
        index=int(index),
        datetime=datetime.strptime(search_result.properties['stopTime'], '%Y-%m-%dT%H:%M:%SZ'),
        footprint=polygon,
        bbox=polygon.bounds,
        data_url=search_result.properties['url'],
        metadata_url=search_result.properties['additionalUrls'][0],
    )
    return burst


def search_results_to_items(search_results):
    bursts = [parse_search_result(search_result) for search_result in search_results]
    items = []
    for orbit in list(set([burst.orbit for burst in bursts])):
        slc_group = [burst for burst in bursts if burst.orbit == orbit]
        full_footprint = box(*unary_union([burst.footprint for burst in slc_group]).bounds)

        assets = {}
        for burst in bursts:
            key_suffix = f'{burst.slc}/{burst.swath}/{burst.polarization}/{burst.index}'
            assets[f'{key_suffix}/DATA'] = pystac.Asset(href=burst.data_url, roles=['data'])
            assets[f'{key_suffix}/METADATA'] = pystac.Asset(href=burst.metadata_url, roles=['data'])

        item = pystac.Item(
            id=f'SLC-{orbit}',
            geometry=mapping(full_footprint),
            bbox=full_footprint.bounds,
            datetime=min([burst.datetime for burst in slc_group]),
            properties={
                'orbit': slc_group[0].orbit,
                'flight_direction': slc_group[0].flight_direction,
                'n_bursts': len(slc_group),
            },
            assets=assets,
        )
        items.append(item)
    return items


def get_catalog(bbox, start_time, stop_time, out_dir=None):
    dataset = asf_search.constants.DATASET.SLC_BURST
    search_results = asf_search.geo_search(
        dataset=dataset,
        intersectsWith=box(*bbox).wkt,
        start=start_time.strftime('%Y-%m-%d'),
        end=stop_time.strftime('%Y-%m-%d'),
    )
    items = search_results_to_items(search_results)
    catalog = pystac.Catalog(id=str(uuid.uuid4()), description='SLC burst groups', stac_extensions=[])
    source = pystac.Link(rel='harmony_source', target='https://cmr.earthdata.nasa.gov/search/concepts/C2709161906-ASF')
    catalog.add_link(source)
    catalog.add_items(items)
    return catalog


def make_zip(source_dir_path, compress=False):
    source_dir_path = Path(source_dir_path).resolve()
    zip_path = source_dir_path.with_suffix('.zip')
    compress = ZIP_DEFLATED if compress else ZIP_STORED
    with ZipFile(str(zip_path), 'w', compression=compress) as zipf:
        paths = [x.relative_to(source_dir_path.parent) for x in source_dir_path.rglob('*')]
        for file_path in paths:
            zipf.write(source_dir_path.parent / file_path, file_path)
    return zip_path


class HarmonyAdapter(harmony_service_lib.BaseHarmonyAdapter):
    def invoke(self):
        """
        Invokes the service to process `self.message`.  By default, this will call process_item
        on all items in the input catalog

        Returns
        -------
        (harmony_service_lib.Message, pystac.Catalog | list)
            A tuple of the Harmony message, with any processed fields marked as such and
            in this implementation, a single STAC catalog describing the output.
            (Services overriding this method may return a list of STAC catalogs if desired.)
        """
        self.logger.info('Logging from invoke!')
        self.logger.info('Message:')
        self.logger.info(str(self.message))
        self.logger.info('Config:')
        self.config = self.config._replace(max_download_retries=5)
        self.logger.info(self.config)

        self.logger.info(str(self.message.subset.bbox))
        bbox = list(self.message.subset.bbox)
        self.logger.info(str(self.message.temporal.start))
        start_time = datetime.strptime(self.message.temporal.start, '%Y-%m-%dT%H:%M:%SZ')
        self.logger.info(str(self.message.temporal.end))
        stop_time = datetime.strptime(self.message.temporal.end, '%Y-%m-%dT%H:%M:%SZ')

        self.catalog = get_catalog(bbox, start_time, stop_time)
        self.logger.info('Catalog:')
        self.logger.info(self.catalog.to_dict())

        result = self._process_catalog_recursive(self.catalog)
        self.is_complete = True
        return (self.message, result)

    def process_item(self, item: pystac.Item, source: harmony_service_lib.message.Source | None = None) -> pystac.Item:
        """
        Processes a single input item.

        Parameters
        ----------
        item : pystac.Item
            the item that should be processed
        source : harmony_service_lib.message.Source
            the input source defining the variables, if any, to subset from the item

        Returns
        -------
        pystac.Item
            a STAC catalog whose metadata and assets describe the service output
        """
        self.logger.info(f'Processing item {item.id}')
        self.logger.info(str(item.to_dict()))
        with tempfile.TemporaryDirectory() as temp_dir:
            self.logger.info('Downloading assets')
            slc_tree = nested_defaultdict()
            for key, asset in item.assets.items():
                filename = harmony_service_lib.util.download(
                    url=asset.href,
                    destination_dir=temp_dir,
                    logger=self.logger,
                    access_token=self.message.accessToken,
                    cfg=self.config,
                )
                slc, swath, polarization, index, url_type = key.split('/')
                slc_tree[slc][swath][polarization][index][url_type] = filename

            self.logger.info('Creating SAFE')
            safe_path = local2safe(slc_dict=slc_tree, work_dir=Path(temp_dir))
            safe_path = make_zip(safe_path)
            self.logger.info(f'Staging {safe_path}')
            url = harmony_service_lib.util.stage(
                local_filename=str(safe_path),
                remote_filename=safe_path.name,
                mime='application/zip',
                location=self.message.stagingLocation,
                logger=self.logger,
                cfg=self.config,
            )
            result = item.clone()
            result.assets = {
                'safe': pystac.Asset(url, title=safe_path.name, media_type='application/zip', roles=['data'])
            }
            self.logger.info(str(result.to_dict()))
            self.logger.info('Done')
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description='Run the Harmony service')
    harmony_service_lib.setup_cli(parser)
    args = parser.parse_args()
    harmony_service_lib.run_cli(parser, args, HarmonyAdapter)


if __name__ == '__main__':
    main()
