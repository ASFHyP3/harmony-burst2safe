import argparse
import json
import uuid
from datetime import datetime
from os import makedirs, path

import asf_search
import harmony_service_lib
import pystac
from harmony_service_lib.aws import is_s3
from harmony_service_lib.cli import MultiCatalogLayoutStrategy
from harmony_service_lib.s3_stac_io import S3StacIO
from shapely.geometry import Polygon, box, mapping


CMR_LINK = pystac.Link(rel='harmony_source', target='https://cmr.earthdata.nasa.gov/search/concepts/C2709161906-ASF')
CATALOG_OPTS = dict(stac_extensions=[])


def search_result_to_item(search_result):
    slc, swath, polarization, index = search_result.properties['url'][:-5].split('/')[3:]
    boundary = search_result.umm['SpatialExtent']['HorizontalSpatialDomain']['Geometry']['GPolygons'][0]['Boundary']
    points = [(x['Longitude'], x['Latitude']) for x in boundary['Points']]
    polygon = Polygon(points)
    item = pystac.Item(
        id=search_result.properties['fileID'],
        geometry=mapping(polygon),
        bbox=polygon.bounds,
        datetime=datetime.strptime(search_result.properties['stopTime'], '%Y-%m-%dT%H:%M:%SZ'),
        properties={
            'orbit': int(search_result.properties['orbit']),
            'flight_direction': search_result.properties['flightDirection'].upper(),
            'slc': slc.upper(),
            'swath': swath.upper(),
            'polarization': polarization.upper(),
            'index': int(index),
        },
        assets={
            'data': pystac.Asset(href=search_result.properties['url'], roles=['data']),
            'data1': pystac.Asset(href=search_result.properties['additionalUrls'][0], roles=['data']),
        },
    )
    return item


def get_catalogs(bbox, start_time, stop_time, out_dir=None):
    dataset = asf_search.constants.DATASET.SLC_BURST
    search_results = asf_search.geo_search(
        dataset=dataset,
        intersectsWith=box(*bbox).wkt,
        start=start_time.strftime('%Y-%m-%d'),
        end=stop_time.strftime('%Y-%m-%d'),
    )
    # FIXME: limiting to one item for now
    items = [[search_result_to_item(search_result) for search_result in search_results][0]]
    catalogs = []
    catalog_names = []
    for i, item in enumerate(items):
        description = f'SLC for absoulte orbit {item.properties["orbit"]}'
        catalog = pystac.Catalog(id=str(uuid.uuid4()), description=description, **CATALOG_OPTS)
        catalog.add_link(CMR_LINK)
        catalog.add_items([item])
        catalogs.append(catalog)
        if out_dir:
            catalog.normalize_and_save(str(out_dir), pystac.CatalogType.SELF_CONTAINED, MultiCatalogLayoutStrategy(i))
            catalog_names.append(f'catalog{i}.json')

    if out_dir:
        with open(out_dir / 'batch-catalogs.json', 'w') as f:
            json.dump(catalog_names, f)
        with open(out_dir / 'batch-count.txt', 'w') as f:
            f.write(str(len(catalog_names)))

    return catalogs


def stage_catalogs(catalogs, metadata_dir):
    s3_io = S3StacIO()
    is_s3_metadata_dir = is_s3(metadata_dir)
    if not is_s3_metadata_dir:
        makedirs(metadata_dir, exist_ok=True)

    updated_catalogs = []
    for idx, catalog in enumerate(catalogs):
        catalog.normalize_and_save(metadata_dir, pystac.CatalogType.SELF_CONTAINED, MultiCatalogLayoutStrategy(idx))
        updated_catalogs.append(catalog)

    json_str = json.dumps([f'catalog{i}.json' for i, c in enumerate(catalogs)])
    s3_io.write_text(path.join(metadata_dir, 'batch-catalogs.json'), json_str)
    s3_io.write_text(path.join(metadata_dir, 'batch-count.txt'), f'{len(catalogs)}')
    return updated_catalogs


def get_metadata_dir(staging_location):
    s3, slash, bucket, _, _, _, request_id, job_id, slash2 = staging_location.split('/')
    metadata_dir = '/'.join([s3, slash, bucket, request_id, job_id, slash2, 'outputs'])
    return metadata_dir


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
        self.logger.info(self.config)

        self.logger.info(str(self.message.subset.bbox))
        bbox = list(self.message.subset.bbox)
        self.logger.info(str(self.message.temporal.start))
        start_time = datetime.strptime(self.message.temporal.start, '%Y-%m-%dT%H:%M:%SZ')
        self.logger.info(str(self.message.temporal.end))
        stop_time = datetime.strptime(self.message.temporal.end, '%Y-%m-%dT%H:%M:%SZ')

        self.catalog = get_catalogs(bbox, start_time, stop_time)
        metadata_dir = get_metadata_dir(self.message.stagingLocation)
        self.catalog = stage_catalogs(self.catalog, metadata_dir)
        self.is_complete = True
        return (self.message, self.catalog)


def main() -> None:
    parser = argparse.ArgumentParser(description='Query CMR for Sentinel-1 burst records and transform to safe items')
    harmony_service_lib.setup_cli(parser)
    args = parser.parse_args()
    harmony_service_lib.run_cli(parser, args, HarmonyAdapter)


if __name__ == '__main__':
    main()
