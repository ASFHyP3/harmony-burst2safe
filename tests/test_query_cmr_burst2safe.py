from datetime import datetime
from pathlib import Path

import harmony_burst2safe.query_cmr_burst2safe as qcmr


BBOX = [-83.71, 43.66, -83.69, 43.67]
START_TIME = datetime(2024, 10, 9)
STOP_TIME = datetime(2024, 10, 11)


def test_get_catalogs():
    out_dir = Path('.')
    qcmr.get_catalogs(BBOX, START_TIME, STOP_TIME, out_dir)
