"""Pipeline to manage all data and models."""
import argparse
import logging
import os

from osgeo import gdal
from ecoshard import geoprocessing
import numpy
import taskgraph

gdal.SetCacheMax(2**30)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
logging.getLogger('taskgraph').setLevel(logging.WARN)
LOGGER = logging.getLogger(__name__)

WORKSPACE_DIR = 'ci_pipeline_workspace'

DATA_URL_MAP = {
    'dem_raster': '',
}


def _fetch_data(url_map, data_dir):
    """Fetch data in url_map and copy to data_dir.

    Return:
        dict mapping `url_map` keys to actual file locations.
    """
    data_map


def _run_sdr(data_map):
    """Run SDR pipeline.

    Args:
        data_map (dict): maps globally known keys to raster paths

    """
    pass


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description='manage CI pipeline')
    args = parser.parse_args()

    os.makedirs(WORKSPACE_DIR, exist_ok=True)
    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)

    task_graph.join()
    task_graph.close()


if __name__ == '__main__':
    main()
