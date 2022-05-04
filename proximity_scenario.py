import logging

from inspring import scenario_gen_proximity

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    filename='proximity_scenario.log')
LOGGER = logging.getLogger(__name__)


edge_args = {
    'area_to_convert': '20000000.0',
    'base_lulc_path': r"D:\repositories\ci-global-restoration\workspace\data\fc_2019_indonesia_md5_3f6187.tif",
    'convert_farthest_from_edge': False,
    'convert_nearest_to_edge': True,
    'convertible_landcover_codes': '1',
    'focal_landcover_codes': '1',
    'n_fragmentation_steps': '1',
    'replacement_lucode': '0',
    'results_suffix': 'edge',
    'workspace_dir': 'D:/repositories/ci-global-restoration/scenario_gen_workspace',
}

if __name__ == '__main__':
    scenario_gen_proximity.execute(edge_args)
