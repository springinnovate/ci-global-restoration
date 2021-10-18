"""Entry point to manage data and run pipeline."""
import glob
import itertools
import logging
import multiprocessing
import os
import shutil

from ecoshard import taskgraph
from osgeo import gdal
import ecoshard
import requests


gdal.SetCacheMax(2**30)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
logging.getLogger('ecoshard.taskgraph').setLevel(logging.DEBUG)
logging.getLogger('ecoshard.ecoshard').setLevel(logging.INFO)
logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)

LOGGER = logging.getLogger(__name__)

WORKSPACE_DIR = 'workspace'

ECOSHARD_MAP = {
    'ESA_LULC': 'https://storage.googleapis.com/ecoshard-root/esa_lulc_smoothed/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif',
    'Scenario1_LULC': 'None',
    'Biophysical table': 'None',
    'DEM': 'https://storage.googleapis.com/global-invest-sdr-data/global_dem_3s_md5_22d0c3809af491fa09d03002bdf09748.zip',
    'Erosivity': 'https://storage.googleapis.com/global-invest-sdr-data/GlobalR_NoPol_compressed_md5_49734c4b1c9c94e49fffd0c39de9bf0c.tif',
    'Erodibility': 'https://storage.googleapis.com/ecoshard-root/pasquale/Kfac_SoilGrid1km_GloSEM_v1.1_md5_e1c74b67ad7fdaf6f69f1f722a5c7dfb.tif',
    'Watersheds': 'https://storage.googleapis.com/global-invest-sdr-data/watersheds_globe_HydroSHEDS_15arcseconds_md5_c6acf2762123bbd5de605358e733a304.zip',
    'Precipitation': 'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/worldclim_2015_md5_16356b3770460a390de7e761a27dbfa1.tif',
    'Fertilizer': 'https://storage.googleapis.com/nci-ecoshards/scenarios050420/NCI_Ext_RevB_add_backgroundN_md5_e4a9cc537cd0092d346e4287e7bd4c36.tif',
    'Global polygon': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_global_polygon_simplified_geometries_md5_653118dde775057e24de52542b01eaee.gpkg',
    'Buffered shore': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/buffered_global_shore_5km_md5_a68e1049c1c03673add014cd29b7b368.gpkg',
    'Shore grid': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/shore_grid_md5_07aea173cf373474c096f1d5e3463c2f.gpkg',
    'Waves': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/wave_watch_iii_md5_c8bb1ce4739e0a27ee608303c217ab5b.gpkg.gz',
    'Coastal DEM': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/global_dem_md5_22c5c09ac4c4c722c844ab331b34996c.tif',
    'SLR': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/MSL_Map_MERGED_Global_AVISO_NoGIA_Adjust_md5_3072845759841d0b2523d00fe9518fee.tif',
    'Geomorphology': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/geomorphology_md5_e65eff55840e7a80cfcb11fdad2d02d7.gpkg',
    'Coastal habitat: reef': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_reef_wgs84_compressed_md5_96d95cc4f2c5348394eccff9e8b84e6b.tif',
    'Coastal habitat: mangrove': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_mangrove_md5_0ec85cb51dab3c9ec3215783268111cc.tif',
    'Coastal habitat: seagrass': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_seagrass_md5_a9cc6d922d2e74a14f74b4107c94a0d6.tif',
    'Coastal habitat: saltmarsh': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_saltmarsh_md5_203d8600fd4b6df91f53f66f2a011bcd.tif',
    'Pollination-dependent yield': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/monfreda_2008_yield_poll_dep_ppl_fed_5min.tif',
    'Population': 'https://storage.googleapis.com/ecoshard-root/population/lspop2019_compressed_md5_d0bf03bd0a2378196327bbe6e898b70c.tif',
    'Friction surface': 'https://storage.googleapis.com/ecoshard-root/critical_natural_capital/friction_surface_2015_v1.0-002_md5_166d17746f5dd49cfb2653d721c2267c.tif',
    'World borders': 'https://storage.googleapis.com/ecoshard-root/critical_natural_capital/TM_WORLD_BORDERS-0.3_simplified_md5_47f2059be8d4016072aa6abe77762021.gpkg',
    'Habitat mask ESA': '(need to make from LULC above)',
    'Habitat mask Scenario1': '(need to make from LULC above)',
    'Coastal population': '(need to make from population above and this mask: https://storage.googleapis.com/ecoshard-root/ipbes-cv/total_pop_masked_by_10m_md5_ef02b7ee48fa100f877e3a1671564be2.tif)',
    'Coastal habitat masks ESA': '(will be outputs of CV)',
    'Coastal habitat masks Scenario 1': '(will be outputs of CV)',
    }


def _flatten_dir(working_dir):
    """Move all files in subdirectory to `working_dir`."""
    all_files = []
    # itertools lets us skip the first iteration (current dir)
    for root, _dirs, files in itertools.islice(os.walk(working_dir), 1, None):
        for filename in files:
            all_files.append(os.path.join(root, filename))
    for filename in all_files:
        shutil.move(filename, working_dir)


def _unpack_and_vrt_tiles(
        zip_path, unpack_dir, target_nodata, target_vrt_path):
    """Unzip multi-file of tiles and create VRT.

    Args:
        zip_path (str): path to zip file of tiles
        unpack_dir (str): path to directory to unpack tiles
        target_vrt_path (str): desired target path for VRT.

    Returns:
        None
    """
    shutil.unpack_archive(zip_path, unpack_dir)
    _flatten_dir(unpack_dir)
    base_raster_path_list = glob.glob(os.path.join(unpack_dir, '*.tif'))
    vrt_options = gdal.BuildVRTOptions(VRTNodata=target_nodata)
    gdal.BuildVRT(
        target_vrt_path, base_raster_path_list, options=vrt_options)
    target_dem = gdal.OpenEx(target_vrt_path, gdal.OF_RASTER)
    if target_dem is None:
        raise RuntimeError(
            f"didn't make VRT at {target_vrt_path} on: {zip_path}")


def _download_and_validate(url, target_path):
    """Download an ecoshard and validate its hash."""
    ecoshard.download_url(url, target_path)
    if not ecoshard.validate(target_path):
        raise ValueError(f'{target_path} did not validate on its hash')


def fetch_data(ecoshard_map, data_dir):
    """Download data in `ecoshard_map` and replace urls with targets.

    Any values that are not urls are kept and a warning is logged.

    Args:
        ecoshard_map (dict): key/value pairs where if value is a url that
            file is downloaded and verified against its hash.
        data_dir (str): path to a directory to store downloaded data.

    Returns:
        dict of {value: filepath} map where `filepath` is the path to the
            downloaded file stored in `data_dir`. If the original value was
            not a url it is copied as-is.
    """
    task_graph = taskgraph.TaskGraph(
        data_dir, multiprocessing.cpu_count(), parallel_mode='thread')
    data_map = {}
    for key, value in ecoshard_map.items():
        if value.startswith('http'):
            response = requests.head(value)
            if response:
                target_path = os.path.join(data_dir, os.path.basename(value))
                task_graph.add_task(
                    func=ecoshard.download_url,
                    args=(value, target_path),
                    target_path_list=[target_path],
                    task_name=f'download {value}')
                data_map[value] = target_path
            else:
                LOGGER.warning(f'{key}: {value} does not refer to a url')
                data_map[key] = value
        else:
            data_map[key] = value
    LOGGER.info('waiting for downloads to complete')
    task_graph.close()
    task_graph.join()
    task_graph = None
    return data_map


def main():
    """Entry point."""
    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)
    data_dir = os.path.join(WORKSPACE_DIR, 'data')
    LOGGER.info('downloading data')
    fetch_task = task_graph.add_task(
        func=fetch_data,
        args=(ECOSHARD_MAP, data_dir),
        store_result=True,
        task_name='download ecoshards')
    file_map = fetch_task.get()
    LOGGER.info('downloaded data')
    dem_dir = os.path.join(data_dir, 'dem')
    dem_vrt_path = os.path.join(dem_dir, 'dem.vrt')
    LOGGER.info('unpack dem')
    _unpack_and_vrt_tiles(file_map['DEM'], dem_dir, -9999, dem_vrt_path)


if __name__ == '__main__':
    main()
