"""Entry point to manage data and run pipeline."""
from datetime import datetime
import collections
import glob
import gzip
import itertools
import logging
import multiprocessing
import os
import shutil
import threading
import time

from inspring import sdr_c_factor
from inspring import ndr_mfd_plus
from ecoshard import geoprocessing
from ecoshard import taskgraph
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import ecoshard
import requests


gdal.SetCacheMax(2**26)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
logging.getLogger('ecoshard.taskgraph').setLevel(logging.INFO)
logging.getLogger('ecoshard.ecoshard').setLevel(logging.INFO)
logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
logging.getLogger('ecoshard.geoprocessing.geoprocessing').setLevel(
    logging.ERROR)
logging.getLogger('ecoshard.geoprocessing.routing.routing').setLevel(
    logging.WARNING)
logging.getLogger('ecoshard.geoprocessing.geoprocessing_core').setLevel(
    logging.ERROR)
logging.getLogger('inspring.sdr_c_factor').setLevel(logging.WARNING)
logging.getLogger('inspring.ndr_mfd_plus').setLevel(logging.WARNING)

LOGGER = logging.getLogger(__name__)

WORKSPACE_DIR = 'workspace'
SDR_WORKSPACE_DIR = os.path.join(WORKSPACE_DIR, 'sdr_workspace')
NDR_WORKSPACE_DIR = os.path.join(WORKSPACE_DIR, 'ndr_workspace')
WATERSHED_SUBSET_TOKEN_PATH = os.path.join(
    WORKSPACE_DIR, 'watershed_partition.token')

# how many jobs to hold back before calling stitcher
N_TO_BUFFER_STITCH = 10

TARGET_PIXEL_SIZE_M = 300  # pixel size in m when operating on projected data
GLOBAL_PIXEL_SIZE_DEG = 10/3600  # 10s resolution
GLOBAL_BB = [-180, -60, 180, 60]

# These SDR constants are what we used as the defaults in a previous project
THRESHOLD_FLOW_ACCUMULATION = 1000
L_CAP = 122
K_PARAM = 2
SDR_MAX = 0.8
IC_0_PARAM = 0.5

DEM_KEY = 'dem'
EROSIVITY_KEY = 'erosivity'
ERODIBILITY_KEY = 'erodibility'
ESA_LULC_KEY = 'esa_lulc'
SCENARIO_1_LULC_KEY = 'scenario_1_lulc'
SCENARIO_1_V2_LULC_KEY = 'scenario_1_v2_lulc'
SDR_BIOPHYSICAL_TABLE_KEY = 'sdr_biophysical_table'
WATERSHEDS_KEY = 'watersheds'
WAVES_KEY = 'waves'
SDR_BIOPHYSICAL_TABLE_LUCODE_KEY = 'ID'
NDR_BIOPHYSICAL_TABLE_LUCODE_KEY = 'Value'

RUNOFF_PROXY_KEY = 'Precipitation'
FERTILZER_KEY = 'Fertilizer'
NDR_BIOPHYSICAL_TABLE_KEY = 'ndr_biophysical_table'

ECOSHARD_MAP = {
    ESA_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/esa_lulc_smoothed/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif',
    SCENARIO_1_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/restoration_pnv0.0001_on_ESA2020_clip_md5_93d43b6124c73cb5dc21698ea5f9c8f4.tif',
    SCENARIO_1_V2_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/restoration_pnv0.0001_on_ESA2020_v2_md5_5530ea58dad595519c69d2ae67d61908.tif',
    SDR_BIOPHYSICAL_TABLE_KEY: 'https://storage.googleapis.com/global-invest-sdr-data/Biophysical_table_ESA_ARIES_RS_md5_e16587ebe01db21034ef94171c76c463.csv',
    NDR_BIOPHYSICAL_TABLE_KEY: 'https://storage.googleapis.com/nci-ecoshards/nci-NDR-biophysical_table_ESA_ARIES_RS3_md5_74d69f7e7dc829c52518f46a5a655fb8.csv',
    DEM_KEY: 'https://storage.googleapis.com/global-invest-sdr-data/global_dem_3s_md5_22d0c3809af491fa09d03002bdf09748.zip',
    EROSIVITY_KEY: 'https://storage.googleapis.com/global-invest-sdr-data/GlobalR_NoPol_compressed_md5_49734c4b1c9c94e49fffd0c39de9bf0c.tif',
    ERODIBILITY_KEY: 'https://storage.googleapis.com/ecoshard-root/pasquale/Kfac_SoilGrid1km_GloSEM_v1.1_md5_e1c74b67ad7fdaf6f69f1f722a5c7dfb.tif',
    WATERSHEDS_KEY: 'https://storage.googleapis.com/global-invest-sdr-data/watersheds_globe_HydroSHEDS_15arcseconds_md5_c6acf2762123bbd5de605358e733a304.zip',
    RUNOFF_PROXY_KEY: 'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/worldclim_2015_md5_16356b3770460a390de7e761a27dbfa1.tif',
    FERTILZER_KEY: 'https://storage.googleapis.com/nci-ecoshards/scenarios050420/NCI_Ext_RevB_add_backgroundN_md5_e4a9cc537cd0092d346e4287e7bd4c36.tif',
    'Global polygon': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_global_polygon_simplified_geometries_md5_653118dde775057e24de52542b01eaee.gpkg',
    'Buffered shore': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/buffered_global_shore_5km_md5_a68e1049c1c03673add014cd29b7b368.gpkg',
    'Shore grid': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/shore_grid_md5_07aea173cf373474c096f1d5e3463c2f.gpkg',
    WAVES_KEY: 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/wave_watch_iii_md5_c8bb1ce4739e0a27ee608303c217ab5b.gpkg.gz',
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
    if not os.path.exists(target_vrt_path):
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
        data_dir, multiprocessing.cpu_count(), parallel_mode='thread',
        taskgraph_name='fetch data')
    data_map = {}
    for key, value in ecoshard_map.items():
        if value.startswith('http'):
            response = requests.head(value)
            if response:
                target_path = os.path.join(data_dir, os.path.basename(value))
                if not os.path.exists(target_path):
                    task_graph.add_task(
                        func=ecoshard.download_url,
                        args=(value, target_path),
                        target_path_list=[target_path],
                        task_name=f'download {value}')
                data_map[key] = target_path
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


def _unpack_archive(archive_path, dest_dir):
    """Unpack archive to dest_dir."""
    if archive_path.endswith('.gz'):
        with gzip.open(archive_path, 'r') as f_in:
            dest_path = os.path.join(
                dest_dir, os.path.basename(os.path.splitext(archive_path)[0]))
            with open(dest_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        shutil.unpack_archive(archive_path, dest_dir)


def fetch_and_unpack_data(task_graph):
    """Fetch & unpack data subroutine."""
    data_dir = os.path.join(WORKSPACE_DIR, 'data')
    LOGGER.info('downloading data')
    fetch_task = task_graph.add_task(
        func=fetch_data,
        args=(ECOSHARD_MAP, data_dir),
        store_result=True,
        task_name='download ecoshards')
    file_map = fetch_task.get()
    LOGGER.debug(file_map)
    LOGGER.info('downloaded data')
    dem_dir = os.path.join(data_dir, DEM_KEY)
    dem_vrt_path = os.path.join(dem_dir, 'dem.vrt')
    LOGGER.info('unpack dem')
    _ = task_graph.add_task(
        func=_unpack_and_vrt_tiles,
        args=(file_map[DEM_KEY], dem_dir, -9999, dem_vrt_path),
        target_path_list=[dem_vrt_path],
        task_name=f'unpack {file_map[DEM_KEY]}')
    file_map[DEM_KEY] = dem_vrt_path
    for compressed_id in [WAVES_KEY, WATERSHEDS_KEY]:
        _ = task_graph.add_task(
            func=_unpack_archive,
            args=(file_map[compressed_id], data_dir),
            task_name=f'decompress {file_map[compressed_id]}')
        file_map[compressed_id] = data_dir
    LOGGER.debug('wait for unpack')
    task_graph.join()

    # just need the base directory for watersheds
    file_map[WATERSHEDS_KEY] = os.path.join(
        file_map[WATERSHEDS_KEY], 'watersheds_globe_HydroSHEDS_15arcseconds')

    # only need to lose the .gz on the waves
    file_map[WAVES_KEY] = os.path.splitext(file_map[WAVES_KEY])

    return file_map


def _batch_into_watershed_subsets(
        watershed_root_dir, degree_separation, done_token_path,
        watershed_subset=None):
    """Construct geospatially adjacent subsets.

    Breaks watersheds up into geospatially similar watersheds and limits
    the upper size to no more than specified area. This allows for a
    computationally efficient batch to run on a large contiguous area in
    parallel while avoiding batching watersheds that are too small.

    Args:
        watershed_root_dir (str): path to watershed .shp files.
        degree_separation (int): a blocksize number of degrees to coalasce
            watershed subsets into.
        done_token_path (str): path to file to write when function is
            complete, indicates for batching that the task is complete.
        watershed_subset (dict): if not None, keys are watershed basefile
            names and values are FIDs to select. If present the simulation
            only constructs batches from these watershed/fids, otherwise
            all watersheds are run.

    Returns:
        list of (job_id, watershed.gpkg) tuples where the job_id is a
        unique identifier for that subwatershed set and watershed.gpkg is
        a subset of the original global watershed files.

    """
    # ensures we don't have more than 1000 watersheds per job
    task_graph = taskgraph.TaskGraph(
        watershed_root_dir, multiprocessing.cpu_count(), 10,
        taskgraph_name='batch watersheds')
    watershed_path_list = []
    job_id_set = set()
    for watershed_path in glob.glob(
            os.path.join(watershed_root_dir, '*.shp')):
        LOGGER.debug(f'scheduling {os.path.basename(watershed_path)}')
        subbatch_job_index_map = collections.defaultdict(int)
        # lambda describes the FIDs to process per job, the list of lat/lng
        # bounding boxes for each FID, and the total degree area of the job
        watershed_fid_index = collections.defaultdict(
            lambda: [list(), list(), 0])
        watershed_basename = os.path.splitext(
            os.path.basename(watershed_path))[0]
        watershed_ids = None
        watershed_vector = gdal.OpenEx(watershed_path, gdal.OF_VECTOR)
        watershed_layer = watershed_vector.GetLayer()

        if watershed_subset:
            if watershed_basename not in watershed_subset:
                continue
            else:
                # just grab the subset
                watershed_ids = watershed_subset[watershed_basename]
                watershed_layer = [
                    watershed_layer.GetFeature(fid) for fid in watershed_ids]

        # watershed layer is either the layer or a list of features
        for watershed_feature in watershed_layer:
            fid = watershed_feature.GetFID()
            watershed_geom = watershed_feature.GetGeometryRef()
            watershed_centroid = watershed_geom.Centroid()
            epsg = geoprocessing.get_utm_zone(
                watershed_centroid.GetX(), watershed_centroid.GetY())
            if watershed_geom.Area() > 1 or watershed_ids:
                # one degree grids or immediates get special treatment
                job_id = (f'{watershed_basename}_{fid}', epsg)
                watershed_fid_index[job_id][0] = [fid]
            else:
                # clamp into degree_separation squares
                x, y = [
                    int(v//degree_separation)*degree_separation for v in (
                        watershed_centroid.GetX(), watershed_centroid.GetY())]
                base_job_id = f'{watershed_basename}_{x}_{y}'
                # keep the epsg in the string because the centroid might lie
                # on a different boundary
                job_id = (f'''{base_job_id}_{
                    subbatch_job_index_map[base_job_id]}_{epsg}''', epsg)
                if len(watershed_fid_index[job_id][0]) > 1000:
                    subbatch_job_index_map[base_job_id] += 1
                    job_id = (f'''{base_job_id}_{
                        subbatch_job_index_map[base_job_id]}_{epsg}''', epsg)
                watershed_fid_index[job_id][0].append(fid)
            watershed_envelope = watershed_geom.GetEnvelope()
            watershed_bb = [watershed_envelope[i] for i in [0, 2, 1, 3]]
            watershed_fid_index[job_id][1].append(watershed_bb)
            watershed_fid_index[job_id][2] += watershed_geom.Area()

        watershed_geom = None
        watershed_feature = None

        watershed_subset_dir = os.path.join(
            watershed_root_dir, 'watershed_subsets')
        os.makedirs(watershed_subset_dir, exist_ok=True)

        for (job_id, epsg), (fid_list, watershed_envelope_list, area) in \
                sorted(
                    watershed_fid_index.items(), key=lambda x: x[1][-1],
                    reverse=True):
            if job_id in job_id_set:
                raise ValueError(f'{job_id} already processed')
            job_id_set.add(job_id)

            watershed_subset_path = os.path.join(
                watershed_subset_dir, f'{job_id}_a{area:.3f}.gpkg')
            if not os.path.exists(watershed_subset_path):
                task_graph.add_task(
                    func=_create_fid_subset,
                    args=(
                        watershed_path, fid_list, epsg, watershed_subset_path),
                    target_path_list=[watershed_subset_path],
                    task_name=job_id)
            watershed_path_list.append(watershed_subset_path)

        watershed_layer = None
        watershed_vector = None

    task_graph.join()
    task_graph.close()
    task_graph = None

    with open(done_token_path, 'w') as token_file:
        token_file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return watershed_path_list


def _create_fid_subset(
        base_vector_path, fid_list, target_epsg, target_vector_path):
    """Create subset of vector that matches fid list, projected into epsg."""
    vector = gdal.OpenEx(base_vector_path, gdal.OF_VECTOR)
    layer = vector.GetLayer()
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(target_epsg)
    layer.SetAttributeFilter(
        f'"FID" in ('
        f'{", ".join([str(v) for v in fid_list])})')
    feature_count = layer.GetFeatureCount()
    gpkg_driver = ogr.GetDriverByName('gpkg')
    unprojected_vector_path = '%s_wgs84%s' % os.path.splitext(
        target_vector_path)
    subset_vector = gpkg_driver.CreateDataSource(unprojected_vector_path)
    subset_vector.CopyLayer(
        layer, os.path.basename(os.path.splitext(target_vector_path)[0]))
    geoprocessing.reproject_vector(
        unprojected_vector_path, srs.ExportToWkt(), target_vector_path,
        driver_name='gpkg', copy_fields=False)
    subset_vector = None
    layer = None
    vector = None
    gpkg_driver.DeleteDataSource(unprojected_vector_path)
    target_vector = gdal.OpenEx(target_vector_path, gdal.OF_VECTOR)
    target_layer = target_vector.GetLayer()
    if feature_count != target_layer.GetFeatureCount():
        raise ValueError(
            f'expected {feature_count} in {target_vector_path} but got '
            f'{target_layer.GetFeatureCount()}')


def _run_sdr(
        workspace_dir,
        watershed_path_list,
        dem_path,
        erosivity_path,
        erodibility_path,
        lulc_path,
        target_pixel_size,
        biophysical_table_path,
        biophysical_table_lucode_field,
        threshold_flow_accumulation,
        l_cap,
        k_param,
        sdr_max,
        ic_0_param,
        target_stitch_raster_map,
        keep_intermediate_files=False,
        c_factor_path=None,
        result_prefix=None,
        ):
    """Run SDR component of the pipeline.

    This function will iterate through the watershed subset list, run the SDR
    model on those subwatershed regions, and stitch those data back into a
    global raster.

    Args:
        workspace_dir (str): path to directory to do all work
        watershed_path_list (list): list of watershed vector files to
            operate on locally. The base filenames are used as the workspace
            directory path.
        dem_path (str): path to global DEM raster
        erosivity_path (str): path to global erosivity raster
        erodibility_path (str): path to erodability raster
        lulc_path (str): path to landcover raster
        target_pixel_size (float): target projected pixel unit size
        biophysical_table_lucode_field (str): name of the lucode field in
            the biophysical table column
        threshold_flow_accumulation (float): flow accumulation threshold
            to use to calculate streams.
        l_cap (float): upper limit to the L factor
        k_param (float): k parameter in SDR model
        sdr_max (float): max SDR value
        ic_0_param (float): IC0 constant in SDR model
        target_stitch_raster_map (dict): maps the local path of an output
            raster of this model to an existing global raster to stich into.
        keep_intermediate_files (bool): if True, the intermediate watershed
            workspace created underneath `workspace_dir` is deleted.
        c_factor_path (str): optional, path to c factor that's used for lucodes
            that use the raster
        result_prefix (str): optional, prepended to the global stitch results.

    Returns:
        None.
    """
    # create global stitch rasters and start workers
    task_graph = taskgraph.TaskGraph(
        workspace_dir, multiprocessing.cpu_count(), 10,
        parallel_mode='process', taskgraph_name='sdr processor')
    stitch_raster_queue_map = {}
    stitch_worker_list = []
    multiprocessing_manager = multiprocessing.Manager()
    signal_done_queue = multiprocessing_manager.Queue()
    for local_result_path, global_stitch_raster_path in \
            target_stitch_raster_map.items():
        if result_prefix is not None:
            global_stitch_raster_path = (
                f'%s_{result_prefix}%s' % os.path.splitext(
                    global_stitch_raster_path))
        if not os.path.exists(global_stitch_raster_path):
            LOGGER.info(f'creating {global_stitch_raster_path}')
            driver = gdal.GetDriverByName('GTiff')
            n_cols = int((GLOBAL_BB[2]-GLOBAL_BB[0])/GLOBAL_PIXEL_SIZE_DEG)
            n_rows = int((GLOBAL_BB[3]-GLOBAL_BB[1])/GLOBAL_PIXEL_SIZE_DEG)
            LOGGER.info(f'**** creating raster of size {n_cols} by {n_rows}')
            target_raster = driver.Create(
                global_stitch_raster_path,
                n_cols, n_rows, 1,
                gdal.GDT_Float32,
                options=(
                    'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
                    'SPARSE_OK=TRUE', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256'))
            wgs84_srs = osr.SpatialReference()
            wgs84_srs.ImportFromEPSG(4326)
            target_raster.SetProjection(wgs84_srs.ExportToWkt())
            target_raster.SetGeoTransform(
                [GLOBAL_BB[0], GLOBAL_PIXEL_SIZE_DEG, 0,
                 GLOBAL_BB[3], 0, -GLOBAL_PIXEL_SIZE_DEG])
            target_band = target_raster.GetRasterBand(1)
            target_band.SetNoDataValue(-9999)
            target_raster = None
        stitch_queue = multiprocessing_manager.Queue(N_TO_BUFFER_STITCH*2)
        stitch_thread = threading.Thread(
            target=stitch_worker,
            args=(
                stitch_queue, global_stitch_raster_path,
                len(watershed_path_list),
                signal_done_queue))
        stitch_thread.start()
        stitch_raster_queue_map[local_result_path] = stitch_queue
        stitch_worker_list.append(stitch_thread)

    clean_workspace_worker = threading.Thread(
        target=_clean_workspace_worker,
        args=(len(target_stitch_raster_map), signal_done_queue))
    clean_workspace_worker.daemon = True
    clean_workspace_worker.start()

    # Iterate through each watershed subset and run SDR
    # stitch the results of whatever outputs to whatever global output raster.
    for index, watershed_path in enumerate(watershed_path_list):
        local_workspace_dir = os.path.join(
            workspace_dir, os.path.splitext(
                os.path.basename(watershed_path))[0])
        task_graph.add_task(
            func=_execute_sdr_job,
            args=(
                watershed_path, local_workspace_dir, dem_path, erosivity_path,
                erodibility_path, lulc_path, biophysical_table_path,
                threshold_flow_accumulation, k_param, sdr_max, ic_0_param,
                target_pixel_size, biophysical_table_lucode_field,
                stitch_raster_queue_map),
            transient_run=True,
            priority=-index,  # priority in insert order
            task_name=f'sdr {os.path.basename(local_workspace_dir)}')

    LOGGER.info('wait for SDR jobs to complete')
    task_graph.join()
    task_graph.close()
    for local_result_path, stitch_queue in stitch_raster_queue_map.items():
        stitch_queue.put(None)
    LOGGER.info('all done with SDR, waiting for stitcher to terminate')
    for stitch_thread in stitch_worker_list:
        stitch_thread.join()
    LOGGER.info(
        'all done with stitching, waiting for workspace worker to terminate')
    signal_done_queue.put(None)
    clean_workspace_worker.join()

    LOGGER.info('all done with SDR -- stitcher terminated')


def _execute_sdr_job(
        watershed_path, local_workspace_dir, dem_path, erosivity_path,
        erodibility_path, lulc_path, biophysical_table_path,
        threshold_flow_accumulation, k_param, sdr_max, ic_0_param,
        target_pixel_size, biophysical_table_lucode_field,
        stitch_raster_queue_map):
    """Worker to execute sdr and send signals to stitcher.

    Args:
        watershed_path (str): path to watershed to run model over
        local_workspace_dir (str): path to local directory

        SDR arguments:
            dem_path
            erosivity_path
            erodibility_path
            lulc_path
            biophysical_table_path
            threshold_flow_accumulation
            k_param
            sdr_max
            ic_0_param
            target_pixel_size
            biophysical_table_lucode_field

        stitch_raster_queue_map (dict): map of local result path to
            the stitch queue to signal when job is done.

    Returns:
        None.
    """
    dem_pixel_size = geoprocessing.get_raster_info(dem_path)['pixel_size']
    base_raster_path_list = [
        dem_path, erosivity_path, erodibility_path, lulc_path]
    resample_method_list = ['bilinear', 'bilinear', 'bilinear', 'mode']

    clipped_data_dir = os.path.join(local_workspace_dir, 'data')
    os.makedirs(clipped_data_dir, exist_ok=True)
    watershed_info = geoprocessing.get_vector_info(watershed_path)
    target_projection_wkt = watershed_info['projection_wkt']
    watershed_bb = watershed_info['bounding_box']
    lat_lng_bb = geoprocessing.transform_bounding_box(
        watershed_bb, target_projection_wkt, osr.SRS_WKT_WGS84_LAT_LONG)

    clipped_raster_path_list = [
        os.path.join(clipped_data_dir, os.path.basename(path))
        for path in base_raster_path_list]

    geoprocessing.align_and_resize_raster_stack(
        base_raster_path_list, clipped_raster_path_list,
        resample_method_list,
        dem_pixel_size, lat_lng_bb,
        target_projection_wkt=osr.SRS_WKT_WGS84_LAT_LONG)

    # clip to lat/lng bounding boxes
    args = {
        'workspace_dir': local_workspace_dir,
        'dem_path': clipped_raster_path_list[0],
        'erosivity_path': clipped_raster_path_list[1],
        'erodibility_path': clipped_raster_path_list[2],
        'lulc_path': clipped_raster_path_list[3],
        'watersheds_path': watershed_path,
        'biophysical_table_path': biophysical_table_path,
        'threshold_flow_accumulation': threshold_flow_accumulation,
        'k_param': k_param,
        'sdr_max': sdr_max,
        'ic_0_param': ic_0_param,
        'target_pixel_size': (target_pixel_size, -target_pixel_size),
        'biophysical_table_lucode_field': biophysical_table_lucode_field,
        'target_projection_wkt': target_projection_wkt,
        'single_outlet': geoprocessing.get_vector_info(
            watershed_path)['feature_count'] == 1,
    }
    sdr_c_factor.execute(args)
    for local_result_path, stitch_queue in stitch_raster_queue_map.items():
        stitch_queue.put(
            (os.path.join(args['workspace_dir'], local_result_path), 1))


def _execute_ndr_job(
        watershed_path, local_workspace_dir, dem_path, lulc_path,
        runoff_proxy_path, fertilizer_path, biophysical_table_path,
        threshold_flow_accumulation, k_param, target_pixel_size,
        biophysical_table_lucode_field, stitch_raster_queue_map):
    """Execute NDR for watershed and push to stitch raster.

        args['workspace_dir'] (string):  path to current workspace
        args['dem_path'] (string): path to digital elevation map raster
        args['lulc_path'] (string): a path to landcover map raster
        args['runoff_proxy_path'] (string): a path to a runoff proxy raster
        args['watersheds_path'] (string): path to the watershed shapefile
        args['biophysical_table_path'] (string): path to csv table on disk
            containing nutrient retention values.

            Must contain the following headers:

            'load_n', 'eff_n', 'crit_len_n'

        args['results_suffix'] (string): (optional) a text field to append to
            all output files
        rgs['fertilizer_path'] (string): path to raster to use for fertlizer
            rates when biophysical table uses a 'use raster' value for the
            biophysical table field.
        args['threshold_flow_accumulation']: a number representing the flow
            accumulation in terms of upstream pixels.
        args['k_param'] (number): The Borselli k parameter. This is a
            calibration parameter that determines the shape of the
            relationship between hydrologic connectivity.
        args['target_pixel_size'] (2-tuple): optional, requested target pixel
            size in local projection coordinate system. If not provided the
            pixel size is the smallest of all the input rasters.
        args['target_projection_wkt'] (str): optional, if provided the
            model is run in this target projection. Otherwise runs in the DEM
            projection.
        args['single_outlet'] (str): if True only one drain is modeled, either
            a large sink or the lowest pixel on the edge of the dem.
    """
    dem_pixel_size = geoprocessing.get_raster_info(dem_path)['pixel_size']
    base_raster_path_list = [
        dem_path, runoff_proxy_path, lulc_path, fertilizer_path]
    resample_method_list = ['bilinear', 'bilinear', 'mode', 'bilinear']

    clipped_data_dir = os.path.join(local_workspace_dir, 'data')
    os.makedirs(clipped_data_dir, exist_ok=True)
    watershed_info = geoprocessing.get_vector_info(watershed_path)
    target_projection_wkt = watershed_info['projection_wkt']
    watershed_bb = watershed_info['bounding_box']
    lat_lng_bb = geoprocessing.transform_bounding_box(
        watershed_bb, target_projection_wkt, osr.SRS_WKT_WGS84_LAT_LONG)

    clipped_raster_path_list = [
        os.path.join(clipped_data_dir, os.path.basename(path))
        for path in base_raster_path_list]

    geoprocessing.align_and_resize_raster_stack(
        base_raster_path_list, clipped_raster_path_list,
        resample_method_list,
        dem_pixel_size, lat_lng_bb,
        target_projection_wkt=osr.SRS_WKT_WGS84_LAT_LONG)

    # clip to lat/lng bounding boxes
    args = {
        'workspace_dir': local_workspace_dir,
        'dem_path': clipped_raster_path_list[0],
        'runoff_proxy_path': clipped_raster_path_list[1],
        'lulc_path': clipped_raster_path_list[2],
        'fertilizer_path': clipped_raster_path_list[3],
        'watersheds_path': watershed_path,
        'biophysical_table_path': biophysical_table_path,
        'threshold_flow_accumulation': threshold_flow_accumulation,
        'k_param': k_param,
        'target_pixel_size': (target_pixel_size, -target_pixel_size),
        'target_projection_wkt': target_projection_wkt,
        'single_outlet': geoprocessing.get_vector_info(
            watershed_path)['feature_count'] == 1,
        'biophyisical_lucode_fieldname': NDR_BIOPHYSICAL_TABLE_LUCODE_KEY,
    }
    ndr_mfd_plus.execute(args)
    for local_result_path, stitch_queue in stitch_raster_queue_map.items():
        stitch_queue.put(
            (os.path.join(args['workspace_dir'], local_result_path), 1))


def _run_cv():
    pass


def _run_pollination_nature_access():
    pass


def _run_downstream_beneficiaries():
    pass


def _run_coastal_beneficiares():
    pass


def _clean_workspace_worker(expected_signal_count, stitch_done_queue):
    """Removes workspaces when completed.

    Args:
        expected_signal_count (int): the number of times to be notified
            of a done path before it should be deleted.
        stitch_done_queue (queue): will contain directory paths with the
            same directory path appearing `expected_signal_count` times,
            the directory will be removed. Recieving `None` will terminate
            the process.

    Returns:
        None
    """
    try:
        count_dict = collections.defaultdict(int)
        while True:
            dir_path = stitch_done_queue.get()
            if dir_path is None:
                LOGGER.info('recieved None, quitting clean_workspace_worker')
                return
            count_dict[dir_path] += 1
            if count_dict[dir_path] == expected_signal_count:
                LOGGER.info(
                    f'removing {dir_path} after {count_dict[dir_path]} '
                    f'signals')
                shutil.rmtree(dir_path)
                del count_dict[dir_path]
    except Exception:
        LOGGER.exception('error on clean_workspace_worker')


def stitch_worker(
        rasters_to_stitch_queue, target_stitch_raster_path, n_expected,
        signal_done_queue):
    """Update the database with completed work.

    Args:
        rasters_to_stitch_queue (queue): queue that recieves paths to
            rasters to stitch into target_stitch_raster_path.
        target_stitch_raster_path (str): path to an existing raster to stitch
            into.
        n_expected (int): number of expected stitch signals
        signal_done_queue (queue): as each job is complete the directory path
            to the raster will be passed in to eventually remove.


    Return:
        ``None``
    """
    try:
        processed_so_far = 0
        n_buffered = 0
        start_time = time.time()
        stitch_buffer_list = []
        LOGGER.info(f'started stitch worker for {target_stitch_raster_path}')
        while True:
            payload = rasters_to_stitch_queue.get()
            if payload is not None:
                stitch_buffer_list.append(payload)

            if len(stitch_buffer_list) > N_TO_BUFFER_STITCH or payload is None:
                LOGGER.info(
                    f'about to stitch {n_buffered} into '
                    f'{target_stitch_raster_path}')
                geoprocessing.stitch_rasters(
                    stitch_buffer_list, ['near']*len(stitch_buffer_list),
                    (target_stitch_raster_path, 1),
                    area_weight_m2_to_wgs84=True,
                    overlap_algorithm='replace')
                #  _ is the band number
                for stitch_path, _ in stitch_buffer_list:
                    signal_done_queue.put(os.path.dirname(stitch_path))
                stitch_buffer_list = []

            if payload is None:
                LOGGER.info(f'all done sitching {target_stitch_raster_path}')
                return

            processed_so_far += 1
            jobs_per_sec = processed_so_far / (time.time() - start_time)
            remaining_time_s = (
                n_expected / jobs_per_sec)
            remaining_time_h = int(remaining_time_s // 3600)
            remaining_time_s -= remaining_time_h * 3600
            remaining_time_m = int(remaining_time_s // 60)
            remaining_time_s -= remaining_time_m * 60
            LOGGER.info(
                f'remaining jobs to process for {target_stitch_raster_path}: '
                f'{n_expected-processed_so_far} - '
                f'processed so far {processed_so_far} - '
                f'process/sec: {jobs_per_sec:.1f}s - '
                f'time left: {remaining_time_h}:'
                f'{remaining_time_m:02d}:{remaining_time_s:04.1f}')
    except Exception:
        LOGGER.exception(
            f'error on stitch worker for {target_stitch_raster_path}')
        raise


def _run_ndr(
        workspace_dir,
        watershed_path_list,
        dem_path,
        runoff_proxy_path,
        fertilizer_path,
        lulc_path,
        target_pixel_size,
        biophysical_table_path,
        biophysical_table_lucode_field,
        threshold_flow_accumulation,
        k_param,
        target_stitch_raster_map,
        keep_intermediate_files=False,
        result_prefix=None,):
    task_graph = taskgraph.TaskGraph(
        workspace_dir, multiprocessing.cpu_count(), 10,
        parallel_mode='process', taskgraph_name='ndr processor')
    #task_graph = taskgraph.TaskGraph(workspace_dir, -1)
    stitch_raster_queue_map = {}
    stitch_worker_list = []
    multiprocessing_manager = multiprocessing.Manager()
    signal_done_queue = multiprocessing_manager.Queue()
    for local_result_path, global_stitch_raster_path in \
            target_stitch_raster_map.items():
        if result_prefix is not None:
            global_stitch_raster_path = (
                f'%s_{result_prefix}%s' % os.path.splitext(
                    global_stitch_raster_path))
        if not os.path.exists(global_stitch_raster_path):
            LOGGER.info(f'creating {global_stitch_raster_path}')
            driver = gdal.GetDriverByName('GTiff')
            n_cols = int((GLOBAL_BB[2]-GLOBAL_BB[0])/GLOBAL_PIXEL_SIZE_DEG)
            n_rows = int((GLOBAL_BB[3]-GLOBAL_BB[1])/GLOBAL_PIXEL_SIZE_DEG)
            LOGGER.info(f'**** creating raster of size {n_cols} by {n_rows}')
            target_raster = driver.Create(
                global_stitch_raster_path,
                n_cols, n_rows, 1,
                gdal.GDT_Float32,
                options=(
                    'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
                    'SPARSE_OK=TRUE', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256'))
            wgs84_srs = osr.SpatialReference()
            wgs84_srs.ImportFromEPSG(4326)
            target_raster.SetProjection(wgs84_srs.ExportToWkt())
            target_raster.SetGeoTransform(
                [GLOBAL_BB[0], GLOBAL_PIXEL_SIZE_DEG, 0,
                 GLOBAL_BB[3], 0, -GLOBAL_PIXEL_SIZE_DEG])
            target_band = target_raster.GetRasterBand(1)
            target_band.SetNoDataValue(-9999)
            target_raster = None
        stitch_queue = multiprocessing_manager.Queue(N_TO_BUFFER_STITCH*2)
        stitch_thread = threading.Thread(
            target=stitch_worker,
            args=(
                stitch_queue, global_stitch_raster_path,
                len(watershed_path_list),
                signal_done_queue))
        stitch_thread.start()
        stitch_raster_queue_map[local_result_path] = stitch_queue
        stitch_worker_list.append(stitch_thread)

    clean_workspace_worker = threading.Thread(
        target=_clean_workspace_worker,
        args=(len(target_stitch_raster_map), signal_done_queue))
    clean_workspace_worker.daemon = True
    clean_workspace_worker.start()

    # Iterate through each watershed subset and run ndr
    # stitch the results of whatever outputs to whatever global output raster.
    for index, watershed_path in enumerate(watershed_path_list):
        local_workspace_dir = os.path.join(
            workspace_dir, os.path.splitext(
                os.path.basename(watershed_path))[0])
        task_graph.add_task(
            func=_execute_ndr_job,
            args=(
                watershed_path, local_workspace_dir, dem_path, lulc_path,
                runoff_proxy_path, fertilizer_path, biophysical_table_path,
                threshold_flow_accumulation, k_param, target_pixel_size,
                biophysical_table_lucode_field, stitch_raster_queue_map),
            transient_run=True,
            priority=-index,  # priority in insert order
            task_name=f'ndr {os.path.basename(local_workspace_dir)}')

    LOGGER.info('wait for ndr jobs to complete')
    task_graph.join()
    task_graph.close()
    for local_result_path, stitch_queue in stitch_raster_queue_map.items():
        stitch_queue.put(None)
    LOGGER.info('all done with ndr, waiting for stitcher to terminate')
    for stitch_thread in stitch_worker_list:
        stitch_thread.join()
    LOGGER.info(
        'all done with stitching, waiting for workspace worker to terminate')
    signal_done_queue.put(None)
    clean_workspace_worker.join()

    LOGGER.info('all done with ndr -- stitcher terminated')



def main():
    """Entry point."""
    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, multiprocessing.cpu_count(),
        parallel_mode='thread', taskgraph_name='run pipeline main')
    data_map = fetch_and_unpack_data(task_graph)

    watershed_subset = {
        'af_bas_15s_beta': [19039, 23576, 18994],
        'au_bas_15s_beta': [125804],
        }
    watershed_subset = None

    # make sure taskgraph doesn't re-run just because the file was opened
    watershed_subset_task = task_graph.add_task(
        func=_batch_into_watershed_subsets,
        args=(
            data_map[WATERSHEDS_KEY], 4, WATERSHED_SUBSET_TOKEN_PATH,
            watershed_subset),
        target_path_list=[WATERSHED_SUBSET_TOKEN_PATH],
        store_result=True,
        task_name='watershed subset batch')
    watershed_subset_list = watershed_subset_task.get()

    task_graph.join()
    task_graph.close()
    task_graph = None

    LOGGER.debug(len(watershed_subset_list))
    LOGGER.debug(watershed_subset_list)

    sdr_target_stitch_raster_map = {
        'sed_export.tif': os.path.join(
            WORKSPACE_DIR, 'global_sed_export.tif'),
        'sed_retention.tif': os.path.join(
            WORKSPACE_DIR, 'global_sed_retention.tif'),
        'sed_deposition.tif': os.path.join(
            WORKSPACE_DIR, 'global_sed_deposition.tif'),
        'usle.tif': os.path.join(
            WORKSPACE_DIR, 'global_usle.tif'),
    }

    ndr_target_stitch_raster_map = {
        'n_export.tif': os.path.join(
            WORKSPACE_DIR, 'global_n_export.tif'),
        'n_retention.tif': os.path.join(
            WORKSPACE_DIR, 'global_n_retention.tif'),
        os.path.join('intermediate_outputs', 'modified_load_n.tif'): os.path.join(
            WORKSPACE_DIR, 'global_modified_load_n.tif'),
    }

    run_sdr = False
    run_ndr = True

    for lulc_key in [SCENARIO_1_V2_LULC_KEY]:
        if run_sdr:
            sdr_workspace_dir = os.path.join(SDR_WORKSPACE_DIR, lulc_key)
            _run_sdr(
                workspace_dir=sdr_workspace_dir,
                watershed_path_list=watershed_subset_list,
                dem_path=data_map[DEM_KEY],
                erosivity_path=data_map[EROSIVITY_KEY],
                erodibility_path=data_map[ERODIBILITY_KEY],
                lulc_path=data_map[lulc_key],
                target_pixel_size=TARGET_PIXEL_SIZE_M,
                biophysical_table_path=data_map[SDR_BIOPHYSICAL_TABLE_KEY],
                biophysical_table_lucode_field=SDR_BIOPHYSICAL_TABLE_LUCODE_KEY,
                threshold_flow_accumulation=THRESHOLD_FLOW_ACCUMULATION,
                l_cap=L_CAP,
                k_param=K_PARAM,
                sdr_max=SDR_MAX,
                ic_0_param=IC_0_PARAM,
                target_stitch_raster_map=sdr_target_stitch_raster_map,
                keep_intermediate_files=False,
                result_prefix=lulc_key,
                )

        if run_ndr:
            ndr_workspace_dir = os.path.join(NDR_WORKSPACE_DIR, lulc_key)
            _run_ndr(
                workspace_dir=ndr_workspace_dir,
                runoff_proxy_path=data_map[RUNOFF_PROXY_KEY],
                fertilizer_path=data_map[FERTILZER_KEY],
                biophysical_table_path=data_map[NDR_BIOPHYSICAL_TABLE_KEY],
                biophysical_table_lucode_field=NDR_BIOPHYSICAL_TABLE_LUCODE_KEY,
                watershed_path_list=watershed_subset_list,
                dem_path=data_map[DEM_KEY],
                lulc_path=data_map[lulc_key],
                target_pixel_size=TARGET_PIXEL_SIZE_M,
                threshold_flow_accumulation=THRESHOLD_FLOW_ACCUMULATION,
                k_param=K_PARAM,
                target_stitch_raster_map=ndr_target_stitch_raster_map,
                keep_intermediate_files=False,
                result_prefix=lulc_key,
                )


if __name__ == '__main__':
    main()
