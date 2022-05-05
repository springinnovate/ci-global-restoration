"""Build Riparian buffers on US 30m."""
import os
import logging

from ecoshard.geoprocessing import routing
from ecoshard import taskgraph
from osgeo import gdal


gdal.SetCacheMax(2**26)
_LARGEST_BLOCK = 2**26

logging.basicConfig(
    level=logging.INFO,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)
logging.getLogger('ecoshard.taskgraph').setLevel(logging.INFO)

WORKSPACE_DIR = '_riparian_buffers_workspace'


def main():
    """Entry point."""
    os.makedirs(WORKSPACE_DIR, exist_ok=True)
    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)
    dem_raster_path = 'data/SRTM_30m_US_huc06_compressed_md5_9c98d7.tif'

    filled_dem_raster_path = os.path.join(
        WORKSPACE_DIR, f'filled_{os.path.basename(dem_raster_path)}.tif')

    LOGGER.info('fill pits')
    fill_task = task_graph.add_task(
        func=routing.fill_pits,
        args=((dem_raster_path, 1), filled_dem_raster_path),
        kwargs={
            'working_dir': WORKSPACE_DIR,
            # on a 30m dem this is about 30 football fields
            'max_pixel_fill_count': 1000},
        target_path_list=[filled_dem_raster_path],
        task_name=f'fill dem at {dem_raster_path}')

    LOGGER.info('flow dir')
    flow_dir_raster_path = os.path.join(
        WORKSPACE_DIR, f'd8_{os.path.basename(dem_raster_path)}.tif')
    flow_dir_task = task_graph.add_task(
        func=routing.flow_dir_d8,
        args=((filled_dem_raster_path, 1), flow_dir_raster_path),
        target_path_list=[flow_dir_raster_path],
        dependent_task_list=[fill_task],
        task_name=f'route {dem_raster_path}')

    LOGGER.debug('flow accum')
    flow_accum_raster_path = os.path.join(
        WORKSPACE_DIR, f'flow_accum_{os.path.basename(dem_raster_path)}')
    flow_accum_task = task_graph.add_task(
        func=routing.flow_accumulation_d8,
        args=(
            (flow_dir_raster_path, 1), flow_accum_raster_path),
        target_path_list=[flow_accum_raster_path],
        dependent_task_list=[flow_dir_task],
        task_name=f'flow accum for {flow_dir_raster_path}')

    LOGGER.debug('stream vector')
    stream_vector_path = os.path.join(WORKSPACE_DIR, f'''streams_{
        os.path.basename(os.path.splitext(dem_raster_path)[0])}.gpkg''')
    extract_streams_task = task_graph.add_task(
        func=routing.extract_strahler_streams_d8,
        args=(
            (flow_dir_raster_path, 1), (flow_accum_raster_path, 1),
            (filled_dem_raster_path, 1),
            stream_vector_path),
        target_path_list=[stream_vector_path],
        dependent_task_list=[flow_dir_task, flow_accum_task],
        task_name=f'extract streams {flow_dir_raster_path}')

    LOGGER.debug('subwatershed extraction')
    watershed_boundary_vector_path = os.path.join(
        WORKSPACE_DIR, f'''subwatersheds_{
            os.path.basename(os.path.splitext(dem_raster_path)[0])}.gpkg''')
    calc_watershed_task = task_graph.add_task(
        func=routing.calculate_subwatershed_boundary,
        args=(
            (flow_dir_raster_path, 1), stream_vector_path,
            watershed_boundary_vector_path),
        target_path_list=[watershed_boundary_vector_path],
        dependent_task_list=[flow_dir_task, extract_streams_task],
        task_name=f'calc watershed for {stream_vector_path}')

    LOGGER.info('detect outlets')
    outlet_vector_path = os.path.join(WORKSPACE_DIR, f'''outlets_{
        os.path.basename(os.path.splitext(dem_raster_path)[0])}.gpkg''')
    detect_outlets_task = task_graph.add_task(
        func=routing.detect_outlets,
        args=((flow_dir_raster_path, 1), 'd8', outlet_vector_path),
        target_path_list=[outlet_vector_path],
        dependent_task_list=[flow_dir_task],
        task_name=f'detect outlets on {flow_dir_raster_path}')

    task_graph.close()
    task_graph.join()


if __name__ == '__main__':
    main()
