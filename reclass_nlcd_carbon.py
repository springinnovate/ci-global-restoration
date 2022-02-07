"""Reclass NLCD with carbon, 1 cotton to 83 (it's all 0) after doing this add it to NLCD input table so i can run SDR/NDR runs."""
import os

from ecoshard import taskgraph
from ecoshard import geoprocessing


def _mask_op(base_array, mask_array, target_val):
    result = base_array.copy()
    result[mask_array] = target_val
    return result


if __name__ == '__main__':
    cl_path = 'Confident_Cotton_Layer_2011_to_2020.tif'
    nlcd_path = 'nlcd2016_compressed_md5_f372b.tif'
    nlcd_cotton_path = f'cotton_to_83_{nlcd_path}'

    workspace_dir = 'confident_cotton_dir'
    os.makedirs(workspace_dir, exist_ok=True)

    warped_cl_path = os.path.join(
        workspace_dir,
        f'warped_{os.path.basename(cl_path)}')
    nlcd_info = geoprocessing.get_raster_info(nlcd_path)
    task_graph = taskgraph.TaskGraph(workspace_dir, -1)
    warp_task = task_graph.add_task(
        func=geoprocessing.warp_raster,
        args=(
            cl_path, nlcd_info['pixel_size'], warped_cl_path,
            'mode'),
        kwargs={
            'target_bb': nlcd_info['bounding_box'],
            'target_projection_wkt': nlcd_info['projection_wkt'],
            'working_dir': workspace_dir},
        target_path_list=[warped_cl_path],
        task_name=f'warp {cl_path}')

    _ = task_graph.add_task(
        func=geoprocessing.raster_calculator,
        args=(
            [(cl_path, 1), (warped_cl_path, 1)], _mask_op, nlcd_cotton_path,
            nlcd_info['datatype'], nlcd_info['nodata'][0]),
        target_path_list=[nlcd_cotton_path],
        dependent_task_list=[warp_task],
        task_name=f'convert to 81 where cotton for {nlcd_cotton_path}')

    task_graph.join()
    task_graph.close()
