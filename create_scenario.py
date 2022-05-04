"""Change a landcover map based on a probabilty map."""
import argparse
import logging
import os
import hashlib

from osgeo import gdal
from ecoshard import geoprocessing
import numpy
from ecoshard import taskgraph


gdal.SetCacheMax(2**30)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
logging.getLogger('taskgraph').setLevel(logging.WARN)
LOGGER = logging.getLogger(__name__)


def _flip_pixel_proportion(
        base_array, probability_array, flip_target, flip_proportion,
        probability_nodata):
    """Flip pixels in `base_array` to `flip_target` where prob >= prop.

    Args:
        base_array (numpy.ndarray of int): base integer array
        probability_array (numpy.ndarray of float): values 0 <= x <= 1.
        flip_target (numpy.ndarray): array of targets to flip to if the
            base pixel exceeds threshold flip proportion.
        flip_proportion (float): in [0, 1] indicates which pixels to flip
            in base if equivalent pixel in probability is >= to this value
        probability_nodata (float): nodata value for the probability array

    Returns:
        numpy.ndarray with pixels flipped where > flip proportion
    """
    result = numpy.copy(base_array)
    flip_mask = (
        (probability_array >= flip_proportion) &
        (probability_array != probability_nodata))
    result[flip_mask] = flip_target[flip_mask]
    return result


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description='change lulc map based on probability map')
    parser.add_argument('lulc_path', type=str, help='path to landcover map')
    parser.add_argument(
        'probability_path', type=str, help='path to probability map')
    parser.add_argument(
        'flip_proportion', type=float, help='value in 0..1 to flip lulc pixel')
    parser.add_argument(
        '--flip_target_path', type=str,
        help='raster of values to set in lulc raster if prop > flip_prop')
    parser.add_argument(
        '--flip_target_val', type=float,
        help='value to set in lulc raster if prop > flip_prop')
    args = parser.parse_args()

    base_raster_path_list = [args.lulc_path, args.probability_path]
    align_mode_list = ['mode', 'average']
    if bool(args.flip_target_path) == bool(args.flip_target_val):
        raise ValueError(
            'either `--flip_target_path` xor `--flip_target_val` must be set')

    if args.flip_target_val:
        flip_val_arg = [numpy.array([args.flip_target_val])]
    else:
        flip_val_arg = []

    if args.flip_target_path:
        flip_basename = os.path.basename(
            os.path.splitext(args.flip_target_path)[0])
        target_raster_path = (
            f'''{flip_basename}_{args.flip_proportion}_{
                os.path.basename(args.lulc_path)}''')
        base_raster_path_list.append(args.flip_target_path)
        align_mode_list.append('mode')
    else:
        target_raster_path = (
            f'''{args.flip_target_val}_{args.flip_proportion}_{
                os.path.basename(args.lulc_path)}''')

    path_hash = hashlib.sha256()
    path_hash.update(','.join([
        os.path.basename(path) for path in base_raster_path_list + [
            str(args.flip_proportion)] + [str(flip_val_arg[0])]]).encode(
        'utf-8'))
    workspace_dir = os.path.join(
        '_create_scenario_workspace', path_hash.hexdigest()[:5])
    os.makedirs(workspace_dir, exist_ok=True)

    task_graph = taskgraph.TaskGraph(workspace_dir, -1)

    aligned_raster_path_list = [
        os.path.join(workspace_dir, os.path.basename(path))
        for path in base_raster_path_list]
    lulc_info = geoprocessing.get_raster_info(args.lulc_path)

    LOGGER.info(f'align raster stack {lulc_info["pixel_size"]}')
    align_task = task_graph.add_task(
        func=geoprocessing.align_and_resize_raster_stack,
        args=(
            base_raster_path_list,
            aligned_raster_path_list, align_mode_list,
            lulc_info['pixel_size'], 'union'),
        target_path_list=aligned_raster_path_list,
        task_name='align raster stack')

    if args.flip_target_path:
        LOGGER.info(
            f'flip values >= {args.flip_proportion} to '
            f'{args.flip_target_path}')
    else:
        LOGGER.info(
            f'flip values >= {args.flip_proportion} to '
            f'{args.flip_target_val}')

    prob_nodata = geoprocessing.get_raster_info(
        args.probability_path)['nodata'][0]
    task_graph.add_task(
        func=geoprocessing.raster_calculator,
        args=(
            [(p, 1) for p in aligned_raster_path_list] + flip_val_arg + [
                (args.flip_proportion, 'raw'), (prob_nodata, 'raw')],
            _flip_pixel_proportion, target_raster_path,
            lulc_info['datatype'],
            lulc_info['nodata'][0]),
        target_path_list=[target_raster_path],
        dependent_task_list=[align_task],
        task_name='flip pixels on scenario')

    task_graph.close()
    task_graph.join()


if __name__ == '__main__':
    main()
