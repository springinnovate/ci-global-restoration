"""Change a landcover map based on a probabilty map."""
import argparse
import logging
import os
import tempfile

from osgeo import gdal
from ecoshard import geoprocessing
import numpy


gdal.SetCacheMax(2**30)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
logging.getLogger('taskgraph').setLevel(logging.WARN)
LOGGER = logging.getLogger(__name__)


def _flip_pixel_proportion(
        base_array, probability_array, flip_target, flip_proportion, flip_nodata):
    """Flip pixels in `base_array` to `flip_target` where prob >= prop.

    Args:
        base_array (numpy.ndarray of int): base integer array
        probability_array (numpy.ndarray of float): values 0 <= x <= 1.
        flip_target (int): target integer to set if base pixel is to be
            flipped.
        flip_proportion (float): in [0, 1] indicates which pixels to flip
            in base if equivalent pixel in probability is >= to this value
        flip_nodata (float): nodata value for flip proportion

    Returns:
        numpy.ndarray with pixels flipped where > flip proportion
    """
    result = numpy.copy(base_array)
    flip_mask = (
        (probability_array >= flip_proportion) &
        (flip_proportion != flip_nodata))
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
        'flip_target_path', type=str,
        help='value to set in lulc raster if prop > flip_prop')
    args = parser.parse_args()

    flip_basename = os.path.basename(
        os.path.splitext(args.flip_target_path)[0])
    target_raster_path = (
        f'''{flip_basename}_{args.flip_proportion}_{
            os.path.basename(args.lulc_path)}''')

    workspace_dir = tempfile.mkdtemp(
        dir='.', prefix='create_scenario_workspace_')

    base_raster_path_list = [
        args.lulc_path, args.probability_path, args.flip_target_path]

    aligned_raster_path_list = [
        os.path.join(workspace_dir, os.path.basename(path))
        for path in base_raster_path_list]
    lulc_info = geoprocessing.get_raster_info(args.lulc_path)

    LOGGER.info('align raster stack')
    geoprocessing.align_and_resize_raster_stack(
        base_raster_path_list,
        aligned_raster_path_list, ['mode', 'average', 'mode'],
        lulc_info['pixel_size'], 'union')

    LOGGER.info(
        f'flip values >= {args.flip_proportion} to {args.flip_target_path}')

    prob_nodata = geoprocessing.get_raster_info(
        args.flip_proportion)['nodata'][0]
    geoprocessing.raster_calculator(
        [(p, 1) for p in aligned_raster_path_list] + [
            (args.flip_proportion, 'raw'), (prob_nodata, 'raw')],
        _flip_pixel_proportion, target_raster_path,
        lulc_info['datatype'], lulc_info['nodata'][0])


if __name__ == '__main__':
    main()
