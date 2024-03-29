"""These calculations are for the CI global restoration project."""
#conda activate py38_gdal312
#conda activate py39_ecoprocess

import glob
import sys
import os
import logging
import multiprocessing
import datetime
import subprocess
import raster_calculations_core
from osgeo import gdal
from osgeo import osr
import taskgraph
#import pygeoprocessing
import ecoshard.geoprocessing as pygeoprocessing

gdal.SetCacheMax(2**30)

WORKSPACE_DIR = 'CNC_workspace'
NCPUS = multiprocessing.cpu_count()
try:
    os.makedirs(WORKSPACE_DIR)
except OSError:
    pass

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def main():
    """Write your expression here."""


    #first run make_poll_suff.py from https://github.com/therealspring/pollination_sufficiency with the following docker commands
    #docker run -d --name pollsuff_container --rm -v %CD%:/usr/local/workspace therealspring/inspring:latest make_poll_suff.py ./ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif && docker logs pollination_container -f
    #docker run -d --name pollsuff_container --rm -v %CD%:/usr/local/workspace therealspring/inspring:latest make_poll_suff.py ./Sc2_Griscom_CookPatton_smithpnv_md5_1536327d82e292529e7872dc6ecc2871.tif && docker logs pollination_container -f
    #python make_poll_suff.py ./ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031
    #python make_poll_suff.py ./Sc1v5_md5_85604d25eb189f3566712feb506a8b9f.tif
    #python make_poll_suff.py ./Sc1v6_md5_c3539eae022a1bf588142bc363edf5a3.tif
    #python make_poll_suff.py ./Sc2v5_md5_a3ce41871b255adcd6e1c65abfb1ddd0.tif
    #python make_poll_suff.py ./Sc2v6_md5_dc75e27f0cb49a84e082a7467bd11214.tif
    #python make_poll_suff.py "D:\ecoshard\CI_PPC\scenarios\Sc3v1_PNVnoag_md5_c07865b995f9ab2236b8df0378f9206f.tif"
    #python make_poll_suff.py "D:\repositories\tnc-sci-ncscobenefits\workspace\data\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif"
    #python make_poll_suff.py "D:\repositories\tnc-sci-ncscobenefits\workspace\data\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7cds_compressed_md5_83ec1b.tif"

    #ACTUALLY, STOP USING THIS ONE GO HERE INSTEAD:
    #pollination_sufficiency/pollination_pipeline_calc_ppl_fed.py
    
    calculation_list = [ 
        {
           'expression': 'raster1*raster2*raster3*(raster4>0)+(raster4<1)*-9999',
           'symbol_to_path_map': { 
               'raster1': "monfreda_2008_yield_poll_dep_ppl_fed_5min.tif", #https://storage.googleapis.com/critical-natural-capital-ecoshards/monfreda_2008_yield_poll_dep_ppl_fed_5min.tif
               'raster2': r"workspace_poll_suff\churn\poll_suff_hab_ag_coverage_rasters\poll_suff_ag_coverage_prop_10s_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7cds_compressed_md5_83ec1b.tif",
               'raster3': "esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif", # https://storage.googleapis.com/ecoshard-root/esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif
               'raster4': r"workspace_poll_suff\churn\ag_mask\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7cds_compressed_md5_83ec1b_ag_mask.tif"
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "pollination_ppl_fed_on_ag_10s_esa2020mar.tif",
        },
        {
           'expression': 'raster1*raster2*raster3*(raster4>0)+(raster4<1)*-9999',
           'symbol_to_path_map': {
               'raster1': "monfreda_2008_yield_poll_dep_ppl_fed_5min.tif", #https://storage.googleapis.com/critical-natural-capital-ecoshards/monfreda_2008_yield_poll_dep_ppl_fed_5min.tif
               'raster2': r"D:\repositories\pollination_sufficiency\workspace_poll_suff\churn\poll_suff_hab_ag_coverage_rasters\poll_suff_ag_coverage_prop_10s_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif",
               'raster3': "esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif", # https://storage.googleapis.com/ecoshard-root/esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif
               'raster4': r"D:\repositories\pollination_sufficiency\workspace_poll_suff\churn\ag_mask\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_ag_mask.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "pollination_ppl_fed_on_ag_10s_esa1992mar.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #then back to docker with both those layers:
    # docker run -d --name pollination_container --rm -v %CD%:/usr/local/workspace therealspring/inspring:latest realized_pollination.py pollination_ppl_fed_on_ag_10s_esa2020mVCF.tif && docker logs pollination_container -f
    # docker run -d --name pollination_container --rm -v %CD%:/usr/local/workspace therealspring/inspring:latest realized_pollination.py pollination_ppl_fed_on_ag_10s_Sc2_Griscom_CookPatton.tif && docker logs pollination_container -f


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
