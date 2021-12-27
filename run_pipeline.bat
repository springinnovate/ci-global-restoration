@echo off
:: SDR NDR in the current directory -- READY TO RUN
python run_ndr_sdr_pipeline.py
touch DONE_SDR_NDR

rem :: Pollination -- READY TO RUN
rem git clone https://github.com/therealspring/pollination_sufficiency
rem cd pollination_sufficiency
rem echo RUN POLLINATION
rem wget -nc https://storage.googleapis.com/ecoshard-root/ci_global_restoration/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif
rem wget -nc https://storage.googleapis.com/critical-natural-capital-ecoshards/monfreda_2008_yield_poll_dep_ppl_fed_5min.tif
rem wget -nc https://storage.googleapis.com/ecoshard-root/esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif
rem python make_poll_suff.py ./ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif

rem wget -nc https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc2_Griscom_CookPatton_smithpnv_md5_1536327d82e292529e7872dc6ecc2871.tif
rem python make_poll_suff.py ./Sc2_Griscom_CookPatton_smithpnv_md5_1536327d82e292529e7872dc6ecc2871.tif
rem python calc_people_fed.py
rem python realized_pollination.py pollination_ppl_fed_on_ag_10s_esa2020mVCF.tif workspace_poll_suff\churn\hab_mask\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46_hab_mask.tif
rem python realized_pollination.py pollination_ppl_fed_on_ag_10s_Sc2_Griscom_CookPatton.tif workspace_poll_suff\churn\hab_mask\Sc2_Griscom_CookPatton_smithpnv_md5_1536327d82e292529e7872dc6ecc2871_hab_mask.tif
rem cd ..
rem touch DONE_POLLINATION

:: CV -- READY TO RUN
rem git clone https://github.com/therealspring/cnc_global_cv
rem pushd cnc_global_cv
rem python global_cv_analysis.py CI-GLOBAL-RESTORATION.txt
rem popd
rem touch DONE_CV

:: Coastal beneficiares
rem git clone https://github.com/therealspring/people_protected_by_coastal_habitat

:: need to copy the following rasters over from global_cv_analysis.py called habitat_value_raster_path on line 2163
rem mkdir people_protected_by_coastal_habitat\value_rasters
rem cp cnc_global_cv/global_cv_workspace/Sc2_Griscom_CookPatton_smithpnv_md5_1536327d82e292529e7872dc6ecc2871_hab_mask/value_rasters/*.tif ./people_protected_by_coastal_habitat/value_rasters
rem cp cnc_global_cv/global_cv_workspace/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46/value_rasters/*.tif ./people_protected_by_coastal_habitat/value_rasters

rem landcover_basename =
rem     ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46,


rem habitat_id =
rem     'reefs',
rem     'mangroves_forest',
rem     'saltmarsh_wetland',
rem     'seagrass',
rem     '4_500',
rem     '2_2000',
::  Coastal habitat masks ESA   (will be outputs of CV)
::  Coastal habitat masks Scenario 1    (will be outputs of CV)
:: I need to go back to CV and make sure the outputs of the _value rasters are named w/r/t
rem cd people_protected_by_coastal_habitat

rem global_cv_workspace/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46/value_rasters/'%s_value.tif habitat_id
rem python global_cv_analysis.py CI-GLOBAL-RESTORATION.txt --shore_point_sample_distance 2000

:: Masking Nature Access TODO
:: there are 6 outputs "service index are the units" from global_cv_analysis that are 6 different habitats in the following order
:: * reefs, mangroves, 2_2000, saltmarsh/wetland, seagrass, 4_500
