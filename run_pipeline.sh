#!/bin/bash
#SDR NDR in the current directory
#python run_ndr_sdr_pipeline.py && touch DONE_SDR_NDR

#Pollination
git clone https://github.com/therealspring/pollination_sufficiency
cd pollination_sufficiency
echo RUN POLLINATION
echo `pwd`
wget -nc https://storage.googleapis.com/ecoshard-root/ci_global_restoration/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif
docker run --name pollsuff_container -it --rm -v `pwd`:/usr/local/workspace therealspring/inspring:latest ./make_poll_suff.py ./ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif #&& docker logs pollination_container -f
cd ..
touch DONE_POLLINATION



# #CV
# git clone https://github.com/therealspring/cnc_global_cv
# pushd cnc_global_cv
# docker run -d --shm-size=4gb --name cv_container --rm -v `pwd`:/workspace therealspring/invest-env:0.1.0 "python global_cv_analysis.py CI-GLOBAL-RESTORATION.txt --shore_point_sample_distance 2000"
# popd
# touch DONE_CV


# #Nature Access
# git clone https://github.com/therealspring/distance-to-hab-with-friction
# pushd distance-to-hab-with-friction
# echo RUN RUN DISTANCE TO HAB
# popd
# touch DONE_NATURE_ACCESS

# #Downstream beneficiaries
# git clone https://github.com/therealspring/downstream-beneficiaries
# pushd downstream-beneficiaries
# echo RUN DOWNSTREAM BENEFICIARIES
# popd
# touch DONE_DOWNSTREAM_BENEFICIARIES

# #Coastal beneficiares
# git clone https://github.com/therealspring/people_protected_by_coastal_habitat
# pushd people_protected_by_coastal_habitat
# echo RUN PEOPLE PROTECTED BY COASTAL HAB
# popd
# touch DONE_COASTAL_BENEFICIARIES
