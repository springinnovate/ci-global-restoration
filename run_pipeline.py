"""Entry point to manage data and run pipeline."""


ECOSHARD_MAP = {
    'ESA_LULC': 'https://storage.googleapis.com/ecoshard-root/esa_lulc_smoothed/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif',
    'Scenario1_LULC': None,
    'Biophysical table': None,
    'DEM': 'https://storage.googleapis.com/global-invest-sdr-data/global_dem_3s_md5_22d0c3809af491fa09d03002bdf09748.zip',
    'Erosivity': 'https://storage.googleapis.com/ecoshard-root/GlobalR_NoPol_compressed_md5_ab6d34ca8827daa3fda42a96b6190ecc.tif',
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
    'Coastal population' :'(need to make from population above and this mask: https://storage.googleapis.com/ecoshard-root/ipbes-cv/total_pop_masked_by_10m_md5_ef02b7ee48fa100f877e3a1671564be2.tif)',
    'Coastal habitat masks ESA': '(will be outputs of CV)',
    'Coastal habitat masks Scenario 1': '(will be outputs of CV)',
    }

def main():
    """Entry point."""
    pass


if __name__ == '__main__':
    main()
