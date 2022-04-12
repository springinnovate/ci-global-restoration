from inspring import scenario_gen_proximity

edge_args = {
    'area_to_convert': '20000.0',
    'base_lulc_path': r"D:\repositories\ci-global-restoration\workspace\data\fc_2019_indonesia_md5_3f6187.tif",
    'convert_farthest_from_edge': False,
    'convert_nearest_to_edge': True,
    'convertible_landcover_codes': '1',
    'focal_landcover_codes': '1',
    'n_fragmentation_steps': '1',
    'replacement_lucode': '0',
    'results_suffix': 'edge',
    'workspace_dir': 'D:/repositories/ci-global-restoration',
}

#core_args = {
#    'aoi_path': 'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_aoi.shp',
#    'area_to_convert': '20000.0',
#    'base_lulc_path': 'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_lulc.tif',
#    'convert_farthest_from_edge': True,
#    'convert_nearest_to_edge': False,
#    'convertible_landcover_codes': '1 2 3 4 5',
#    'focal_landcover_codes': '1 2 3 4 5',
#    'n_fragmentation_steps': '1',
#    'replacement_lucode': '12',
#    'results_suffix': 'core',
#    'workspace_dir': 'C:/Users/Rich/Documents/scenario_proximity_workspace',
#}
#
#frag_args = {
#    'aoi_path': 'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_aoi.shp',
#    'area_to_convert': '20000.0',
#    'base_lulc_path': 'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_lulc.tif',
#    'convert_farthest_from_edge': True,
#    'convert_nearest_to_edge': False,
#    'convertible_landcover_codes': '1 2 3 4 5',
#    'focal_landcover_codes': '1 2 3 4 5',
#    'n_fragmentation_steps': '10',
#    'replacement_lucode': '12',
#    'results_suffix': 'frag',
#    'workspace_dir': 'C:/Users/Rich/Documents/scenario_proximity_workspace',
#}
#
#ag_args = {
#    'aoi_path': 'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_aoi.shp',
#    'area_to_convert': '20000.0',
#    'base_lulc_path': 'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_lulc.tif',
#    'convert_farthest_from_edge': False,
#    'convert_nearest_to_edge': True,
#    'convertible_landcover_codes': '12',
#    'focal_landcover_codes': '1 2 3 4 5',
#    'n_fragmentation_steps': '1',
#    'replacement_lucode': '12',
#    'results_suffix': 'ag',
#    'workspace_dir': 'C:/Users/Rich/Documents/scenario_proximity_workspace',
#}

if __name__ == '__main__':
    scenario_gen_proximity.execute(edge_args)
#    scenario_gen_proximity.execute(core_args)
#    scenario_gen_proximity.execute(frag_args)
#    scenario_gen_proximity.execute(ag_args)