import natcap.invest.scenario_generator_proximity_based

edge_args = {
    u'area_to_convert': u'20000.0',
    u'base_lulc_path': r"C:\Users\Becky\Documents\ci-global-restoration\viscose\fc_20190000065536-0000262144.tif",
    u'convert_farthest_from_edge': False,
    u'convert_nearest_to_edge': True,
    u'convertible_landcover_codes': u'1',
    u'focal_landcover_codes': u'1',
    u'n_fragmentation_steps': u'1',
    u'replacment_lucode': u'0',
    u'results_suffix': 'edge',
    u'workspace_dir': r"C:\Users\Becky\Documents\ci-global-restoration\viscose",
}

#core_args = {
#    u'aoi_path': u'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_aoi.shp',
#    u'area_to_convert': u'20000.0',
#    u'base_lulc_path': u'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_lulc.tif',
#    u'convert_farthest_from_edge': True,
#    u'convert_nearest_to_edge': False,
#    u'convertible_landcover_codes': u'1 2 3 4 5',
#    u'focal_landcover_codes': u'1 2 3 4 5',
#    u'n_fragmentation_steps': u'1',
#    u'replacment_lucode': u'12',
#    u'results_suffix': 'core',
#    u'workspace_dir': u'C:\\Users\\Rich/Documents/scenario_proximity_workspace',
#}
#
#frag_args = {
#    u'aoi_path': u'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_aoi.shp',
#    u'area_to_convert': u'20000.0',
#    u'base_lulc_path': u'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_lulc.tif',
#    u'convert_farthest_from_edge': True,
#    u'convert_nearest_to_edge': False,
#    u'convertible_landcover_codes': u'1 2 3 4 5',
#    u'focal_landcover_codes': u'1 2 3 4 5',
#    u'n_fragmentation_steps': u'10',
#    u'replacment_lucode': u'12',
#    u'results_suffix': 'frag',
#    u'workspace_dir': u'C:\\Users\\Rich/Documents/scenario_proximity_workspace',
#}
#
#ag_args = {
#    u'aoi_path': u'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_aoi.shp',
#    u'area_to_convert': u'20000.0',
#    u'base_lulc_path': u'C:/Users/Rich/Documents/svn_repos/invest-sample-data/scenario_proximity/scenario_proximity_lulc.tif',
#    u'convert_farthest_from_edge': False,
#    u'convert_nearest_to_edge': True,
#    u'convertible_landcover_codes': u'12',
#    u'focal_landcover_codes': u'1 2 3 4 5',
#    u'n_fragmentation_steps': u'1',
#    u'replacment_lucode': u'12',
#    u'results_suffix': 'ag',
#    u'workspace_dir': u'C:\\Users\\Rich/Documents/scenario_proximity_workspace',
#}
if __name__ == '__main__':
    natcap.invest.scenario_generator_proximity_based.execute(edge_args)
#    natcap.invest.scenario_generator_proximity_based.execute(core_args)
#    natcap.invest.scenario_generator_proximity_based.execute(frag_args)
#    natcap.invest.scenario_generator_proximity_based.execute(ag_args)