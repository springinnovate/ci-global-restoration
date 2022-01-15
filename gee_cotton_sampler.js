var image_sum = ee.Image(0)
var start_year = 2011;
var end_year = 2020;
for (var year = start_year; year <= end_year; year++) {
    var cdl_dataset = ee.ImageCollection('USDA/NASS/CDL')
                  .filter(ee.Filter.date(String(year)+'-01-01', String(year)+'-12-31'))
                  .first();
    var cropLandcoverLayer = cdl_dataset.select('cropland')
    var confidenceLayer = cdl_dataset.select('confidence');
    //confidence > .9 for cotton only for current year
    image_sum = image_sum.add(confidenceLayer.multiply(cropLandcoverLayer.eq(2)).gte(.9))
 }


var confident_cotton_layer = image_sum.gte(2);
var label = 'Confident Cotton Layer '+String(start_year)+' to '+String(end_year);
label = label.replace(/ /g, '_');
confident_cotton_layer = confident_cotton_layer.rename(label);
confident_cotton_layer = confident_cotton_layer.updateMask(confident_cotton_layer.gt(0));
Map.addLayer(confident_cotton_layer, {min: 0.5, max: 1, palette: ['0000FF', '0000FF']}, label);
Map.addLayer(confidenceLayer, {}, 'confidence '+String(end_year));

var bucket = 'ecoshard-root';
Export.image.toCloudStorage(
  {
    'image': confident_cotton_layer,
    'description': label,
    'bucket': bucket,
    'fileNamePrefix': 'gee_export/'+label,
    'scale':300,
    'crs': "EPSG:4326",
    'maxPixels': 1e10,
});
