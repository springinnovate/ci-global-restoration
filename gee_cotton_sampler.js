for (var year = 2010; year <= 2020; year++) {
    var cdl_dataset = ee.ImageCollection('USDA/NASS/CDL')
                  .filter(ee.Filter.date(String(year)+'-01-01', String(year)+'-12-31'))
                  .first();
    var cropLandcoverLayer = cdl_dataset.select('cropland')
    var confidenceLayer = cdl_dataset.select('confidence');
    Map.addLayer(confidenceLayer.multiply(cropLandcoverLayer.eq(2)).gte(.9), {}, String(year)+' Confidence');
}