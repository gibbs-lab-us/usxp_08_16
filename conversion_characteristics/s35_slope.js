/*
  Author: Seth Spawn (spawn@wisc.edu)
  Date: Jan 10, 2019 
  Purpose: Calculate slope from USGS/NED within areas of cropland expansion and stable cropland. Tabulate
           nationwide averages and export aggregated images for visualization.
  Usage: Can be run as is but must be run iteratively to aggregate images. First iteration aggregates 30m 
         image to 300m and saves as asset. Second iteration aggregates 300m asset to 3000m and exports to 
         drive. Iteration necessary to overcome memory issues.
  Parameters: As shown.
*/

// load USXP s35 dataset
var s35 = ee.Image('users/spawnpac/s35_mtr_all')

// load USGS NED DEM; reproject to match s35
var ned = ee.Image('USGS/NED')
  .reduceResolution(ee.Reducer.mean())
  .reproject({crs: s35.projection(), scale: 30})

// extract slope for areas of expansion
var exp_slope = ee.Terrain.slope(ned)
  .updateMask(s35.eq(3))

// export expansion slope as an asset
Export.image.toAsset({
  image: exp_slope,
  description: 's35_exp_slope_30m',
  assetId: 's35_exp_slope_30m',
  region: s35.geometry().bounds(),
  scale: 30,
  maxPixels: 35421888477
})

// extract for areas of stable cropland
var stb_slope = ee.Terrain.slope(ned)
  .updateMask(s35.eq(2))

// export stable cropland slope as an asset
Export.image.toAsset({
  image: stb_slope,
  description: 's35_stb_slope_30m',
  assetId: 's35_stb_slope_30m',
  region: s35.geometry().bounds(),
  scale: 30,
  maxPixels: 35421888477
})

// tabulate nationwide mean of slope within areas of expansion; export as table
var exp_mean = exp_slope.reduceRegion({
  reducer: ee.Reducer.mean(),
  geometry: s35.geometry().bounds(),
  scale: 30,
  maxPixels: 50000000000
})
    exp_mean = ee.FeatureCollection([ee.Feature(null, exp_mean)])

Export.table.toDrive({
  collection: exp_mean,
  description: 'exp_slope_mean',
  fileNamePrefix: 'exp_slope_mean',
  fileFormat: 'CSV'
})

// tabulate nationwide standard deviation of slope within areas of expansion; export as table
var exp_sd = exp_slope.reduceRegion({
  reducer: ee.Reducer.stdDev(),
  geometry: s35.geometry().bounds(),
  scale: 30,
  maxPixels: 50000000000
})
    exp_sd = ee.FeatureCollection([ee.Feature(null, exp_sd)])

Export.table.toDrive({
  collection: exp_sd,
  description: 'exp_slope_sd',
  fileNamePrefix: 'exp_slope_sd',
  fileFormat: 'CSV'
})

// ---------------------------------------------------------------
// aggreggate expansion slope image for visualization (first to 300m)
var slope = ee.Image('users/spawnpac/s35_exp_slope_30m')
  .reduceResolution({
    reducer: ee.Reducer.mean(),
    maxPixels: 10000
  })
  .reproject({crs: s35.projection(), scale: 300})
    slope = slope.updateMask(slope.gt(0))

Export.image.toAsset({
  image: slope,
  description: 's35_exp_slope_300m',
  assetId: 's35_exp_slope_300m',
  region: s35.geometry().bounds(),
  scale: 300,
  maxPixels: 35421888477
})

// aggreggate expansion slope image for visualization (now to 3000m); export to drive
var slope = ee.Image('users/spawnpac/s35_exp_slope_300m')
  .reduceResolution({
    reducer: ee.Reducer.mean(),
    maxPixels: 10000
  })
  .reproject({crs: s35.projection(), scale: 3000})
    slope = slope.updateMask(slope.gt(0))

Map.addLayer(slope, {min:0, max:10, palette: ['yellow', 'brown']})

Export.image.toDrive({
    image: slope,
    description: 's35_exp_slope_3000m',
    fileNamePrefix: 's35_exp_slope_3000m',
    region: s35.geometry().bounds(),
    fileFormat: 'GeoTIFF',
    scale: 3000,
    maxPixels: 150000000
  })
  
