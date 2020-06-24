/*
  Author: Seth Spawn (spawn@wisc.edu)
  Date: June 27, 2019
  Purpose: Calculate frequency-weighted mean and standard deviation of the yield differencials for corn, soy,
           and weight. Frequency-weighting accounts acounts for the relative contribution of fields to annaul
           production (e.g. down-weighting infrequently planted fields and upweighting frequently planted 
           fields). 
  Usage: Can be run as is. Requires a date range to be specified (overwhich the frequency of a given crop is 
         calculated). Must be run iteratively to facilitate the aggregations necessary to calculate differentials 
         within 10km by 10km gridcells.
  Parameters: As shown. Date range can be changed in line 60 (when filtering the CDL)
*/

// function to calculate mean proportion with in 1km gridcells
function propTo1kmAsset(img, name){
  
  var prop = img.unmask().reduceResolution({
        reducer: ee.Reducer.mean(),
        maxPixels: 10000
      })
      prop = prop.updateMask(prop.gt(0))
      
  var bounds = mtr.geometry()
  
  Export.image.toAsset({
    image: prop,
    description: name,
    assetId: name,
    region: bounds,
    scale: 990,
    maxPixels: 150000000
  })
}

function sumTo10kmAsset(img, name){
  
  var sum = img.unmask().reduceResolution({
        reducer: ee.Reducer.sum(),
        maxPixels: 10000
      })
      sum = sum.updateMask(sum.gt(0))
      
  var bounds = mtr.geometry()
  
  Export.image.toAsset({
    image: sum,
    description: name,
    assetId: name,
    region: bounds,
    scale: 9900,
    maxPixels: 150000000
  })
}

// ==========================================================================================================
// calculate corn, soy and wheat frequency between 2008 and 2017 from CDL; mask to areas of s35 expansion.

// load layers
var mtr = ee.Image('users/spawnpac/s35_mtr_all')
var cdls = ee.ImageCollection('USDA/NASS/CDL').filterDate('2008-01-01', '2017-12-31')

// calculate corn frequency of occurance
var cornOcc = cdls.map(function(img){
  img = img.select('cropland')
  img = img.eq(1)
  return img.selfMask()
})
var cornMask = cornOcc.reduce(ee.Reducer.max()).updateMask(mtr.eq(3)).setDefaultProjection(mtr.projection())

// calculate soy frequency
var soyOcc = cdls.map(function(img){
  img = img.select('cropland')
  img = img.eq(5)
  return img.selfMask()
})
var soyMask = soyOcc.reduce(ee.Reducer.max()).updateMask(mtr.eq(3)).setDefaultProjection(mtr.projection())

// calculate wheat frequency
var wheatOcc = cdls.map(function(img){
  img = img.select('cropland')
  img = img.eq(22).or(img.eq(23)).or(img.eq(24))
  return img.selfMask()
})
var wheatMask = wheatOcc.reduce(ee.Reducer.max()).updateMask(mtr.eq(3)).setDefaultProjection(mtr.projection())

// export frequency rasters to 1km resolution assets showing the mean frequency of corn, soy, or wheat
propTo1kmAsset(cornMask, 'cornProp_1km')
propTo1kmAsset(wheatMask, 'wheatProp_1km')
propTo1kmAsset(soyMask, 'soyProp_1km')

// load 1km images
var soyProp1km = ee.Image('users/spawnpac/soyProp_1km')
var cornProp1km = ee.Image('users/spawnpac/cornProp_1km')
var wheatProp1km = ee.Image('users/spawnpac/wheatProp_1km')

// Aggregate to 10km resolution and export to assets.
sumTo10kmAsset(soyProp1km, 'soyProp_10km')
sumTo10kmAsset(cornProp1km, 'cornProp_10km')
sumTo10kmAsset(wheatProp1km, 'wheatProp_10km')

// load 10km resolution images
var soyProp10km = ee.Image('users/spawnpac/soyProp_10km')
var cornProp10km = ee.Image('users/spawnpac/cornProp_10km')
var wheatProp10km = ee.Image('users/spawnpac/wheatProp_10km')

// ==============================================================================

// function to calculate weight for weighted average
var calcWeight = function(img){
  var imgArea = img.multiply(ee.Image.pixelArea().multiply(0.0001))
  var sum = imgArea.reduceRegion({
    reducer: ee.Reducer.sum(),
    geometry: img.geometry(),
    scale: 9900,
    maxPixels: 1e12
  })
      sum = ee.Dictionary(sum).get('cropland_max')
  return imgArea.divide(ee.Number(sum))
}

// calculate frequency-weighted mean and standard deviation of yield differential
var weightedDiff = function(stable, expand, prop){
  
  // within a 10km by 10km gridcell calculate the local differential
  var localDiff = expand.subtract(stable).divide(stable)
  
  // calculate weights
  var imgArea = prop.multiply(ee.Image.pixelArea().multiply(0.0001))
  var sum = imgArea.reduceRegion({
    reducer: ee.Reducer.sum(),
    geometry: prop.geometry(),
    scale: 9900,
    maxPixels: 1e12
  })
      sum = ee.Dictionary(sum).get('cropland_max')
  
  var weights =  imgArea.divide(ee.Number(sum))
  
  // weighted mean
  var wMean = localDiff.multiply(weights).reduceRegion({
    reducer: ee.Reducer.sum(),
    geometry: prop.geometry(),
    scale: 9900,
    maxPixels: 1e12
  })
  
  // weighted standard deviation
  var M = localDiff.mask().reduceRegion({
    reducer: ee.Reducer.sum(),
    geometry: prop.geometry(),
    scale: 9900,
    maxPixels: 1e12
  })
  
      M = ee.Dictionary(M).get('classification')
  
  var wMean = ee.Dictionary(wMean).get('classification')
  var weightedSquaredDevs = localDiff.subtract(ee.Number(wMean)).pow(2).multiply(weights)
  
  var numerator = weightedSquaredDevs.reduceRegion({
    reducer: ee.Reducer.sum(),
    geometry: prop.geometry(),
    scale: 9900,
    maxPixels: 1e12
  })
    numerator = ee.Dictionary(numerator).get('classification')

  var weightSum = weights.reduceRegion({
    reducer: ee.Reducer.sum(),
    geometry: prop.geometry(),
    scale: 9900,
    maxPixels: 1e12
  })
      weightSum = ee.Dictionary(weightSum).get('cropland_max')
  var denominator = ee.Number(M).subtract(ee.Number(1)).divide(M).multiply(weightSum)
  
  // percent negative
  var negArea = prop.updateMask(localDiff.lt(0)).multiply(ee.Image.pixelArea().multiply(0.0001))
  var negSum = negArea.reduceRegion({
    reducer: ee.Reducer.sum(),
    geometry: prop.geometry(),
    scale: 9900,
    maxPixels: 1e12
  })
      negSum = ee.Dictionary(negSum).get('cropland_max')
  
  // print results
  print(ee.Number(wMean).multiply(100), 'weighted mean (%)')
  print(ee.Number(numerator).divide(ee.Number(denominator)).sqrt().multiply(100), 'weighted sd (%)')
  print(ee.Number(negSum).divide(ee.Number(sum)).multiply(100), '% negative')
  
  
}

// load layers
var soyStable = ee.Image('users/spawnpac/stableSoy_yield_9900m')
var soyExpand = ee.Image('users/spawnpac/expandSoy_yield_9900m')

var cornStable = ee.Image('users/spawnpac/stableCorn_yield_9900m')
var cornExpand = ee.Image('users/spawnpac/expandCorn_yield_9900m')

var wheatStable = ee.Image('users/spawnpac/stableCorn_yield_9900m')
var wheatExpand = ee.Image('users/spawnpac/expandCorn_yield_9900m')

var soyProp10km = ee.Image('users/spawnpac/soyProp_10km')
var cornProp10km = ee.Image('users/spawnpac/cornProp_10km')
var wheatProp10km = ee.Image('users/spawnpac/wheatProp_10km')

// calculate weighted differences
weightedDiff(soyStable, soyExpand, soyProp10km)
weightedDiff(cornStable, cornExpand, cornProp10km)
weightedDiff(wheatStable, wheatExpand, wheatProp10km)

