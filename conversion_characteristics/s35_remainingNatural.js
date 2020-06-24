/*
  Author: Seth Spawn (spawn@wisc.edu)
  Date: May 14, 2019 
  Purpose: Calculate percent of aggregrated gridcells that remains in natural cover.
  Usage: Can be run as is but must be run iteratively. Due to memory constraints. The first run aggregates
         to a 990m resolution and saves as an asset. The next run aggregates to 9900m resolution and both
         saves as an asset and exports a GeoTIFF.
  Parameters: As shown.
*/

// function to aggregate image (by counting contained pixels) and export as asset
function aggCount(img, res, name){
  
  var cnt = img.reduceResolution({
        reducer: ee.Reducer.sum(),
        maxPixels: 10000
      }).reproject({crs: img.projection(), scale: res}).unmask()

  var bounds = mtr.geometry()

  Export.image.toAsset({
    image: cnt,
    description: name,
    assetId: name,
    region: bounds,
    scale: res,
    maxPixels: 150000000
  })
}

// function to export 9900m image to drive 
function exportImg(img, name){
  
  var bounds = img.geometry()
  
  Export.image.toDrive({
    image: img,
    description: name,
    fileNamePrefix: name,
    region: bounds,
    fileFormat: 'GeoTIFF',
    scale: 9900,
    maxPixels: 150000000
  })
  
}

// ====================================================================================================

// load CDL and USXP dataset
var cdl = ee.Image(ee.ImageCollection('USDA/NASS/CDL').filterDate('2016-01-01', '2016-12-31').first()).select('cropland')
var mtr = ee.Image('users/spawnpac/s35_mtr_all')

// keep all except water classes (this image will be used to calculate the denominator of %remaining); mask
var rmv = ee.List([81, 83, 88, 92, 111])
var cdl_rmv = cdl.remap(rmv, ee.List.repeat(1, ee.Number(rmv.length())))
var area_tot = cdl.mask().updateMask(cdl_rmv.mask().not())

// keep mtr1 for natural classes but not water (this will be the numerator)
var nat = ee.List([37, 62, 63, 64, 87, 112,  131, 141, 142, 143, 152, 171, 181, 176, 190, 195])
var cdl_keep = cdl.remap(nat, ee.List.repeat(1, ee.Number(nat.length())))

// get mtr 1 image (stable non-crop)
var mtr1 = mtr.updateMask(mtr.eq(1))
    mtr1 = mtr1.updateMask(cdl_rmv.mask().not())
    mtr1 = mtr1.updateMask(cdl_keep.mask())

// aggregated to 990m resolution (need to use two steps [990m and then 9900m] to avoid memmory issues)
aggCount(mtr1.mask(), 990, 'remainNat_mtr1_990_mask') // mask to get values of 0/1 for sum
aggCount(area_tot.mask(), 990, 'remainNat_area_tot_990_mask')

// load 990m images
var mtr1_990 = ee.Image('users/spawnpac/remainNat_mtr1_990_mask')
var area_tot_990 = ee.Image('users/spawnpac/remainNat_area_tot_990_mask')

// aggregate further to 9900m
aggCount(mtr1_990, 9900, 'remainNat_mtr1_9900')
aggCount(area_tot_990, 9900, 'remainNat_area_tot_9900')

// load the 9900m images
var mtr1_9900 = ee.Image('users/spawnpac/remainNat_mtr1_9900')
var area_tot_9900 = ee.Image('users/spawnpac/remainNat_area_tot_9900')

// export 9900m images to drive
exportImg(mtr1_9900, 'remainNat_mtr1_9900')
exportImg(area_tot_9900, 'remainNat_area_tot_9900')