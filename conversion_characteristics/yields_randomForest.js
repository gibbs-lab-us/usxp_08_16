/*
  Author: Seth Spawn (spawn@wisc.edu)
  Date: Nov 19, 2018 
  Purpose: Generates covariate image for random forest corn, soy, or wheat prediction. Applies random forest
           model to image to spatialy predict representative corn, soy, or wheat yields. Gives wall-to-wall 
           map of predicted representative yields. Random forest paremeters were determined using the randomForest 
           package in R.
  Usage: Can be run as is. Representative date range can be adjusted by providing alternative years to the 
         rfImage() function.
  Parameters: yearStart: first year of representative yield date range
              yearEnd: last year of representative yield date range.
*/

// function for generating the image to which the random forest model will be applie
function rfImage(yearStart, yearEnd){

  // load dem and calculate terrain variables
  var dem = ee.Image('USGS/NED');
  var terrain = ee.Terrain.products(dem);

  // load nccpi variables, mask, and rename
  var nccpiSG = ee.Image('users/spawnpac/conus_30mINT16_nccpi2sg_0nulls');
      nccpiSG = nccpiSG.updateMask(nccpiSG.neq(0)).rename('nccpiSG');

  var nccpiCS = ee.Image('users/spawnpac/conus_30mINT16_nccpi2cs_0nulls');
      nccpiCS = nccpiCS.updateMask(nccpiCS.neq(0)).rename('nccpiCS');

  // load terra climate as image collection and filter by date
  var terrac = ee.ImageCollection('IDAHO_EPSCOR/TERRACLIMATE');
      terrac = terrac.filterDate(ee.Date.fromYMD(yearStart, 1, 1), ee.Date.fromYMD(yearEnd, 12, 31));

  // add month band for later extraction
      terrac = terrac.map(function(img){
        var month = img.date().get('month');
        return img.set('month', month);
      });
  
  // extract images by month, and calculate multiyear mean for that month
  var months = ee.List.sequence(1,12);
      months = ee.ImageCollection.fromImages(months.map(function(m){
        return terrac.filterMetadata('month', 'equals', m).mean()
      }))

  // calculate long term annual mean, sum, min, and max
  var terracSum = months.reduce(ee.Reducer.sum()); 
  var terracMean = months.reduce(ee.Reducer.mean());
  var terracMinMax = months.reduce(ee.Reducer.minMax());
  
  // combined all images and return the multiband image
  var image = nccpiCS.addBands(nccpiSG).addBands(terrain).addBands(terracSum).addBands(terracMean).addBands(terracMinMax);

  return image;
}

var img = rfImage(2008, 2017);

//-------------------------------------------------------------------------------------------------//

// Training data for soy
var training = ee.FeatureCollection('ft:1u7rh3MvIfnkETeQ9oKWxAq3Pe5ht9KHMXDe-bDMm')

// Randomforest model (parameters refined in R)
var classifier = ee.Classifier.randomForest({numberOfTrees:250, variablesPerSplit: 21, minLeafPopulation: 5, outOfBagMode:true})
                              .train(training, 'yield', img.bandNames()).setOutputMode('REGRESSION');

// Create map from classifier
var classified = img.select(img.bandNames().remove('yield')).classify(classifier)

var bounds = ee.Image('USDA/NASS/CDL/2017').geometry()
var conusLand = ee.Image('USDA/NASS/CDL/2017').select('cultivated').mask()

Export.image.toAsset({
  image: classified.updateMask(conusLand).multiply(10).uint16(),
  description: 'soyYield_rfPred_2008to2017_x10_masked',
  assetId: 'soyYield_rfPred_2008to2017_x10_masked',
  region: bounds,
  scale: 30,
  maxPixels: 15000000000
})

//-------------------------------------------------------------------------------------------------//

// Training data for corn
var training = ee.FeatureCollection('ft:1sJbj480WF5KZaMMDUlXDJ4UkepMAzRA7onOxEbFb')

// Randomforest model (parameters refined in R)
var classifier = ee.Classifier.randomForest({numberOfTrees:250, variablesPerSplit: 21, minLeafPopulation: 5, outOfBagMode:true})
                              .train(training, 'yield', img.bandNames()).setOutputMode('REGRESSION');

// Create map from classifier
var classified = img.select(img.bandNames().remove('yield')).classify(classifier)

//Map.addLayer(classified, {min:50, max: 250, palette: ['blue', 'green', 'yellow', 'orange', 'red']})
var bounds = ee.Image('USDA/NASS/CDL/2017').geometry()

var cultivated = ee.Image('USDA/NASS/CDL/2017').select('cultivated')

Export.image.toAsset({
  image: classified.updateMask(conusLand).multiply(10).uint16(),
  description: 'cornYield_rfPred_2008to2017_x10_masked',
  assetId: 'cornYield_rfPred_2008to2017_x10_masked',
  region: bounds,
  scale: 30,
  maxPixels: 15000000000
})

//-------------------------------------------------------------------------------------------------//

// Training data for wheat
var training = ee.FeatureCollection('ft:1WwSKqERFrNjRouac-kBG0ZfxWhqTKAZLQQsp3lFN')

// Randomforest model (parameters refined in R)
var classifier = ee.Classifier.randomForest({numberOfTrees:250, variablesPerSplit: 21, minLeafPopulation: 5, outOfBagMode:true})
                              .train(training, 'yield', img.bandNames()).setOutputMode('REGRESSION');

// Create map from classifier
var classified = img.select(img.bandNames().remove('yield')).classify(classifier)

var bounds = ee.Image('USDA/NASS/CDL/2017').geometry()

var cultivated = ee.Image('USDA/NASS/CDL/2017').select('cultivated')

Export.image.toAsset({
  image: classified.updateMask(conusLand).multiply(10).uint16(),
  description: 'wheatYield_rfPred_2008to2017_x10_masked',
  assetId: 'wheatYield_rfPred_2008to2017_x10_masked',
  region: bounds,
  scale: 30,
  maxPixels: 15000000000
})
