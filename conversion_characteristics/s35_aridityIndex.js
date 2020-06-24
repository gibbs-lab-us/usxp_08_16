/*
  Author: Seth Spawn (spawn@wisc.edu)
  Date: May 14, 2019
  Purpose: Creates an image reporting the mean climate water deficit and additional aridity measures
           from the TerraClimate database over a specified range of years. Images are then summarized by
           MTR (Lark et al. 2020) for the entire nation and exported as tables.
  Usage: Can be run as is. Representative date range can be adjusted by providing alternative years to the 
         defImage() function.
  Parameters: yearStart: first year of representative yield date range
              yearEnd: last year of representative yield date range.
*/

// function to get mean aridity index from TerraClimate
function defImage(yearStart, yearEnd){

  // load terra climate as image collection and filter by date
  var terrac = ee.ImageCollection('IDAHO_EPSCOR/TERRACLIMATE');
      terrac = terrac.filterDate(ee.Date.fromYMD(yearStart, 1, 1), ee.Date.fromYMD(yearEnd, 12, 31));

  // add month band for later extraction
      terrac = terrac.map(function(img){
        var year = img.date().get('year');
        return img.select(['aet', 'def', 'pdsi', 'pet', 'pr']).set('year', year);
      });
      
  // extract images by month, and calculate multiyear mean for that month
  var years = ee.List.sequence(yearStart,yearEnd);
      years = ee.ImageCollection.fromImages(years.map(function(y){
        return terrac.filterMetadata('year', 'equals', y).sum();
      }));

  return ee.Image(years.reduce(ee.Reducer.mean()));
}

// calculate mean aridity index between 2008 and 2017
var img = defImage(2008, 2017);

// extract band names
var bandNames = img.bandNames()

// load USXP s35 dataset
var mtr = ee.Image('users/spawnpac/s35_mtr_all');

// mask aridity image seperately to (i) stable cropland and (ii) areas of cropland expansion
var mtr2 = img.updateMask(mtr.eq(2)).rename(bandNames.map(function(n){return ee.String(n).cat('_mtr2')})); // stable crop
var mtr3 = img.updateMask(mtr.eq(3)).rename(bandNames.map(function(n){return ee.String(n).cat('_mtr3')})); // crop expansion

// combine masked images for tabulation
var combined = mtr2.addBands(mtr3);

// calculate the national mean
var summary = combined.reduceRegion({
  reducer: ee.Reducer.mean(),
  geometry: mtr.geometry(),
  scale: 30,
  maxPixels: 500000000000
});
    summary = ee.FeatureCollection([ee.Feature(null, summary)]);

// export tabulation as CSV
Export.table.toDrive({
  collection: summary,
  description: 'cwd_mtrs23_mean',
  fileNamePrefix: 'cwd_mtrs23_mean',
  fileFormat: 'CSV'
})