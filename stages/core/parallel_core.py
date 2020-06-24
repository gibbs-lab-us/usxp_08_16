'''
Author: Matt Bougie (mbougie@wisc.edu)
Date: June 21, 2020
Purpose: Script to reclassify the refined trajectory from pre-processing stage to 1 - 5 mtr classes.
		 Spatial filters are also applied in this script. 
Usage: Need to set up connections to geodatabases to reference datasets and store outputs.
       Also have to have a connection set up to PostgreSQL database to store attributes of rasters as tables
Parameters: Parameters for all scripts are stored in the json file and referenced as a global data object.
'''


import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen
import json


#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



def createReclassifyList(data):
    engine = create_engine('[[database_argument_string]]')
    query = " SELECT \"Value\", mtr from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])
    print 'query:', query
    df = pd.read_sql_query(query, con=engine)
    print df
    fulllist=[[0,0,"NODATA"]]
    for index, row in df.iterrows():
        templist=[]
        value=row['Value'] 
        mtr=row['mtr']  
        templist.append(int(value))
        templist.append(int(mtr))
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist

 



def execute_task(args):
	in_extentDict, data, traj_list = args

	filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
	filter_key = data['core']['filter']

	rg_combos = {'4w':["FOUR", "WITHIN"], '8w':["EIGHT", "WITHIN"], '4c':["FOUR", "CROSS"], '8c':["EIGHT", "CROSS"]}
	rg_instance = rg_combos[data['core']['rg']]

	# for count in masks_list:
	cond = "Count < " + str(gen.getPixelCount(str(data['global']['res']), int(data['core']['mmu'])))
	print 'cond: ',cond


	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	cdl=Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2012')
	

	if data['core']['route'] == 'r1':
		raster_yxc = Reclassify(Raster(data['pre']['traj_rfnd']['path']), "Value", RemapRange(traj_list), "NODATA")
		raster_filter = MajorityFilter(raster_yxc, filter_combos[filter_key][0], filter_combos[filter_key][1])
		raster_rg = RegionGroup(raster_filter, rg_instance[0], rg_instance[1],"NO_LINK")
		raster_mask = SetNull(raster_rg, 1, cond)
		raster_nbl = arcpy.sa.Nibble(raster_filter, raster_mask, "DATA_ONLY")

		#clear out the extent for next time
		arcpy.ClearEnvironment("extent")

		outname = "tile_" + str(fc_count) +'.tif'

		outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

		raster_nbl.save(outpath)


	elif data['core']['route'] == 'r2':
		##perform a majority filter on the refined trajectory 
		raster_filter = MajorityFilter(Raster(data['pre']['traj_rfnd']['path']), filter_combos[filter_key][0], filter_combos[filter_key][1])
		
        ####reclassify the filtered raster to the MTR labels
		raster_yxc = Reclassify(raster_filter, "Value", RemapRange(traj_list), "NODATA")
		raster_filter=None

		### perform region group on the raster_yxc to get the number of pixels for each region
		raster_rg = RegionGroup(raster_yxc, rg_instance[0], rg_instance[1], "NO_LINK")
		
		### set null the regions that are less than the mmu treshold
		raster_mask = SetNull(raster_rg, raster_yxc, cond)
		raster_yxc=None
		raster_rg=None

		### fill in the regions that were below the mmu threshold.  Perform these in series starting small (taking advantage of spatial autocorelation) and expanding out to get 
		### past the equilibrium sink of a certain kernel size
		filled_1 = Con(IsNull(raster_mask),FocalStatistics(raster_mask,NbrRectangle(3, 3, "CELL"),'MAJORITY'), raster_mask)
		raster_mask=None
		filled_2 = Con(IsNull(filled_1),FocalStatistics(filled_1,NbrRectangle(5, 5, "CELL"),'MAJORITY'), filled_1)
		filled_1=None
		filled_3 = Con(IsNull(filled_2),FocalStatistics(filled_2,NbrRectangle(10, 10, "CELL"),'MAJORITY'), filled_2)
		filled_2=None
		filled_4 = Con(IsNull(filled_3),FocalStatistics(filled_3,NbrRectangle(20, 20, "CELL"),'MAJORITY'), filled_3)
		filled_3=None

		##### trim of the patches that have expanded past the cdl map boundaries  #####################
		cond = "Value = 0"
		raster_mask = SetNull(cdl, filled_4, cond)
		filled_4=None


        ##### create a tiff file from the raster object  #######################################
		#clear out the extent for next time
		arcpy.ClearEnvironment("extent")

		outname = "tile_" + str(fc_count) +'.tif'

		outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

		raster_mask.save(outpath)
        raster_mask=None






def mosiacRasters(data):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['core']['filename']
	print 'filename:-----------------------------', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['core']['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['core']['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['core']['path'])







def run(data):

	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	traj_list = createReclassifyList(data)
	
	fishnet = 'fishnet_cdl_7_7_subset'

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\{}'.format(fishnet), ["oid","SHAPE@"]):
		atlas_stco = row[0]
		print atlas_stco
		extent_curr = row[1].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[atlas_stco] = ls

	print 'extDict', extDict
	print'extDict.items',  extDict.items()

	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=7)
	pool.map(execute_task, [(ed, data, traj_list) for ed in extDict.items()])
	pool.close()
	pool.join

	mosiacRasters(data)


if __name__ == '__main__':
	run(data)
