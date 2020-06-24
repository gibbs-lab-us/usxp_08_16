'''
Author: Matt Bougie (mbougie@wisc.edu)
Date: June 21, 2020
Purpose: Script to reclassify the MTR dataset to YTC/YFC.
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
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


def createReclassifyList(data, yxc):
	engine = create_engine('[[database_argument_string]]')
	query = " SELECT \"Value\", {} from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE {} IS NOT NULL".format(yxc, data['pre']['traj']['filename'],  data['pre']['traj']['lookup_name'], yxc)
	print 'query:', query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[]
	    value=row['Value'] 
	    mtr=row[yxc]  
	    templist.append(int(value))
	    templist.append(int(mtr))
	    fulllist.append(templist)
	print 'fulllist: ', fulllist
	return fulllist




def execute_task(args):
	in_extentDict, data, yxc, traj_list = args
	yxc_dict = {'ytc':3, 'yfc':4}

	fc_count = in_extentDict[0]

	procExt = in_extentDict[1]

	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	path_traj_rfnd = data['pre']['traj_rfnd']['path']
	print 'path_traj_rfnd:', path_traj_rfnd

	path_mtr = Raster(data['core']['path'])
	print path_mtr

	#set environments
	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	##  Execute the three functions  #####################
	raster_yxc = Reclassify(Raster(path_traj_rfnd), "Value", RemapRange(traj_list), "NODATA")
	raster_mask = Con((path_mtr == yxc_dict[yxc]) & (raster_yxc >= 2008), raster_yxc)
	raster_yxc = None
	filled_1 = Con(IsNull(raster_mask),FocalStatistics(raster_mask,NbrRectangle(3, 3, "CELL"),'MAJORITY'), raster_mask)
	raster_mask = None
	filled_2 = Con(IsNull(filled_1),FocalStatistics(filled_1,NbrRectangle(5, 5, "CELL"),'MAJORITY'), filled_1)
	filled_1 = None
	filled_3 = Con(IsNull(filled_2),FocalStatistics(filled_2,NbrRectangle(10, 10, "CELL"),'MAJORITY'), filled_2)
	filled_2 = None
	filled_4 = Con(IsNull(filled_3),FocalStatistics(filled_3,NbrRectangle(20, 20, "CELL"),'MAJORITY'), filled_3)
	filled_3 = None
	final = SetNull(path_mtr, filled_4, "VALUE <> {}".format(str(yxc_dict[yxc])))
	filled_4 = None


	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")

	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)
    
	final.save(outpath)

	outpath = None




def mosiacRasters(data, yxc):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['post'][yxc]['filename']
	print 'filename:', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['post'][yxc]['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['post'][yxc]['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['post'][yxc]['path'])







def run(data, yxc):

	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	traj_list = createReclassifyList(data, yxc)

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
    
	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=7)
	pool.map(execute_task, [(ed, data, yxc, traj_list) for ed in extDict.items()])
	pool.close()
	pool.join

	mosiacRasters(data, yxc)



if __name__ == '__main__':
	run(data, yxc)









