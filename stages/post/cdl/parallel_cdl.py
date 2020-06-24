'''
Author: Matt Bougie (mbougie@wisc.edu)
Date: June 21, 2020
Purpose: Script to reclassify the MTR dataset to FC/FNC.
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

sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\cdl\\')
import replace_61_w_hard_crop


#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 




def createReclassifyList(data, yxc):
	engine = create_engine('[[database_argument_string]]')
	query = " SELECT \"Value\", {} from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE {} IS NOT NULL".format(yxc, data['pre']['traj']['filename'],  data['pre']['traj']['lookup_name'], yxc)
	# print 'query:', query
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


  
# def execute_task(in_extentDict):
def execute_task(args):
	in_extentDict, data, yxc, subtype, traj_list = args
	yxc_dict = {'ytc':3, 'yfc':4}


	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	path_traj_rfnd = data['pre']['traj_rfnd']['path']
	print 'path_traj_rfnd:', path_traj_rfnd
    
    ## the finished mtr product datset
	path_mtr = Raster(data['core']['path'])

	#set environments
	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	##  Execute the three functions  #####################

	## this is the base yxc dataset derived from trajectory (speckles)
	raster_yxc = Reclassify(Raster(path_traj_rfnd), "Value", RemapRange(traj_list), "NODATA")
    
	## clean the speckles so only left with yxc where the mtr regions fullfilling mmu requirement
	yxc_inraster = Con((path_mtr == yxc_dict[yxc]) & (raster_yxc >= 2008), raster_yxc)
	raster_mask = yxc_inraster
	##delete object for memory
	raster_yxc = None

	###get the cdl path object from current instance to loop through each of the cdl paths in the object
	for year, cdlpath in data['post'][yxc][subtype]['cdlpaths'].iteritems():

		print year, cdlpath

		# allow raster to be overwritten
		arcpy.env.overwriteOutput = True
		print "overwrite on? ", arcpy.env.overwriteOutput

		#establish the condition
		cond = "Value = " + year
		print 'cond: ', cond
        
        ## replace the yxc year value with the appropriate cdl value for that given year
		raster_mask = Con(raster_mask, cdlpath, raster_mask, cond)
    

	raster_mask = replace_61_w_hard_crop.run(data, yxc_inraster, raster_mask, XMin, YMin, XMax, YMax)


    ####fill in the null values ####################################
	filled_1 = Con(IsNull(raster_mask),FocalStatistics(raster_mask,NbrRectangle(3, 3, "CELL"),'MAJORITY'), raster_mask)
	raster_mask=None
	filled_2 = Con(IsNull(filled_1),FocalStatistics(filled_1,NbrRectangle(5, 5, "CELL"),'MAJORITY'), filled_1)
	filled_1=None
	filled_3 = Con(IsNull(filled_2),FocalStatistics(filled_2,NbrRectangle(10, 10, "CELL"),'MAJORITY'), filled_2)
	filled_2=None
	filled_4 = Con(IsNull(filled_3),FocalStatistics(filled_3,NbrRectangle(20, 20, "CELL"),'MAJORITY'), filled_3)
	filled_3=None
	final = SetNull(path_mtr, filled_4, "VALUE <> {}".format(str(yxc_dict[yxc])))
	filled_4 = None

	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

	arcpy.ClearEnvironment("extent")

	# final.save(outpath)
	raster_mask.save(outpath)
	raster_mask=None

	outpath=None
	final=None







def mosiacRasters(data, yxc, subtype):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['post'][yxc][subtype]['filename']
	print 'filename:', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['post'][yxc]['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['post'][yxc][subtype]['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['post'][yxc][subtype]['path'])





def run(data, yxc, subtype):

	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	traj_list = createReclassifyList(data, yxc)
	
	fishnet = 'fishnet_cdl_7_7_subset_yxc'

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
	pool = Pool(processes=4)
	pool.map(execute_task, [(ed, data, yxc, subtype, traj_list) for ed in extDict.items()])
	pool.close()
	pool.join

	mosiacRasters(data, yxc, subtype)



if __name__ == '__main__':
	run(data, yxc, subtype)