'''
Author: Matt Bougie (mbougie@wisc.edu)
Date: June 21, 2020
Purpose: Script used to mask out false postive pixels in the YTC dataset by leveraging the CDL datasets. 
Usage: Need to set up connections to geodatabases to reference datasets and store outputs.
       Also have to have a connection set up to postgres database to store attributes of rasters as tables
Parameters: Parameters for all scripts are stored in the json file and referenced as a global data object.
'''


import sys
import os
#import modules from other folders
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import arcpy
from arcpy import env
from arcpy.sa import *
import glob

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
import general as gen
import json
import fnmatch
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
import multiprocessing




arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


def createReclassifyList(conn, data):
	cur = conn.cursor()

	query = "SELECT \"Value\", ytc from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc IS NOT NULL".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])
	print 'query:', query

	cur.execute(query)
	#create empty list
	fulllist=[[0,0,"NODATA"]]

	### fetch all rows from table
	rows = cur.fetchall()

	return rows




def getNonCropList(conn):
	cur = conn.cursor()

	query = "SELECT value FROM misc.lookup_cdl WHERE b = '0'"
	print 'query:', query

	cur.execute(query)

	### fetch all rows from table
	rows = cur.fetchall()

	### use list comprehension to convert list of tuples to list
	noncrop_list = [i[0] for i in rows]
	print 'noncrop_list:', noncrop_list
	
	return noncrop_list




def getCropList(conn):
	cur = conn.cursor()

	query = "SELECT value FROM misc.lookup_cdl WHERE b = '1' ORDER BY value"
	print 'query:', query

	cur.execute(query)

	### fetch all rows from table
	rows = cur.fetchall()

	### use list comprehension to convert list of tuples to list
	crop_list = [i[0] for i in rows]
	print 'crop_list:', crop_list
	
	return crop_list

		



def execute_task(args):
	in_extentDict, data, traj_list, noncroplist, croplist, cls, rws = args

	fc_count = in_extentDict[0]

	procExt = in_extentDict[1]
	# ##print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.snapRaster = data['pre']['traj']['path']
	arcpy.env.cellsize = data['pre']['traj']['path']
	arcpy.env.outputCoordinateSystem = data['pre']['traj']['path']	
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	print 'rws==================================',rws
	print 'cls==================================',cls

	outData = np.zeros((rws, cls), dtype=np.uint8)
    
    ### create numpy arrays for input datasets cdls and traj
	cdls = {
			2008:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2008', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2009:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2009', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2010:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2010', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2011:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2011', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2012:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2012', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2013:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2013', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2014:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2014', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2015:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2015', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2016:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2016', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2017:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	       }
	
	arr_traj = arcpy.RasterToNumPyArray(in_raster=data['pre']['traj']['path'], lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)

	# find the location of each pixel labeled with specific arbitray value in the rows list  
	for row in traj_list:
		traj_value = row[0]
		#year of conversion for either expansion or abandonmen
		ytx = row[1]
		##print 'ytx', ytx

		#year before conversion for either expansion or abandonment
		ybx = row[1]-1
		##print 'ybx', ybx

		#Return the indices of the pixels that have values of the ytc arbitrsy values of the traj
		indices = (arr_traj == row[0]).nonzero()

		#stack indices so easier to work with
		stacked_indices=np.column_stack((indices[0],indices[1]))
        
        #get the x and y location of each pixel that has been selected from above
		for pixel_location in stacked_indices:
			row = pixel_location[0] 
			col = pixel_location[1]

			##print '---------- new pixel to analyze -----------------------------------'
			#get the pixel value for ybx
			pixel_value_ybx =  cdls[ybx][row][col]

			#get the pixel value for ytx
			pixel_value_ytx =  cdls[ytx][row][col]

			#### find the years stil before in the time series for this pixel location
			years_before_list = [i for i in data['global']['years'] if i < ytx]

			list_before = []
			for year in years_before_list:
				list_before.append(cdls[year][row][col])
			
			list_kernel = list_before[:-2]

			#### find the value of the years after cy
			years_after_list = [i for i in data['global']['years'] if i >= ytx]

			list_after = []	
			for year in years_after_list:
				list_after.append(cdls[year][row][col])

			list_entire=list_before+list_after

            #### define lists that contain lcc that pixel will be compared to
			fuzzylist=[36,37,61,152,176]
			fuzzycroplist = [58,59,60]
			fruitlist=[66,67,68,69,71,72,74,75,76,77,204,210,211,212,218,220,223]

            #### fruit mask #############################
			if(np.isin(list_before, fruitlist).any() and np.isin(list_after, fruitlist).any()):
				outData[row,col] = 201

			#### rice ####################################
			#####needs to have rice before AND after to be considered false conversion
			if(np.isin(list_before, [3]).any()) and (np.isin(list_after, [3]).any()):
				outData[row,col] = 204

			####  dev mask  ############################
			#### logic: dev never converted to crop 
			if np.isin(list_before, [122,123,124]).any():
				outData[row,col] = 101

			#### fuzzy filter ##########################
			if(np.isin(list_entire, fuzzylist).all()):
				outData[row,col] = 102

			if(np.isin(list_after, noncroplist + [61]).all()):
				outData[row,col] = 103

			if(np.isin(list_after, noncroplist + [36]).all()):
				outData[row,col] = 104

			if(pixel_value_ytx in fuzzycroplist):
				outData[row,col] = 105



	arcpy.ClearEnvironment("extent")

	outname = "tile_" + str(fc_count) +'.tif'
	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

	myRaster = arcpy.NumPyArrayToRaster(outData, lower_left_corner=arcpy.Point(XMin, YMin), x_cell_size=30, y_cell_size=30, value_to_nodata=0)

	##free memory from outdata array
	outData = None

	myRaster.save(outpath)

	myRaster = None






def mosiacRasters(data):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])
	print 'inTraj:', inTraj

	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['refine']['gdb'], data['refine']['mask_fp_ytc_preview']['filename'], inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['refine']['mask_fp_ytc_preview']['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['refine']['mask_fp_ytc_preview']['path'])




def reclassRaster(data):

    # Reclassify (in_raster, reclass_field, remap, {missing_values})
	outReclass1 = Reclassify(in_raster=data['refine']['mask_fp_ytc_preview']['path'], reclass_field="Value", remap=RemapValue([[101,1],[102,1],[103,1],[104,1],[105,1],[201,249],[204,249]]))
	outReclass1.save(data['refine']['mask_fp_ytc']['path'])

	# Overwrite pyramids
	gen.buildPyramids(data['refine']['mask_fp_ytc']['path'])





def run(data):
	conn = psycopg2.connect('[[database_argument_string]]')

	print "mask mutiple masks-----------"
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	traj_list = createReclassifyList(conn, data)

	noncroplist = getNonCropList(conn)

	croplist = getCropList(conn)


	fishnet = 'fishnet_cdl_49_7'
	####  NEED TO DEVELOP THIS METHOD  #################################
	cls = data['ancillary']['tiles'][fishnet]['cls']
	rws = data['ancillary']['tiles'][fishnet]['rws']

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
	pool = Pool(processes=8)
	pool.map(execute_task, [(ed, data, traj_list, noncroplist, croplist, cls, rws) for ed in extDict.items()])
	pool.close()
	pool.join

	#close connection for this client
	conn.close ()

	mosiacRasters(data)

	reclassRaster(data)


if __name__ == '__main__':
	run(data)
   
