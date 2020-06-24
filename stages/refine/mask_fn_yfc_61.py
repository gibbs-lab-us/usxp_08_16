'''
Author: Matt Bougie (mbougie@wisc.edu)
Date: June 21, 2020
Purpose: Script used to mask out false negative pixels associated with CDL lcc 61 (fallow) in the YFC dataset by leveraging the CDL datasets. 
Usage: Need to set up connections to geodatabases to reference datasets and store outputs.
       Also have to have a connection set up to postgres database to store attributes of rasters as tables
Parameters: Parameters for all scripts are stored in the json file and referenced as a global data object.
'''




import sys
import os

#import modules from other folders
sys.path.append('C:\\Users\\Bougie\\Desktop\\scripts\\usxp\\misc\\')
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
import collections


arcpy.CheckOutExtension("Spatial")

arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


def createReclassifyList(conn, data):
	cur = conn.cursor()
	
	###mtr2 = cropped
	###mtr5 = intermittent
	query = "SELECT \"Value\", mtr, a.traj_array from pre.{} as a JOIN pre.v4_traj_lookup_2008to2017_v3 as b ON a.traj_array = b.traj_array WHERE mtr=2 or mtr=5".format(data['pre']['traj']['filename'])
	print 'query:', query

	cur.execute(query)
	#create empty list
	fulllist=[[0,0,"NODATA"]]

	# fetch all rows from table
	rows = cur.fetchall()
	print rows
	print 'number of records in lookup table', len(rows)
	return rows


def getCropList(conn, binary):
	cur = conn.cursor()

	query = "SELECT value FROM misc.lookup_cdl WHERE b = '{}' ORDER BY value".format(binary)
	print 'query:', query

	cur.execute(query)

	### fetch all rows from table
	rows = cur.fetchall()

	### use list comprehension to convert list of tuples to list
	crop_list = [i[0] for i in rows]
	print 'crop_list:', crop_list
	
	### Note: 36 and 61 are added to the noncrop list.
	return crop_list


def getIndex(traj_array_before):
	###this is a function to determine the conversion year of the trajectory 
	# print '--------inside getCY() function------------------------'
	traj_array_before_size = len(traj_array_before)
	# print 'traj_array_before_size', traj_array_before_size
	count_zeros = traj_array_before.count(0)
	# print 'count_zeros', count_zeros
	index = traj_array_before_size - count_zeros
	# print 'index', index
	return index
	

def execute_task(args):
	in_extentDict, data, traj_list, noncroplist, croplist, cls, rws = args

	fc_count = in_extentDict[0]
	
	###get the extent from the SPECIFIC tile
	procExt = in_extentDict[1]
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.snapRaster = data['pre']['traj']['path']
	arcpy.env.cellsize = data['pre']['traj']['path']
	arcpy.env.outputCoordinateSystem = data['pre']['traj']['path']	

	###set the extent for this SPECIFIC tile 
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)


	print 'rws==================================',rws
	print 'cls==================================',cls
	

	##create an empty matrix that will be filled with values using the code below if conditions are met
	outData = np.zeros((rws, cls), dtype=np.uint16)
	print 'outdata', outData
    
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

	###this is the trajectory that is referenced with the specific trajectory values we are looking for from the sql statement above
	arr_traj = arcpy.RasterToNumPyArray(in_raster=data['pre']['traj']['path'], lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)

	#### find the location of each pixel labeled with specific arbitray value in the rows list  
	#### note the traj_list is derived from the sql query above
	for traj in traj_list:
		###
		traj_value = traj[0]
		mtr = traj[1]
		traj_array = traj[2]

		#Return the indices of the pixels that have values of the ytc arbitray values of the traj.
		indices = (arr_traj == traj_value).nonzero()

		#stack the indices variable above so easier to work with
		stacked_indices=np.column_stack((indices[0],indices[1]))
        
        #####  get the x and y location of each pixel that has been selected from above
		for pixel_location in stacked_indices:

			row = pixel_location[0] 
			col = pixel_location[1]


			### attach the cdl values to each binary trajectory at each pixel location
			entirelist = []
			for year in data['global']['years']:
				entirelist.append(cdls[year][row][col])

			### if 61 in the entirelist AND the entirelist does not start with 61 then process the entirelist otherwise skip processing of pixel
			if(61 in entirelist):

				# print 'entirelist---', entirelist
				current_index=entirelist.index(61)

				beforelist=np.array(entirelist[:current_index])
				# print 'beforelist---', beforelist

				afterlist=np.array(entirelist[current_index:])
				# print 'afterlist---', afterlist

				### remove the first for elements from traj_array
				traj_array_before = traj_array[:current_index]

				##make sure that there are at least two elements in the beforelist(i.e 2 elements before the first 61)-----length of 2 means yfc is 2010
				if(beforelist.size >= 2):
					
					####  Conditions  ################################################
					## only uniques elements -1 and 0 in numpy diff list to make sure change is only in one direction (ie 1 to 0)
					cond1 =  np.isin(np.diff(traj_array_before), [-1,0]).all()
					
					## make sure that the first two elements in beforelist are crop ----- this works with cond1
					cond2 = traj_array_before[0] == 1 and traj_array_before[1] == 1
					
					## make sure that afterlist contains only noncrop and 61
					cond3 = np.isin(afterlist, noncroplist + [61]).all()

					## make sure that the afterlist length is greater than 1
					cond4 = afterlist.size > 1

					if(cond1 and cond2 and cond3 and cond4):
	
						#####  label the pixel with conversion year  ##############################################
						outData[row,col] = data['global']['years'][getIndex(traj_array_before)]



    

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
	arcpy.MosaicToNewRaster_management(tilelist, data['refine']['gdb'], data['refine']['mask_fn_yfc_61_preview']['filename'], inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['refine']['mask_fn_yfc_61_preview']['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['refine']['mask_fn_yfc_61_preview']['path'])





def reclassRaster(data):
	print data["refine"]["list_arb_yfc"]

	outReclass1 = Reclassify(in_raster=data['refine']['mask_fn_yfc_61_preview']['path'], reclass_field="Value", remap=RemapValue(data["refine"]["list_arb_yfc"]))
	outReclass1.save(data['refine']['mask_fn_yfc_61']['path'])

	# Overwrite pyramids
	gen.buildPyramids(data['refine']['mask_fn_yfc_61']['path'])










def run(data):
	conn = psycopg2.connect('[[database_argument_string]]')
	
	print "mask nlcd------------"
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	traj_list = createReclassifyList(conn, data)

	noncroplist=getCropList(conn, '0')
	croplist=getCropList(conn, '1')


	fishnet = 'fishnet_cdl_49_7'

	cls = data['ancillary']['tiles'][fishnet]['cls']
	rws = data['ancillary']['tiles'][fishnet]['rws']

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\{}'.format(fishnet), ["oid","SHAPE@"]):
		atlas_stco = row[0]
		print atlas_stco
		# if  atlas_stco == 128:  #####qaqc line 
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
	pool.map(execute_task, [(ed, data, traj_list, noncroplist, croplist, cls, rws) for ed in extDict.items()])
	pool.close()
	pool.join

	#close connection for this client
	conn.close ()

	mosiacRasters(data)
	reclassRaster(data)

	# gen.addGDBTable2postgres_recent('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\qaqc.gdb\\mask_fn_yfc_61', 'mask_fn_yfc_61', 'usxp', 'qaqc_refine')


if __name__ == '__main__':
	run(data)
   

    
   

