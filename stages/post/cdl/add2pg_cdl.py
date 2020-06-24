'''
Author: Matt Bougie (mbougie@wisc.edu)
Date: June 21, 2020
Purpose: Script to add the attribute table of the FC/FNC datasets to PostgreSQL database. 
Usage: Need to set up connections to geodatabases to reference datasets and store outputs.
       Also have to have a connection set up to PostgreSQL database to store attributes of rasters as tables
Parameters: Parameters for all scripts are stored in the json file and referenced as a global data object.
'''



from sqlalchemy import create_engine
import numpy as np, sys, os
import fnmatch
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen
import shutil
import matplotlib.pyplot as plt



#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"


try:
    conn = psycopg2.connect('[[database_argument_string]]')
except:
    print "I am unable to connect to the database"



def addGDBTable2postgres(data, yxc, subtype):
    # set the engine.....
    engine = create_engine('[[database_argument_string]]')
    
    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(data['post'][yxc][subtype]['path'])]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(data['post'][yxc][subtype]['path'],fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    # df_cdl = pd.read_sql_query('select * from "Stat_Table"',con=engine)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df

    total=df['count'].sum()
    
    # # use pandas method to import table into psotgres
    print 'replace table: {}.{} if exists otherwise create '.format('counts_cdl', data['post'][yxc][subtype]['filename'])
    df.to_sql(data['post'][yxc][subtype]['filename'], engine, schema='counts_cdl', if_exists='replace')
    
    #add trajectory field to table
    addAcresField('counts_cdl', data['post'][yxc][subtype]['filename'], yxc, 30, total)




def addAcresField(schema, tablename, yxc, res, total):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    ####DDL: add column to hold arrays
    query = 'ALTER TABLE {}.{} ADD COLUMN acres bigint, ADD COLUMN perc numeric, ADD COLUMN series text'.format(schema, tablename)
    print query
    cur.execute(query)

    print int(tablename.split("_")[0][1:])

    #####DML: insert values into new array column
    cur.execute("UPDATE {0}.{1} SET acres=count*{2}, perc=(count/{3})*100, series='{4}'".format(schema, tablename, gen.getPixelConversion2Acres(res), total, tablename.split("_")[0]))
    conn.commit() 




def createMergedTable():
  cur = conn.cursor()
  query="SELECT table_name FROM information_schema.tables WHERE table_schema = 'counts_cdl' AND SUBSTR(table_name, 1, 1) = 's';"
  cur.execute(query)
  rows = cur.fetchall()
  print rows
  
  table_list = []
  for row in rows:
    query_temp="SELECT value,count,acres,series,yxc FROM counts_cdl.{}".format(row[0])
    table_list.append(query_temp)

  query_final = "DROP TABLE IF EXISTS counts_cdl.merged_series; CREATE TABLE counts_cdl.merged_series AS {}".format(' UNION '.join(table_list))
  print query_final
  cur.execute(query_final)
  conn.commit()



def run(data, yxc, subtype):
  addGDBTable2postgres(data, yxc, subtype)
  createMergedTable()


if __name__ == '__main__':
  run(data, yxc, subtype)





