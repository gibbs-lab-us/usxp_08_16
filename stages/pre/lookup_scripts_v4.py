'''
Author: Matt Bougie (mbougie@wisc.edu)
Date: June 21, 2020
Purpose: Script to label each trajectory a values 1 - 5 using moving window approach. 
Usage: Need to set up connections to geodatabases to reference datasets and store outputs.
       Also have to have a connection set up to postgres database to store attributes of rasters as tables
Parameters: Parameters for all scripts are stored in the json file and referenced as a global data object.
'''

# Import system modules
import sys 
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
import json
import fnmatch

sys.path.append('C:\\Users\\Bougie\\Desktop\\scripts\\modules\\')
import general as gen


arcpy.CheckOutExtension("Spatial")


#######  update the lookup table  ######
def run(data):
    try:
        conn = psycopg2.connect('[[database_argument_string]]')
    except:
        print "I am unable to connect to the database"

    ## NOTE: the order that these queries is important --- mtr5 query needs to last.
    
    ### get parameters from json file
    cur = conn.cursor()
    res = data['global']['res']
    years=data['global']['years']
    datarange = data['global']['datarange']
    table = data['pre']['traj']['filename']
    lookuptable = data['pre']['traj']['lookup_name']

    #clear columns of values each time run script
    query_initial = 'update pre.{} set mtr = NULL, ytc = NULL, yfc = NULL, potential_yfc=0'.format(lookuptable)
    print query_initial
    cur.execute(query_initial)
    conn.commit()
    
    #run query_mtr1
    query_mtr1 = 'update pre.{} set mtr = 1 where traj_array in (SELECT (SELECT traj_array FROM UNNEST(traj_array) as s HAVING SUM(s) <= 1) from pre.{})'.format(lookuptable,lookuptable)
    print query_mtr1
    cur.execute(query_mtr1)
    conn.commit()

    #run query_mtr2
    query_mtr2 = 'update pre.{} set mtr = 2 where traj_array in (SELECT (SELECT traj_array FROM UNNEST(traj_array) as s HAVING SUM(s) >= {}) from pre.{})'.format(lookuptable, str(len(years)-1), lookuptable)
    print query_mtr2
    cur.execute(query_mtr2)
    conn.commit()
    
    # run the mtr3 and mtr4 queries for all conversion years EXCEPT 2009
    for year in data['global']['years_conv']:
        pre_context = 'cdl'+res+'_b_'+str(year - 2)
        before_year ='cdl'+res+'_b_'+str(year - 1)
        year_cdl = 'cdl'+res+'_b_'+str(year)
        post_context = 'cdl'+res+'_b_'+str(year + 1)
        
        if year != 2009:
            query_mtr3 = 'update pre.{} set mtr = 3, ytc = {} where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where {} = 0 AND {}= 0 AND {} = 1 AND {} = 1)'.format(lookuptable, str(year), table, lookuptable, pre_context, before_year, year_cdl, post_context)
            print query_mtr3
            cur.execute(query_mtr3)
            conn.commit()

            query_mtr4 = 'update pre.{} set mtr = 4, yfc = {} where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where {} = 1 AND {}= 1 AND {} = 0 AND {} = 0)'.format(lookuptable, str(year), table, lookuptable, pre_context, before_year, year_cdl, post_context)
            print query_mtr4
            cur.execute(query_mtr4)
            conn.commit()

    

    
    ###run 2009 queries AFTER all the other queries (except mtr5) is complete-----------------------------------------------------------------
    query_mtr3 = 'update pre.{} set mtr = 3, ytc = 2009 where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where cdl30_b_2008= 0 AND cdl30_b_2009 = 1 AND cdl30_b_2010 = 1 AND ytc IS NULL AND yfc IS NULL)'.format(lookuptable, table, lookuptable)
    print query_mtr3
    cur.execute(query_mtr3)
    conn.commit()

    query_mtr4 = 'update pre.{} set mtr = 4, yfc = 2009 where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where cdl30_b_2008= 1 AND cdl30_b_2009 = 0 AND cdl30_b_2010 = 0 AND ytc IS NULL AND yfc IS NULL)'.format(lookuptable, table, lookuptable)
    print query_mtr4
    cur.execute(query_mtr4)
    conn.commit()

    ###--------------------------------------------------------------------------------------------------------------------------------------
    year_index = {2008:0, 2009:1, 2010:2, 2011:3, 2012:4, 2013:5, 2014:6, 2015:7, 2016:8, 2017:9}
    query_refine_p1 = 'SELECT "Value",traj_array,ytc,yfc FROM pre.v4_traj_cdl30_b_2008to2017 a INNER JOIN pre.v4_traj_lookup_2008to2017_v4 b using(traj_array) Where ytc < yfc'
    print query_refine_p1
    cur.execute(query_refine_p1)
    conn.commit()

    rows = cur.fetchall()
    for row in rows:

        before_ytc = row[1][:year_index[row[2]]]

        after_yfc = row[1][year_index[row[3]]:]

        if (sum(before_ytc)==0) and (sum(after_yfc)==0):
            print "fits the condition!!!"
            print 'traj', row[1]
            print 'ytc', row[2]
            print 'yfc', row[3]
            query_refine_p2 = 'update pre.{0} set mtr = 4, ytc = NULL, yfc = {1}, potential_yfc=1 FROM pre.{2} WHERE {0}.traj_array={2}.traj_array AND "Value" = {3}'.format(lookuptable, row[3], table, row[0])
            print query_refine_p2
            cur.execute(query_refine_p2)
            conn.commit()



    ## run query_mtr5 to label all null mtr values AND remove records that dont have mutually exclusive ytc and yfc
    query_mtr5 = 'update pre.{} set mtr = 5, ytc = NULL, yfc = NULL where mtr IS NULL OR (ytc IS NOT NULL AND yfc IS NOT NULL)'.format(lookuptable)
    print query_mtr5
    cur.execute(query_mtr5)
    conn.commit()

    #close connection for this client
    conn.close ()



if __name__ == '__main__':
    run(data)



