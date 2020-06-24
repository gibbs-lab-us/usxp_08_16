'''
Author: Matt Bougie (mbougie@wisc.edu)
Date: June 21, 2020
Purpose: Central script to call all other scripts for processing data. 
Usage: Need to set up connections to directories of scripts and to geodatabases where the output is saved.
Parameters: Parameters for all scripts are stored in the json file and referenced as a global data object.
'''

import sys
import os
# from config import from_config
from sqlalchemy import create_engine
import pandas as pd
import json
sys.path.append('C:\\Users\\Bougie\\Desktop\\scripts\\modules\\')
import general as gen



### import pre ###########################################################################
sys.path.append('C:\\Users\\Bougie\\Desktop\\scripts\\projects\\usxp\\stages\\pre\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\scripts\\projects\\usxp\\stages\\pre\\lookup_scripts')
import pre
import lookup_scripts_v4


###  import refinement scripts  ##########################################################
sys.path.append('C:\\Users\\Bougie\\Desktop\\scripts\\projects\\usxp\\stages\\refine')
import mask_fn_yfc_61
import mask_fp_2007
import mask_fp_nlcd_yfc
import mask_fp_nlcd_ytc
import mask_fp_yfc
import mask_fp_ytc


### import core scripts ##################################################################
sys.path.append('C:\\Users\\Bougie\\Desktop\\scripts\\projects\\usxp\\stages\\core\\')
import parallel_core as core


### import post scripts ##################################################################
sys.path.append('C:\\Users\\Bougie\\Desktop\\scripts\\projects\\usxp\\stages\\post\\yxc\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\scripts\\projects\\usxp\\stages\\post\\cdl\\')
import parallel_yxc as yxc
import parallel_cdl as cdl
import parallel_cdl_bfc_bfnc as cdl_bfc_bfnc

import add2pg_yxc
import add2pg_cdl



if __name__ == '__main__':
	### get json parameters from current instance
	data = gen.getCurrentInstance(file='C:\\Users\\Bougie\\Desktop\\scripts\\projects\\usxp\\configure\\json\\current_instance.json')
	print(data)


	#####################################################################################################################################################
	###### pre and refinement stages #####################################################################################################################
	#####################################################################################################################################################

	####************ create trajectory ***************###############################
	### NOTE: only run script to create new traj lookup#################
	lookup_scripts_v4.run(data)

	####************* create the refined trajectory *************#################################

	####____false negative refinement______________________________________
	###run script to create false negative mask
	mask_fn_yfc_61.run(data)

	#####create the add_yfc trajectories dataset############################
	#####apply false negative mask above to trajectory so false negative masks can be applied to it
	pre.run(data)

	###____false positve refinement________________________________________
	###run scripts to create each false positive mask
	mask_fp_2007.run(data)
	mask_fp_nlcd_yfc.run(data)
	mask_fp_nlcd_ytc.run(data)
	mask_fp_yfc.run(data)
	mask_fp_ytc.run(data)

	######################create the rfnd dataset###################################
	pre.run(data)


    #####################################################################################################################################################
	###### core stage ###################################################################################################################################
	#####################################################################################################################################################
	core.run(data)
	add2pg_mtr.run(data)


    #####################################################################################################################################################
	###### post stage ###################################################################################################################################
	#####################################################################################################################################################

	##_______YTC________________________________________________
	### create rasters 
	yxc.run(data, 'ytc')
	cdl.run(data, 'ytc', 'bfc')
	cdl_bfc_bfnc.run(data, 'ytc', 'fc')

    ### add raster attribute table to database
	add2pg_yxc.run(data, 'ytc')
	add2pg_cdl.run(data, 'ytc', 'bfc')
	add2pg_cdl.run(data, 'ytc', 'fc')


	##________YFC_______________________________________________
	### create rasters 
	yxc.run(data, 'yfc')
    cdl.run(data, 'yfc', 'fnc')
	cdl_bfc_bfnc.run(data, 'yfc', 'bfnc')

    ### add raster attribute table to database
	add2pg_yxc.run(data, 'yfc')
	add2pg_cdl.run(data, 'yfc', 'bfnc')
	add2pg_cdl.run(data, 'yfc', 'fnc')


			




