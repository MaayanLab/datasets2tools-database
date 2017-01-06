#################################################################
#################################################################
############### Datasets2Tools Database Pipeline ################
#################################################################
#################################################################
##### Author: Denis Torre
##### Affiliation: Ma'ayan Laboratory,
##### Icahn School of Medicine at Mount Sinai

#############################################
########## 1. Load libraries
#############################################
##### 1. Python modules #####
from ruffus import *
import glob, sys, os, time

##### 2. Custom modules #####
# Pipeline running
sys.path.append('pipeline/scripts')
sys.path.append('pipeline/scripts/euclid')
import DBConnection
import euclid

#############################################
########## 2. General setup
#############################################
##### 1. Default variables #####


#######################################################
#######################################################
########## S1. Euclid
#######################################################
#######################################################

#############################################
########## 1. Euclid Data
#############################################

@files('mysql/dbconnection.json',
	   'reports/01-euclid.txt')

def migrateEuclidData(infile, outfile):

	# Create engines
	amazonEngine = DBConnection.create('amazon', infile)
	localEngine = DBConnection.create('local', infile, 'euclid')

	# Get euclid data
	euclidDataDict = euclid.getData(localEngine)

	# Define database name
	dbname = 'datasets2tools'

	# Create and use new database
	DBConnection.executeCommand('DROP DATABASE IF EXISTS %(dbname)s' % locals(), localEngine)
	DBConnection.executeCommand('CREATE DATABASE %(dbname)s' % locals(), localEngine)
	DBConnection.executeCommand('USE %(dbname)s' % locals(), localEngine)

	# Upload tables
	euclid.uploadTables(euclidDataDict, localEngine)

	# Set foreign key checks
	euclid.setForeignKeys(localEngine)

	# Write report
	with open(outfile, 'w') as openfile:

		# Get date stamp
		timeString = time.strftime("%Y-%m-%d, %H:%M")

		# Write
		openfile.write('Completed %(timeString)s.' % locals())

#######################################################
#######################################################
########## S. 
#######################################################
#######################################################

#############################################
########## . 
#############################################



##################################################
##################################################
########## Run pipeline
##################################################
##################################################
pipeline_run([sys.argv[-1]], multiprocess=1, verbose=1)
print('Done!')
