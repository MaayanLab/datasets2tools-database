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
import pandas as pd

##### 2. Custom modules #####
# Pipeline running
sys.path.append('pipeline/scripts')
import DBConnection, euclid, associationData

#############################################
########## 2. General setup
#############################################
##### 1. Default variables #####
dbname = 'datasets2tools'

##### 2. Functions #####
### 2.1 Write report
def writeReport(outfile):
	with open(outfile, 'w') as openfile:
		timeString = time.strftime("%Y-%m-%d, %H:%M")
		openfile.write('Completed %(timeString)s.' % locals())

#######################################################
#######################################################
########## S1. Create Database
#######################################################
#######################################################

@files(['mysql/dbconnection.json',
	    'mysql/dbschema.sql'],
	   'reports/01-dbschema.txt')

def createDatasets2toolsDatabase(infiles, outfile):

	# Split infiles
	connectionFile, sqlFile = infiles

	# Get connection
	dbEngine = DBConnection.create('local', connectionFile)

	# Create and use new database
	DBConnection.executeCommand('DROP DATABASE IF EXISTS %(dbname)s' % globals(), dbEngine)
	DBConnection.executeCommand('CREATE DATABASE %(dbname)s' % globals(), dbEngine)
	DBConnection.executeCommand('USE %(dbname)s' % globals(), dbEngine)

	# Update connection
	dbEngine = DBConnection.create('local', connectionFile, dbname)

	# Read SQL file
	with open(sqlFile, 'r') as openfile:
		sqlCommandString = openfile.read()

	# Get commands
	sqlCommandList = [x for x in sqlCommandString.split(';') if x != '\n']

	# Loop through commands
	for sqlCommand in sqlCommandList:

		# Execute command
		DBConnection.executeCommand(sqlCommand, dbEngine)

	# Write report
	writeReport(outfile)

#######################################################
#######################################################
########## S2. Euclid Data
#######################################################
#######################################################

#############################################
########## 1. Euclid Data
#############################################

@follows(createDatasets2toolsDatabase)

@files('mysql/dbconnection.json',
	   'reports/02-euclid.txt')

def migrateEuclidData(infile, outfile):

	# Create engines
	amazonEngine = DBConnection.create('local', infile, 'datasets2tools')#DBConnection.create('amazon', infile)
	localEngine = DBConnection.create('local', infile, 'euclid')

	# Get euclid data
	euclidDataDict = euclid.getData(localEngine)

	# Loop through tables
	for tableKey in euclidDataDict.keys():

		# Upload table
		DBConnection.uploadTable(euclidDataDict[tableKey], amazonEngine, tableKey, index=False, index_label='id')

	# Set foreign key checks
	# euclid.setForeignKeys(localEngine)

	# Write report
	writeReport(outfile)

#######################################################
#######################################################
########## S2. Association Data
#######################################################
#######################################################

#############################################
########## 2.1 Association Data
#############################################

@follows(migrateEuclidData)

@files(['mysql/dbconnection.json',
		'data/dataset_tool_associations.xlsx'],
		'reports/03-dataset_tool_associations.txt')

def loadAssociationTools(infiles, outfile):

	# Split infiles
	connectionFile, associationFile = infiles

	# Get tool list dataframe
	toolListDataframe = associationData.getToolListDataframe(associationFile)

	# Get tool category dataframe
	toolCategoryDataframe = associationData.getToolCategoryDataframe(associationFile)

	# Merge dataframes
	mergedToolDataframeSubset = associationData.mergeToolDataframes(toolListDataframe, toolCategoryDataframe)

	# Get engine
	engine = DBConnection.create('local', connectionFile, 'datasets2tools')

	# Upload data
	DBConnection.uploadTable(mergedToolDataframeSubset, engine, 'tool', index=False)

	# Write report
	writeReport(outfile)

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
