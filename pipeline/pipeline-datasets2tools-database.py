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
import dbConnection, euclid, associationData, dataSubmission

#############################################
########## 2. General setup
#############################################
##### 1. Default variables #####
dbConnectionFile = 'mysql/dbconnection.json'
dbSchemaFile = 'mysql/dbschema.sql'
associationsFile = 'f1-data.dir/lincs/dataset_tool_associations.xlsx'
dbname = 'datasets2tools1'

##### 2. Functions #####
### 2.1 Write report
def writeReport(outfile):
	with open(outfile, 'w') as openfile:
		timeString = time.strftime("%Y-%m-%d, %H:%M")
		openfile.write('Completed %(timeString)s.' % locals())

#######################################################
#######################################################
########## S1. Database Setup
#######################################################
#######################################################

#############################################
########## 1. Create database
#############################################

@follows(mkdir('f2-setup_reports.dir'))

@files(dbSchemaFile,
	   'f2-setup_reports.dir/01-db_creation.txt')

def createDatasets2toolsDatabase(infile, outfile):

	# Get connection
	dbEngine = dbConnection.create('phpmyadmin', dbConnectionFile)

	# Create and use new database
	# dbConnection.executeCommand('DROP DATABASE IF EXISTS datasets2tools' % globals(), dbEngine)
	dbConnection.executeCommand('CREATE DATABASE %(dbname)s' % globals(), dbEngine)

	# Read SQL file
	with open(infile, 'r') as openfile:
		sqlCommandString = openfile.read()

	# Get commands
	sqlCommandList = [x for x in sqlCommandString.split(';') if x != '\n']

	# Update connection
	dbEngine = dbConnection.create('phpmyadmin', dbConnectionFile, dbname)

	# Loop through commands
	for sqlCommand in sqlCommandList:

		# Execute command
		dbConnection.executeCommand(sqlCommand, dbEngine)

	# Write report
	writeReport(outfile)

#############################################
########## 2. Load tools
#############################################

@follows(createDatasets2toolsDatabase)

@files(associationsFile,
	   'f2-setup_reports.dir/02-tools.txt')

def loadAssociationTools(infile, outfile):

	# Get engine
	dbEngine = dbConnection.create('phpmyadmin', dbConnectionFile, dbname)

	# Get tool list dataframe
	toolListDataframe = associationData.getToolListDataframe(infile)

	# Get tool category dataframe
	toolCategoryDataframe = associationData.getToolCategoryDataframe(infile)

	# Merge dataframes
	mergedToolDataframeSubset = associationData.mergeToolDataframes(toolListDataframe, toolCategoryDataframe)

	# Upload data
	dbConnection.uploadTable(mergedToolDataframeSubset, dbEngine, 'tool', index=False)

	# Write report
	writeReport(outfile)

#######################################################
#######################################################
########## S2. Get Euclid Data
#######################################################
#######################################################

#############################################
########## 1. Get canned analyses
#############################################

@follows(mkdir('f1-data.dir/euclid'))

@files(None,
	   'f1-data.dir/euclid/euclid-canned_analysis_table.txt')

def getEuclidCannedAnalyses(infile, outfile):

	# Get engine
	dbEngine = dbConnection.create('phpmyadmin', dbConnectionFile, 'euclid')

	# Get euclid data
	euclidData = euclid.getCannedAnalysisDataframe(dbEngine)

	# Save
	euclidData.to_csv(outfile, sep='\t', index=True, index_label='index')

#############################################
########## 2. Get canned analysis metadata
#############################################

@files(getEuclidCannedAnalyses,
	   'f1-data.dir/euclid/euclid-canned_analysis_metadata_table.txt')

def getEuclidMetadata(infile, outfile):

	# Read canned analysis table
	cannedAnalysisDataframe = pd.read_table(infile)

	# Get engine
	dbEngine = dbConnection.create('phpmyadmin', dbConnectionFile, 'euclid')

	# Get euclid data
	cannedAnalysisMetadataDataframe = euclid.getMetadataDataframe(dbEngine)

	# Process metadata dataframe
	processedMetadataDataframe = euclid.processMetadataDataframe(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe)

	# Save result
	processedMetadataDataframe.to_csv(outfile, sep='\t', index=False)

#######################################################
#######################################################
########## S3. Load Data
#######################################################
#######################################################

#############################################
########## 1. Load data
#############################################

@follows(mkdir('f3-load_reports.dir'))

@collate(glob.glob('f1-data.dir/*/*-canned_analysis*table.txt'),
	     regex(r'.*/(.*)-canned_analysis.*table.txt'),
	     r'f3-load_reports.dir/\1.txt')

def loadCannedAnalysisData(infiles, outfile):

	# Split infiles
	cannedAnalysisMetadataFile, cannedAnalysisFile = infiles

	# Read dataframes
	cannedAnalysisDataframe = pd.read_table(cannedAnalysisFile)
	cannedAnalysisMetadataDataframe = pd.read_table(cannedAnalysisMetadataFile)

	# Create database engine
	dbEngine = dbConnection.create('phpmyadmin', dbConnectionFile, dbname)

	# Load data
	dataSubmission.loadCannedAnalysisData(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe, dbEngine)

	# Write report
	writeReport(outfile)

##################################################
##################################################
########## Run pipeline
##################################################
##################################################
####################################################### i think you're crazy, maybe.
pipeline_run([sys.argv[-1]], multiprocess=1, verbose=1)
print('Done!')
