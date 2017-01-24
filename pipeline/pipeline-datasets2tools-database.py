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
import dbConnection, euclid, associationData, dataSubmissionAPI

#############################################
########## 2. General setup
#############################################
##### 1. Default variables #####
dbConnectionFile = 'mysql/dbconnection.json'
dbSchemaFile = 'mysql/dbschema.sql'
associationsFile = 'f1-associations.dir/dataset_tool_associations.xlsx'

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

@follows(mkdir('f2-upload_reports.dir'))

@files([dbConnectionFile,
	    dbSchemaFile],
	   'f2-upload_reports.dir/01-db_creation.txt')

def createDatasets2toolsDatabase(infiles, outfile):

	# Split infiles
	dbConnectionFile, sqlFile = infiles

	# Get connection
	dbEngine = dbConnection.create('phpmyadmin', dbConnectionFile)

	# Create and use new database
	dbConnection.executeCommand('DROP DATABASE IF EXISTS datasets2tools' % globals(), dbEngine)
	dbConnection.executeCommand('CREATE DATABASE datasets2tools' % globals(), dbEngine)

	# Update connection
	dbEngine = dbConnection.create('phpmyadmin', dbConnectionFile, 'datasets2tools')

	# Read SQL file
	with open(sqlFile, 'r') as openfile:
		sqlCommandString = openfile.read()

	# Get commands
	sqlCommandList = [x for x in sqlCommandString.split(';') if x != '\n']

	# Loop through commands
	for sqlCommand in sqlCommandList:

		# Execute command
		dbConnection.executeCommand(sqlCommand, dbEngine)

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

@follows(mkdir('f3-euclid.dir/dump.dir'), createDatasets2toolsDatabase)

@files(dbConnectionFile,
	   'f3-euclid.dir/dump.dir/euclid-canned_analysis_table.txt')

def getEuclidCannedAnalyses(infile, outfile):

	# Get engine
	dbEngine = dbConnection.create('phpmyadmin', infile, 'euclid')

	# Get euclid data
	euclidData = euclid.getCannedAnalysisDataframe(dbEngine)

	# Save
	euclidData.to_csv(outfile, sep='\t', index=True, index_label='index')

#############################################
########## 2. Get canned analysis metadata
#############################################

@follows(getEuclidCannedAnalyses)

@files(dbConnectionFile,
	   'f3-euclid.dir/dump.dir/euclid-canned_analysis_metadata_table.txt')

def getEuclidMetadata(infile, outfile):

	# Get engine
	dbEngine = dbConnection.create('phpmyadmin', infile, 'euclid')

	# Get euclid data
	euclidMetadata = euclid.getMetadataDataframe(dbEngine)

	# Save
	euclidMetadata.to_csv(outfile, sep='\t', index=False)

#############################################
########## 3. Process metadata dataframe
#############################################

@follows(mkdir('f3-euclid.dir/processed.dir'))

@transform(getEuclidMetadata,
		   suffix('.txt'),
		   add_inputs(getEuclidCannedAnalyses),
		   '_processed.txt')

def processEuclidMetadata(infiles, outfile):

	# Split infiles
	metadataFile, cannedAnalysisFile = infiles

	# Read canned analysis metadata
	cannedAnalysisMetadataDataframe = pd.read_table(metadataFile)

	# Read canned analysis table
	cannedAnalysisDataframe = pd.read_table(cannedAnalysisFile)

	# Merge dataframes
	mergedDataframe = cannedAnalysisDataframe.merge(cannedAnalysisMetadataDataframe, on='gene_list_id', how='left')

	# Create metadata dictionary
	metadataDict = {x:{'tool_name': y} for x, y in mergedDataframe[['index','tool_name']].drop_duplicates().as_matrix()}

	# Add metadata
	for index, variable, value in mergedDataframe[['index', 'variable', 'value']].as_matrix():

		# Add values
		metadataDict[index][variable] = value

	# Get processed metadata dataframe
	processedMetadataDataframe = mergedDataframe.loc[:, ['index','variable','value']]

	# Create description list
	descriptionList = [[x, 'description', euclid.getCannedAnalysisDescription(metadataDict[x])] for x in metadataDict.keys()]

	# Convert to dataframe
	descriptionDataframe = pd.DataFrame(descriptionList, columns=['index','variable','value'])

	# Add to metadata dataframe
	processedMetadataDataframe = pd.concat([processedMetadataDataframe, descriptionDataframe]).sort_values(by='index')

	# Save result
	processedMetadataDataframe.to_csv(outfile, sep='\t', index=False)


#######################################################
#######################################################
########## S3. Load Data
#######################################################
#######################################################

#############################################
########## 1. Load Tools
#############################################

@follows(getEuclidMetadata)

@files([dbConnectionFile,
		associationsFile],
		'f2-upload_reports.dir/02-tools.txt')

def loadAssociationTools(infiles, outfile):

	# Split infiles
	dbConnectionFile, associationsFile = infiles

	# Get engine
	dbEngine = dbConnection.create('phpmyadmin', dbConnectionFile, 'datasets2tools')

	# Get tool list dataframe
	toolListDataframe = associationData.getToolListDataframe(associationsFile)

	# Get tool category dataframe
	toolCategoryDataframe = associationData.getToolCategoryDataframe(associationsFile)

	# Merge dataframes
	mergedToolDataframeSubset = associationData.mergeToolDataframes(toolListDataframe, toolCategoryDataframe)

	# Upload data
	dbConnection.uploadTable(mergedToolDataframeSubset, dbEngine, 'tool', index=False)

	# Write report
	writeReport(outfile)

#############################################
########## 2. Upload Canned Analyses
#############################################

@follows(loadAssociationTools)

@merge([dbConnectionFile,
		getEuclidCannedAnalyses,
		processEuclidMetadata],
	   'f3-euclid.dir/processed.dir/euclid-id_conversion_table.txt')

def loadEuclidCannedAnalyses(infiles, outfile):

	# Split infiles
	dbConnectionFile, cannedAnalysisFile, cannedAnalysisMetadataFile = infiles

	# Get engine
	mysql = dbConnection.create('phpmyadmin', dbConnectionFile, 'datasets2tools')

	# Load canned analysis dataframe
	cannedAnalysisDataframe = pd.read_table(cannedAnalysisFile)

	# Load canned analysis metadata dataframe
	cannedAnalysisMetadataDataframe = pd.read_table(cannedAnalysisMetadataFile)

	# Process dataframes
	cannedAnalysisDataframe, cannedAnalysisMetadataDataframe = dataSubmissionAPI.processDataframes(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe, 'index')

	# Get matching dataset and tool IDs
	idDict = dataSubmissionAPI.matchIds(cannedAnalysisDataframe, mysql)

	# Upload canned analyses
	print 'Loading Canned Analyses...'
	cannedAnalysisIdDict = dataSubmissionAPI.uploadCannedAnalyses(cannedAnalysisDataframe, idDict, mysql, 'index')

	# Get paired list of IDs
	idPairList = [[x, y] for x in cannedAnalysisIdDict.keys() for y in cannedAnalysisIdDict[x]]

	# Convert to dataframe
	idPairDataframe = pd.DataFrame(idPairList, columns=['index', 'canned_analysis_fk'])

	# Save file
	idPairDataframe.to_csv(outfile, sep='\t', index=False)


#############################################
########## 3. Upload Analysis Metadata
#############################################

@files([dbConnectionFile,
		loadEuclidCannedAnalyses,
		processEuclidMetadata],
	   'f2-upload_reports.dir/03-euclid.txt')

def loadEuclidCannedAnalysisMetadata(infiles, outfile):

	# Split infiles
	dbConnectionFile, idPairFile, metadataFile = infiles

	# Get engine
	mysql = dbConnection.create('phpmyadmin', dbConnectionFile, 'datasets2tools')

	# Read ID Pair dataframe
	idPairDataframe = pd.read_table(idPairFile)

	# Read metadata file
	cannedAnalysisMetadataDataframe = pd.read_table(metadataFile)

	# Merge Dataframes
	mergedMetadataDataframe = idPairDataframe.merge(cannedAnalysisMetadataDataframe, on='index').drop_duplicates(subset=['canned_analysis_fk', 'variable'])

	# Load table
	dbConnection.insertDataframe(mergedMetadataDataframe[['canned_analysis_fk', 'variable', 'value']], 'canned_analysis_metadata', mysql)

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
