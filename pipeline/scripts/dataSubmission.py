#################################################################
#################################################################
############### dataSubmission Functions ########################
#################################################################
#################################################################

#############################################
########## 1. Load libraries
#############################################
##### 1. Python modules #####
import sys, sqlalchemy
import pandas as pd

##### 2. Custom modules #####
sys.path.append('.')
import dbConnection

#######################################################
#######################################################
########## S1. Dataframe preprocessing
#######################################################
#######################################################

#############################################
########## 1. Preprocess dataframe
#############################################

def processDataframes(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe, indexColumn):

	# Rename index column
	cannedAnalysisDataframe.rename(columns={indexColumn: 'index'}, inplace=True)
	cannedAnalysisMetadataDataframe.rename(columns={indexColumn: 'index'}, inplace=True)

	# Clean dataframes
	cannedAnalysisDataframe = cannedAnalysisDataframe.dropna()
	cannedAnalysisMetadataDataframe = cannedAnalysisMetadataDataframe.dropna()

	# Check for duplicates
	if any(cannedAnalysisDataframe.duplicated()):
		raise ValueError('Canned Analysis dataframe contains duplicated entries.  Please remove duplicates and resubmit.')
	elif any(cannedAnalysisMetadataDataframe.duplicated()):
		raise ValueError('Canned Analysis Metadata dataframe contains duplicated entries.  Please remove duplicates and resubmit.')

	# Get common index IDs
	commonIndexes = set(cannedAnalysisDataframe['index']).intersection(set(cannedAnalysisMetadataDataframe['index']))

	# Get subsets
	cannedAnalysisDataframe = cannedAnalysisDataframe.loc[cannedAnalysisDataframe['index'].isin(commonIndexes), :]
	cannedAnalysisMetadataDataframe = cannedAnalysisMetadataDataframe.loc[cannedAnalysisMetadataDataframe['index'].isin(commonIndexes), :]

	# Return dataframes
	return cannedAnalysisDataframe, cannedAnalysisMetadataDataframe

#############################################
########## 2. Add foreign keys
#############################################

def addForeignKeys(cannedAnalysisDataframe, dbEngine):

	# Loop through columns to replace
	for columnLabel in ['dataset_accession', 'tool_name']:

		# Get unique labels
		uniqueLabels = list(set(cannedAnalysisDataframe.loc[:, columnLabel]))

		# Get table name
		tableName = columnLabel.split('_')[0]

		# Create MySQL statement
		queryString = 'SELECT * FROM ' + tableName + ' WHERE ' + columnLabel + ' IN ("' + '","'.join(uniqueLabels) + '")'

		# Get foreign key dataframe
		foreignKeyDataframe = dbConnection.executeQuery(queryString, dbEngine).set_index(columnLabel)

		# Convert to dictionary
		foreignKeyDict = {x:foreignKeyDataframe.loc[x, 'id'] if x in foreignKeyDataframe.index else dbConnection.insertData('INSERT INTO ' + tableName + '(' + columnLabel + ') VALUES ("' + x + '")', dbEngine) for x in uniqueLabels}

		# Get foreign key label
		foreignKeyLabel = tableName + '_fk'

		# Add foreign key column
		cannedAnalysisDataframe[foreignKeyLabel] = [foreignKeyDict[x] for x in cannedAnalysisDataframe[columnLabel]]

		# Drop column
		cannedAnalysisDataframe.drop(columnLabel, inplace=True, axis=1)

	# Return dataframe
	return cannedAnalysisDataframe


#######################################################
#######################################################
########## S2. Dataframe upload
#######################################################
#######################################################

#############################################
########## 1. Upload canned analyses
#############################################

def uploadCannedAnalyses(cannedAnalysisDataframe, dbEngine):

	# Create empty dict
	cannedAnalysisForeignKeyDict = {}

	# Loop through canned analyses
	for index, dataset_fk, tool_fk, canned_analysis_url in cannedAnalysisDataframe[['index', 'dataset_fk', 'tool_fk', 'canned_analysis_url']].as_matrix():

		# Get database canned analysis primary key ID
		try:
		    cannedAnalysisId = dbConnection.insertData('INSERT INTO canned_analysis (dataset_fk, tool_fk, canned_analysis_url) VALUES (%(dataset_fk)s, %(tool_fk)s, "%(canned_analysis_url)s")' % locals(), dbEngine)
		except:
		    cannedAnalysisId = dbConnection.executeQuery('SELECT id FROM canned_analysis WHERE canned_analysis_url = "%(canned_analysis_url)s"' % locals(), dbEngine).ix[0, 'id']

		# Add to dict
		cannedAnalysisForeignKeyDict[index] = cannedAnalysisId

	# Convert to dataframe
	cannedAnalysisForeignKeyDataframe = pd.DataFrame.from_dict(cannedAnalysisForeignKeyDict, orient='index').rename(columns={0: 'canned_analysis_fk'})

	# Add index
	cannedAnalysisForeignKeyDataframe['index'] = cannedAnalysisForeignKeyDataframe.index

	# Return dataframe
	return cannedAnalysisForeignKeyDataframe

#############################################
########## 2. Upload canned analysis metadata
#############################################

def uploadCannedAnalysisMetadata(cannedAnalysisMetadataDataframe, cannedAnalysisForeignKeyDataframe, dbEngine):

	# Merge dataframes
	mergedMetadataDataframe = cannedAnalysisForeignKeyDataframe.merge(cannedAnalysisMetadataDataframe, on='index', how='inner').loc[:, ['canned_analysis_fk', 'variable', 'value']]

	# Upload table
	try:
		dbConnection.insertDataframe(mergedMetadataDataframe, 'canned_analysis_metadata', dbEngine)
	except:
		print 'There has been an error inserting the canned analysis metadata dataframe.  Moving on to the next.'

#######################################################
#######################################################
########## S3. Wrapper
#######################################################
#######################################################

#############################################
########## 1. Upload data
#############################################

def loadCannedAnalysisData(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe, dbEngine, indexColumn='index'):

	# Process dataframes
	cannedAnalysisDataframe, cannedAnalysisMetadataDataframe = processDataframes(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe, indexColumn)

	# Add foreign keys
	cannedAnalysisDataframe = addForeignKeys(cannedAnalysisDataframe, dbEngine)

	# Upload canned analyses
	cannedAnalysisForeignKeyDataframe = uploadCannedAnalyses(cannedAnalysisDataframe, dbEngine)

	# Upload canned analysis metadata
	uploadCannedAnalysisMetadata(cannedAnalysisMetadataDataframe, cannedAnalysisForeignKeyDataframe, dbEngine)
