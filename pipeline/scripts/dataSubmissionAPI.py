############################################################
############################################################
############### Datasets2Tools Extension API Module ########
############################################################
############################################################

#######################################################
########## 1.1 Setup ##################################
#######################################################

##############################
##### 1.1.1 Python Libraries
##############################
import json, sys
import pandas as pd

##############################
##### 1.1.2 Custom Libraries
##############################
sys.path.append('.')
from dbConnection import *

############################################################
############################################################
############### 3. New Data Submission #####################
############################################################
############################################################

#######################################################
########## 3.1 New Dataset ############################
#######################################################

##############################
##### 3.1.1 Upload new dataset
##############################

def insertNewDatasets(idDict, mysql):

	# Loop through keys
	for datasetAccession in idDict['dataset'].keys():

		# Check if data
		if not idDict['dataset'][datasetAccession]:

			# Create command string
			insertCommandString = 'INSERT INTO dataset (dataset_accession) VALUES ("%(datasetAccession)s")' % locals()

			# Upload
			idDict['dataset'][datasetAccession] = insertData(insertCommandString, mysql)

	# Return updated dictionary
	return idDict

##############################
##### 
##############################

def insertCannedAnalysis(dataset_fk, tool_fk, canned_analysis_url, mysql):

	# Try inserting
	try:
	    
	    # Create command string
	    insertCommandString = 'INSERT INTO canned_analysis (dataset_fk, tool_fk, canned_analysis_url) VALUES (%(dataset_fk)s, %(tool_fk)s, "%(canned_analysis_url)s")' % locals()

	    # Add Canned Analysis ID
	    cannedAnalysisId = insertData(insertCommandString, mysql)
	    
	except:
	    
	    # Create query string
	    queryString = 'SELECT id FROM canned_analysis WHERE canned_analysis_url = "%(canned_analysis_url)s"' % locals()

	    # Add Canned Analysis ID
	    cannedAnalysisId = executeQuery(queryString, mysql).ix[0, 'id']

	# Return
	return cannedAnalysisId

##############################
##### 
##############################

def uploadCannedAnalyses(cannedAnalysisDataframe, idDict, mysql, custom_id_column):

	# Get dataset FKs
	cannedAnalysisDataframe['dataset_fk'] = [idDict['dataset'][x] for x in cannedAnalysisDataframe['dataset_accession']]

	# Get tool FKs
	cannedAnalysisDataframe['tool_fk'] = [idDict['tool'][x] for x in cannedAnalysisDataframe['tool_name']]

	# Create empty list
	cannedAnalysisIdDict = {x:[] for x in cannedAnalysisDataframe[custom_id_column]}

	# Loop through canned analyses
	for custom_id, dataset_fk, tool_fk, canned_analysis_url in cannedAnalysisDataframe[[custom_id_column, 'dataset_fk', 'tool_fk', 'canned_analysis_url']].as_matrix():

		# Get ID
		cannedAnalysisId = insertCannedAnalysis(dataset_fk, tool_fk, canned_analysis_url, mysql)

		# Append
		cannedAnalysisIdDict[custom_id].append(cannedAnalysisId)

	# Return dictionary
	return cannedAnalysisIdDict

##############################
##### 
##############################

def uploadCannedAnalysisMetadata(cannedAnalysisMetadataDataframe, cannedAnalysisIdDict, mysql, custom_id_column):

	# Get paired list of IDs
	idPairList = [[x, y] for x in cannedAnalysisIdDict.keys() for y in cannedAnalysisIdDict[x]]

	# Convert to dataframe
	idPairDataframe = pd.DataFrame(idPairList, columns=['canned_analysis_fk', custom_id_column])

	# Merge dataframes
	mergedMetadataDataframe = idPairDataframe.merge(cannedAnalysisMetadataDataframe, on=custom_id_column).drop_duplicates(subset=['canned_analysis_fk', 'variable'])

	# Load table
	insertDataframe(mergedMetadataDataframe[['canned_analysis_fk', 'variable', 'value']], 'canned_analysis_metadata', mysql)

############################################################
############################################################
############### 2. Name-ID Matching ########################
############################################################
############################################################

#######################################################
########## 2.1 Name Lookup ############################
#######################################################

##############################
##### 2.1.1 Find IDs
##############################

def processDataframes(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe, custom_id_column):

	# Get common IDs
	commonIds = set(cannedAnalysisDataframe[custom_id_column]).intersection(set(cannedAnalysisMetadataDataframe[custom_id_column]))

	# Get canned analysis subset
	cannedAnalysisDataframe = cannedAnalysisDataframe.loc[cannedAnalysisDataframe[custom_id_column].isin(commonIds)]

	# Get canned analysis metadata subset
	cannedAnalysisMetadataDataframe = cannedAnalysisMetadataDataframe.loc[cannedAnalysisMetadataDataframe[custom_id_column].isin(commonIds)]

	# Return dataframes
	return cannedAnalysisDataframe, cannedAnalysisMetadataDataframe

############################################################
############################################################
############### 2. Name-ID Matching ########################
############################################################
############################################################

#######################################################
########## 2.1 Name Lookup ############################
#######################################################

##############################
##### 2.1.1 Find IDs
##############################

def findIds(nameList, tableName, mysql):

	# Set index label
	if tableName == 'dataset':
		nameLabel = 'dataset_accession'
	elif tableName == 'tool':
		nameLabel = 'tool_name'
	else:
		raise ValueError('Incorrect value for tableName, must be dataset or tool.')

	# Create comma-separated string
	nameString = '"' + '","'.join(nameList) + '"'

	# Create MySQL query string
	queryStatement = 'SELECT * FROM %(tableName)s WHERE %(nameLabel)s IN (%(nameString)s)' % locals()

	# Get Dataset ID dataframe
	idDataframe = executeQuery(queryStatement, mysql).set_index(nameLabel)

	# Convert to dict
	idDict = {x:idDataframe.loc[x, 'id'] if x in idDataframe.index else None for x in nameList}

	# Return dict
	return idDict


##############################
##### 3.1.1 Name-ID matching
##############################

def matchIds(cannedAnalysisDataframe, mysql):

	# Define dictionary
	idDict = {}

	# Get dataset IDs
	idDict['dataset'] = findIds(set(cannedAnalysisDataframe['dataset_accession']), 'dataset', mysql)

	# Get tool IDs
	idDict['tool'] = findIds(set(cannedAnalysisDataframe['tool_name']), 'tool', mysql)

	# Add new datasets
	idDict = insertNewDatasets(idDict, mysql)

	# Return dict
	return idDict

############################################################
############################################################
############### 4. Wrapper Functions #######################
############################################################
############################################################

#######################################################
########## 4.1 Wrappers ###############################
#######################################################

##############################
##### 
##############################

def submissionMain(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe, mysql, custom_id_column='gene_list_id'):

	# Process dataframes
	cannedAnalysisDataframe, cannedAnalysisMetadataDataframe = processDataframes(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe, custom_id_column)

	# Get matching IDs
	idDict = matchIds(cannedAnalysisDataframe, mysql)

	# Upload canned analyses
	cannedAnalysisIdDict = uploadCannedAnalyses(cannedAnalysisDataframe, idDict, mysql, custom_id_column)

	# Upload metadata
	uploadCannedAnalysisMetadata(cannedAnalysisMetadataDataframe, cannedAnalysisIdDict, mysql, custom_id_column)

