#################################################################
#################################################################
############### DBConnection Functions ##########################
#################################################################
#################################################################

#############################################
########## 1. Load libraries
#############################################
##### 1. Python modules #####
import sqlalchemy, json, os, sys
import pandas as pd

##### 2. Custom modules #####
sys.path.append('pipeline/scripts')
import DBConnection

#######################################################
#######################################################
########## S1. Tool dataframes
#######################################################
#######################################################

#############################################
########## 1.1 Tool List Dataframe
#############################################

def getToolListDataframe(associationFile):
		
	# Read excel
	toolListDataframe = pd.read_excel(associationFile, sheetname=2)

	# Define rename dictionary
	renameDict = {'name': 'tool_name', 'description': 'tool_description', 'url': 'tool_homepage_url', 'icon_url': 'tool_icon_url', 'tutorial_url': 'tool_tutorial_url'}

	# Rename dataframe
	toolListDataframe.rename(columns=renameDict, inplace=True)

	# Return dataframe
	return toolListDataframe

#############################################
########## 1.2 Tool Category Dataframe
#############################################

def getToolCategoryDataframe(associationFile):
		
	# Read excel
	toolCategoryDataframe = pd.read_excel(associationFile, sheetname=1)

	# Define rename dictionary
	renameDict = {'Tool': 'tool_name'}

	# Rename dataframe
	toolCategoryDataframe.rename(columns=renameDict, inplace=True)

	# Return dataframe
	return toolCategoryDataframe

#############################################
########## 1.3 Merge Tool Dataframes
#############################################

def mergeToolDataframes(toolListDataframe, toolCategoryDataframe):
		
	# Read excel
	mergedToolDataframe = toolListDataframe.merge(toolCategoryDataframe, on='tool_name', how='inner')

	# Get subset
	mergedToolDataframeSubset = mergedToolDataframe[['tool_name', 'tool_icon_url', 'tool_homepage_url', 'tool_description', 'doi']]

	# Return dataframe
	return mergedToolDataframeSubset


#######################################################
#######################################################
########## S2. Dataset Dataframes
#######################################################
#######################################################

#############################################
########## 2.1 Dataset dataframe
#############################################

def getDatasetDataframe(associationTextFile):

	# Read association dataframe
	associationDataframe = pd.read_table(associationTextFile)

	# Get tool dataframe

#######################################################
#######################################################
########## S3. Annotation
#######################################################
#######################################################

#############################################
########## 3.1 Annotate dataframe
#############################################

def annotateCannedAnalysisDataframe(cannedAnalysisDataframe, dbEngine):

	# Get tool dataframe
	toolDataframe = DBConnection.executeQuery('SELECT id AS tool_fk, tool_name FROM tool', dbEngine)
	
	# Get dataset dataframe
	datasetDataframe = DBConnection.executeQuery('SELECT id AS dataset_fk, dataset_accession FROM dataset', dbEngine)

	# Merge
	annotatedDataframe = cannedAnalysisDataframe.merge(toolDataframe, on='tool_name', how='left').merge(datasetDataframe, on='dataset_accession', how='left')

	# Drop columns
	annotatedDataframe.drop(['tool_name', 'dataset_accession', 'canned_analysis_description'], 1, inplace=True)

	# Return
	return annotatedDataframe


#######################################################
#######################################################
########## S. 
#######################################################
#######################################################

#############################################
########## . 
#############################################


