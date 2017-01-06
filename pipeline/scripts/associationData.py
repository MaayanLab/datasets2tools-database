#################################################################
#################################################################
############### DBConnection Functions ##########################
#################################################################
#################################################################

#############################################
########## 1. Load libraries
#############################################
##### 1. Python modules #####
import sqlalchemy, json, os
import pandas as pd

##### 2. Custom modules #####

#######################################################
#######################################################
########## S1. Get dataframes
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
########## S. 
#######################################################
#######################################################

#############################################
########## . 
#############################################


