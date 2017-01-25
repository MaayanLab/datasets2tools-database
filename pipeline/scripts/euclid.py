#################################################################
#################################################################
############### dbConnection Functions ##########################
#################################################################
#################################################################

#############################################
########## 1. Load libraries
#############################################
##### 1. Python modules #####
import sys, sqlalchemy
import pandas as pd
import numpy as np

##### 2. Custom modules #####
sys.path.append('.')
import dbConnection

#######################################################
#######################################################
########## S1. Canned Analysis Extraction
#######################################################
#######################################################

#############################################
########## 1. Get Canned Analysis dataframe
#############################################

def getCannedAnalysisDataframe(dbEngine):

	# Build query
	queryString = ''' SELECT gl.id AS gene_list_id, d.accession AS dataset_accession, ta.name AS tool_name, tal.link AS canned_analysis_url
	                  FROM target_app_link tal
	                      LEFT JOIN target_app ta
	                      ON ta.id = tal.target_app_fk
	                          LEFT JOIN gene_list gl
	                          ON gl.id = tal.gene_list_fk
	                              LEFT JOIN gene_signature gs
	                              ON gs.id = gl.gene_signature_fk
	                                  LEFT JOIN soft_file sf
	                                  ON gs.id = sf.gene_signature_fk
	                                      LEFT JOIN dataset d
	                                      ON d.id=sf.dataset_fk
					      WHERE gl.id IS NOT NULL AND
				      		  d.accession IS NOT NULL AND
					      	  ta.name IS NOT NULL AND
					          tal.link IS NOT NULL AND
					          ta.name NOT IN ('clustergrammer', 'crowdsourcing') LIMIT 500 '''

	# Perform query
	resultDataframe = dbConnection.executeQuery(queryString, dbEngine).dropna()

	# Define renaming dictionary
	toolNamedict = {'enrichr': 'Enrichr', 'l1000cds2': 'L1000CDS2', 'paea': 'PAEA', 'clustergrammer': 'GEN3VA', 'crowdsourcing': 'CREEDS'}

	# Rename tools
	resultDataframe['tool_name'] = [toolNamedict[x] for x in resultDataframe['tool_name']]

	# Return dataframe
	return resultDataframe

#######################################################
#######################################################
########## S2. Canned Analysis Metadata
#######################################################
#######################################################

#############################################
########## 1. Required metadata
#############################################

def getRequiredMetadata(dbEngine):

	# Build query
	queryString = ''' SELECT gl.id AS gene_list_id, rm.diff_exp_method, rm.ttest_correction_method, rm.cutoff, rm.threshold, gl.direction
	                  FROM required_metadata rm
	                      LEFT JOIN gene_signature gs
	                      ON gs.id = rm.gene_signature_fk
	                          LEFT JOIN gene_list gl
	                          ON gs.id = gl.gene_signature_fk'''

	# Perform query
	requiredMetadataDataframe = pd.melt(dbConnection.executeQuery(queryString, dbEngine), id_vars='gene_list_id')

	# Return dataframe
	return requiredMetadataDataframe

#############################################
########## 2. Optional metadata
#############################################

def getOptionalMetadata(dbEngine):

	# Build query
	queryString = ''' SELECT gl.id AS gene_list_id, om.name AS variable, om.value
	                  FROM optional_metadata om
	                      LEFT JOIN gene_signature gs
	                          ON gs.id = om.gene_signature_fk
	                              LEFT JOIN gene_list gl
	                              ON gs.id = gl.gene_signature_fk
	                      WHERE name NOT IN ('userEmail', 'userKey', 'user_key', 'user_email', '')
		                      AND name IS NOT NULL
		                      AND value NOT IN ('None', 'none', 'NA', 'NaN', 'NULL')
		                      AND value IS NOT NULL'''

	# Perform query
	optionalMetadataDataframe = dbConnection.executeQuery(queryString, dbEngine)

	# Return dataframe
	return optionalMetadataDataframe

#############################################
########## 3. All metadata
#############################################

def getMetadataDataframe(dbEngine):

	# Get required metadata
	requiredMetadataDataframe = getRequiredMetadata(dbEngine)

	# Get optional metadata
	optionalMetadataDataframe = getOptionalMetadata(dbEngine)

	# Concatenate dataframes
	mergedMetadataDataframe = pd.concat([requiredMetadataDataframe, optionalMetadataDataframe])

	# Define values to drop
	valuesToDrop = [None, 'None', 'none', 'NULL', 'null', 'NA', 'na', 'N/A', 'NAN', 'NaN', 'nan', '', ' ']

	# Get indices to remove
	indicesToDrop = [index for index, value in enumerate(mergedMetadataDataframe['value']) if str(value) in valuesToDrop]

	# Remove unwanted values
	filteredMetadataDataframe = mergedMetadataDataframe.drop(mergedMetadataDataframe.index[indicesToDrop]).dropna()

	# Strip trailing whitespace
	filteredMetadataDataframe.loc[:, 'variable'] = [x.strip() for x in filteredMetadataDataframe.loc[:, 'variable']]

	# Drop duplicates
	filteredMetadataDataframe.drop_duplicates(subset=['gene_list_id', 'variable'], inplace=True)

	# Return dataframe
	return filteredMetadataDataframe

#############################################
########## 4. Add descriptions
#############################################

def processMetadataDataframe(cannedAnalysisDataframe, cannedAnalysisMetadataDataframe):

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
	descriptionList = [[x, 'description', getCannedAnalysisDescription(metadataDict[x])] for x in metadataDict.keys()]

	# Convert to dataframe
	descriptionDataframe = pd.DataFrame(descriptionList, columns=['index','variable','value'])

	# Add to metadata dataframe
	processedMetadataDataframe = pd.concat([processedMetadataDataframe, descriptionDataframe]).sort_values(by='index')

	# Return dataframe
	return processedMetadataDataframe	

#############################################
########## 5. Add descriptions
#############################################

def getCannedAnalysisDescription(cannedAnalysisDict):
    
    # Tool description dictionary
    toolDescriptionDict = {'Enrichr': 'Enrichment analysis of the top',
                           'L1000CDS2': 'Signature search of the top',
                           'PAEA': 'Principal angle enrichment analysis of the top',
                           'GEN3VA': 'Interactive visualization of the top',
                           'CREEDS': 'Crowdsourcing analysis of the top'}
    
    # Gene list dictionary
    geneListDict = {'-1.0': ' most underexpressed genes',
                    '0.0': ' combined underexpressed and overexpressed genes',
                    '1.0': ' most overexpressed genes'}
    
    # Get list of relevant variables
    relevantVariables = ['diff_exp_method', 'organism', 'disease', 'perturbation' ,'cell', 'gene', 'ttest_correction_method']
    
    # Get data
    toolName = cannedAnalysisDict['tool_name']
    geneListDirection = str(cannedAnalysisDict['direction'])
    
    # Get description
    if 'cutoff' in cannedAnalysisDict.keys():
        
        descriptionString = toolDescriptionDict[toolName] + ' ' + str(int(float(cannedAnalysisDict['cutoff']))) + geneListDict[geneListDirection]
    
    elif 'threshold' in cannedAnalysisDict.keys():

        descriptionString = toolDescriptionDict[toolName] + geneListDict[geneListDirection] + ', p < ' + str(cannedAnalysisDict['threshold'])

    else:
        
        raise ValueError('Differential expression method incorrectly specified.')
        
    # Add other info
    for metadataKey in cannedAnalysisDict.keys():
        
        # Check if relevant
        if metadataKey in relevantVariables:
        
            # Append
            descriptionString += ', ' + str(cannedAnalysisDict[metadataKey]) + ' ' + str(metadataKey)
        
    return descriptionString

