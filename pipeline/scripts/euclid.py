#################################################################
#################################################################
############### DBConnection Functions ##########################
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
import DBConnection

nDatasets = 100

#######################################################
#######################################################
########## S1. Data Extraction
#######################################################
#######################################################

#############################################
########## 1.1 Get Datasets
#############################################

def DBgetDatasetTable(engine):

	# Get Query String
	queryString = ''' SELECT id, accession AS dataset_accession
						FROM dataset
						WHERE record_type='geo' AND id < %(nDatasets)s ''' % globals()

	# Return Dataframe
	return DBConnection.executeQuery(queryString, engine).set_index('id')

#############################################
########## 1.2 Get Tools
#############################################

def getToolTable(engine):

	# Get Query String
	queryString = ''' SELECT id, `name` AS tool_name
						FROM target_app '''

	# Add Tool URL and Tool Icon URL Data
	toolData = {'enrichr': {
					'name': 'Enrichr',
					'tool_icon_url': 'http://amp.pharm.mssm.edu/Enrichr/images/enrichr-icon.png',
					'tool_homepage_url':'http://amp.pharm.mssm.edu/Enrichr',
					'publication_url': 'http://www.ncbi.nlm.nih.gov/pubmed/23586463',
					'tool_description': 'An intuitive web-based gene list enrichment analysis tool with 90 libraries.'},
				'l1000cds2': {
					'name': 'L1000CDS2',
					'tool_icon_url': 'http://amp.pharm.mssm.edu/L1000CDS2/CSS/images/sigine.png', 
					'tool_homepage_url':'http://amp.pharm.mssm.edu/L1000CDS2',
					'publication_url': 'http://www.nature.com/articles/npjsba201615',
					'tool_description': 'An ultra-fast LINCS L1000 Characteristic Direction signature search engine.'},
				'paea': {
					'name': 'PAEA',
					'tool_icon_url': 'http://lincsproject.org/LINCS/files/tools_logos/paea.png',
					'tool_homepage_url':'http://amp.pharm.mssm.edu/PAEA',
					'publication_url': 'http://www.ncbi.nlm.nih.gov/pubmed/26848405',
					'tool_description': 'Enrichment analysis tool implementing the principal angle method.'},
				'crowdsourcing': {
					'name': 'Crowdsourcing',
					'tool_icon_url': 'https://www.iconexperience.com/_img/o_collection_png/green_dark_grey/512x512/plain/users_crowd.png',
					'tool_homepage_url':'http://amp.pharm.mssm.edu/CREEDS/',
					'publication_url': '',
					'tool_description': 'CREEDS Crowdsourcing project.'},
				'clustergrammer': {
					'name': 'Clustergrammer',
					'tool_icon_url': 'http://amp.pharm.mssm.edu/clustergrammer/static/icons/graham_cracker_70.png',
					'tool_homepage_url':'http://amp.pharm.mssm.edu/clustergrammer/',
					'publication_url': '',
					'tool_description': 'Visualization tool that enables users to easily generate highly interactive and shareable clustergram/heatmap visualizations from a matrix.'}}

	# Return Dataframe
	tool_dataframe =  DBConnection.executeQuery(queryString, engine).set_index('id')

	# Add Tool Icon URL
	tool_dataframe['tool_icon_url'] = [toolData[x]['tool_icon_url'] for x in tool_dataframe['tool_name']]
	tool_dataframe['tool_homepage_url'] = [toolData[x]['tool_homepage_url'] for x in tool_dataframe['tool_name']]
	tool_dataframe['tool_description'] = [toolData[x]['tool_description'] for x in tool_dataframe['tool_name']]
	tool_dataframe['tool_name'] = [toolData[x]['name'] for x in tool_dataframe['tool_name']]

	# Return Dataframe
	return tool_dataframe

#############################################
########## 1.3 Get Canned Analyses
#############################################

def getCannedAnalysisTable(engine):

	# Get Query String
	queryString = ''' SELECT tal.id, dataset_fk, target_app_fk AS tool_fk, link AS canned_analysis_url
						FROM target_app_link tal
						LEFT JOIN gene_list gl
					    ON gl.id = tal.gene_list_fk
							LEFT JOIN gene_signature gs
					        ON gs.id = gl.gene_signature_fk
								LEFT JOIN soft_file sf
					            ON gs.id = sf.gene_signature_fk
									LEFT JOIN dataset d
									ON d.id = sf.dataset_fk
						WHERE d.record_type='geo' AND d.id < %(nDatasets)s ''' % globals()

	# Return Dataframe
	return DBConnection.executeQuery(queryString, engine).set_index('id')

#############################################
########## 1.4 Get Required Metadata
#############################################

def getRequiredMetadataTable(engine):

	# Get Query String
	queryString = ''' SELECT tal.id AS canned_analysis_fk, `direction`, `diff_exp_method`, `ttest_correction_method`, `cutoff`, `threshold` FROM target_app_link tal
						LEFT JOIN gene_list gl
					    ON gl.id = tal.gene_list_fk
							LEFT JOIN gene_signature gs
					        ON gs.id = gl.gene_signature_fk
						        LEFT JOIN soft_file sf
					            ON gs.id = sf.gene_signature_fk
									LEFT JOIN dataset d
									ON d.id = sf.dataset_fk
										LEFT JOIN required_metadata rm
							            ON gs.id = rm.gene_signature_fk
						WHERE d.record_type='geo' AND d.id < %(nDatasets)s ''' % globals()
	# Return Dataframe
	required_metadata_dataframe = DBConnection.executeQuery(queryString, engine)

	# Melt Dataframe
	required_metadata_dataframe_melted = pd.melt(required_metadata_dataframe, id_vars='canned_analysis_fk').dropna()

	# Rename Index to ID
	required_metadata_dataframe_melted.index.name = 'id'

	# Return Dataframe
	return required_metadata_dataframe_melted

#############################################
########## 1.5 Get Optional Metadata
#############################################

def getOptionalMetadataTable(engine):

	# Get Query String
	queryString = ''' SELECT tal.id as canned_analysis_fk, om.name AS variable, om.value FROM target_app_link tal
						LEFT JOIN gene_list gl
					    ON gl.id = tal.gene_list_fk
							LEFT JOIN gene_signature gs
					        ON gs.id = gl.gene_signature_fk
						        LEFT JOIN soft_file sf
					            ON gs.id = sf.gene_signature_fk
									LEFT JOIN dataset d
									ON d.id = sf.dataset_fk
										LEFT JOIN optional_metadata om
							            ON gs.id = om.gene_signature_fk
						WHERE om.name IS NOT NULL AND om.value IS NOT NULL AND d.record_type='geo' AND d.id < %(nDatasets)s ''' % globals()

	# Return Dataframe
	optional_metadata_dataframe = DBConnection.executeQuery(queryString, engine)

	# Rename Index to ID
	optional_metadata_dataframe.index.name = 'id'

	# Return Dataframe
	return optional_metadata_dataframe

#############################################
########## 1.6 Wrapper Function
#############################################

def getData(engine):

	# Define Dictionary
	euclidDataDict = {}

	# Get Dataset Table
	euclidDataDict['dataset'] = DBgetDatasetTable(engine)

	# Get Tool Table
	euclidDataDict['tool'] = getToolTable(engine)

	# Get Canned Analysis Table
	euclidDataDict['canned_analysis'] = getCannedAnalysisTable(engine)

	# Get Required Metadata Table
	euclidDataDict['canned_analysis_metadata'] = pd.concat([getRequiredMetadataTable(engine), getOptionalMetadataTable(engine)])

	# Return Result
	return euclidDataDict

#######################################################
#######################################################
########## S2. Data Upload
#######################################################
#######################################################

#############################################
########## 2.2 Set Foreign Keys
#############################################

def setForeignKeys(engine):
	
	# Get commands
	foreignKeyCommands = ['ALTER TABLE canned_analysis ADD FOREIGN KEY (`dataset_fk`) REFERENCES dataset(`id`)',
						  'ALTER TABLE canned_analysis ADD FOREIGN KEY (`tool_fk`) REFERENCES tool(`id`)',
						  'ALTER TABLE canned_analysis_metadata ADD FOREIGN KEY (`canned_analysis_fk`) REFERENCES canned_analysis(`id`)']

	# Run Commands
	for foreignKeyCommand in foreignKeyCommands:
		DBConnection.executeCommand(foreignKeyCommand, engine)

#######################################################
#######################################################
########## S. 
#######################################################
#######################################################

#############################################
########## . 
#############################################


