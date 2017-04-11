# -*- coding: utf-8 -*-
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
import glob, sys, os, time, json
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

##### 2. Custom modules #####
# Pipeline running
sys.path.append('pipeline/scripts')
import db

#############################################
########## 2. General setup
#############################################
##### 1. Default variables #####
# DB Files
schemaFile = 'f1-mysql.dir/schema.sql'
connectionFile = 'f1-mysql.dir/conn.json'

# bioCADDIE Repository File
repositoryHtmlFile = 'f3-repositories.dir/Repository List _ bioCADDIE Data Discovery Index.htm'

# Canned Analyses
creedsAnalyses = glob.glob('../datasets2tools-canned-analyses/f1-creeds.dir/*/*-canned_analyses.txt')
# archsAnalyses = '../datasets2tools-canned-analyses/f1-creeds.dir/archs-canned_analyses.txt'
# clustergrammerAnalyses = glob.glob('../datasets2tools-canned-analyses/f3-geo.dir/*/*/*-canned_analyses.txt')


##### 2. Functions #####

#######################################################
#######################################################
########## S1. Create Database
#######################################################
#######################################################

#############################################
########## 1. Create Schema
#############################################

@merge(['f1-mysql.dir/schema.sql',
		'f1-mysql.dir/conn.json'],
		'f1-mysql.dir/schema.load')

def createDatabase(infiles, outfile):

	# Split infiles
	schemaFile, connectionFile = infiles

	# Get dict
	host, username, password = db.connect(connectionFile, 'localhost', returnData=True)

	# Get command
	commandString = ''' mysql --user='%(username)s' --password='%(password)s' < %(schemaFile)s; touch %(outfile)s; ''' % locals()

	# Run
	os.system(commandString)


#######################################################
#######################################################
########## S2. Load Tools and Repositories
#######################################################
#######################################################

#############################################
########## 1. Load Tools
#############################################

@follows(createDatabase)

@transform('f2-tools.dir/lincs_tools_mar152017.xlsx',
		   suffix('.xlsx'),
		   add_inputs(connectionFile),
		   '.load')

def loadTools(infiles, outfile):

	# Split files
	toolFile, connectionFile = infiles

	# Read table
	toolDataframe = pd.read_excel(toolFile)

	# Rename dict
	renameDict = {'name < 20 characters including spaces': 'tool_name',
	              'icon_url': 'tool_icon_url',
	              'url': 'tool_homepage_url',
	              'description < 80 charcaters including spaces': 'tool_description'}

	# Rename
	toolDataframe = toolDataframe.rename(columns=renameDict)

	# Select columns
	selectedColumns = ['id', 'tool_name', 'tool_icon_url', 'tool_homepage_url', 'tool_description']

	# Get engine
	engine = db.connect(connectionFile, 'localhost', 'datasets2tools')

	# Truncate
	engine.execute('SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE tool; SET FOREIGN_KEY_CHECKS = 1;')

	# Send to SQL
	toolDataframe[selectedColumns].to_sql('tool', engine, if_exists='append', index=False)

	# Outfile
	os.system('touch %(outfile)s' % locals())


#######################################################
#######################################################
########## S3. Load Repositories
#######################################################
#######################################################

#############################################
########## 1. Create Repository Table
#############################################

@files(repositoryHtmlFile,
	   'f3-repositories.dir/repositories.xlsx')

def makeRepositoryTable(infiles, outfile):

	# Parse table
	table = BeautifulSoup(open('f3-repositories.dir/Repository List _ bioCADDIE Data Discovery Index.htm'), "lxml").find('table')


	# Define dict
	resultDict = {}

	# Loop through rows
	for row in table.find_all('tr'):
	    
	    try:
	        # Add data
	        resultDict[row.find('a').text.encode('utf-8')] = {'repository_icon_url': row.find('img').attrs['src'].replace('./', 'https://datamed.org/').encode('utf-8'),
					                                          'repository_description': row.find_all('td')[-1].text.encode('utf-8'),
					                                          'repository_homepage_url': ''}
	    except:
	        pass

	# Convert to dataframe
	repositoryDataframe = pd.DataFrame(resultDict).T.reset_index().rename(columns={'index':'repository_name'})
	repositoryDataframe.index = [x+1 for x in repositoryDataframe.index]

	# Save
	repositoryDataframe.to_excel(outfile, index_label='id')

#############################################
########## 2. Load Repositories
#############################################

@follows(loadTools)

@transform('f3-repositories.dir/repositories.xlsx',
		   suffix('.xlsx'),
		   add_inputs(connectionFile),
		   '.load')

def loadRepositories(infiles, outfile):

	# Split files
	toolFile, connectionFile = infiles

	# Read table
	repositoryDataframe = pd.read_excel(toolFile, encoding='ascii')

	# Get engine
	engine = db.connect(connectionFile, 'localhost', 'datasets2tools')

	# Truncate
	engine.execute('SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE repository; SET FOREIGN_KEY_CHECKS = 1;')

	# Send to SQL
	repositoryDataframe.to_sql('repository', engine, if_exists='append', index=False)

	# Outfile
	os.system('touch %(outfile)s' % locals())


#######################################################
#######################################################
########## S4. Load Analyses
#######################################################
#######################################################

#############################################
########## 1. Load Analyses
#############################################

@follows(loadRepositories)

@follows(mkdir('f4-analyses.dir'))

def loadJobs():
	infiles = creedsAnalyses
	for analysisFile in infiles:
		outDir = os.path.join('f4-analyses.dir', os.path.basename(analysisFile)[:-len('-canned_analyses.txt')])
		if not os.path.exists(outDir):
			os.makedirs(outDir)
		outfiles = [os.path.join(outDir, '-'.join([os.path.basename(outDir), x])) for x in ['datasets.txt', 'canned_analyses.txt', 'canned_analysis_metadata.txt', 'terms.txt', 'all.load']]
		yield [[analysisFile, connectionFile], outfiles]

@files(loadJobs)

def loadAnalyses(infiles, outfiles):

	# Split files
	analysisFile, connectionFile = infiles
	datasetOutfile, analysisOutfile, metadataOutfile, termOutfile, loadOutfile = outfiles

	# Connect
	engine = db.connect(connectionFile, 'localhost', 'datasets2tools')
	connection = engine.connect()
	transaction = connection.begin()

	# Read dataframes
	inputAnalysisDataframe = pd.read_table(analysisFile, index_col='index').dropna().rename(columns={'geo_id': 'dataset_accession', 'link': 'canned_analysis_url'})
	toolDataframe = pd.read_sql_query('SELECT id AS tool_fk, LCASE(tool_name) AS tool_name FROM tool', engine)
	datasetDataframe = pd.read_sql_query('SELECT id AS dataset_fk, LCASE(dataset_accession) AS dataset_accession FROM dataset', engine)
	repositoryDataframe = pd.read_sql_query('SELECT id AS repository_fk, LCASE(repository_name) AS repository_name FROM repository', engine)
	termDataframe = pd.read_sql_query('SELECT id AS term_fk, LCASE(term_name) AS term_name FROM term', engine)
	repositoryDataframe['repository_name'] = [x.replace('\xc2\xa0', ' ') for x in repositoryDataframe['repository_name']]

	# Annotate analysis dataframe
	annotatedDataframe = inputAnalysisDataframe.merge(toolDataframe, left_on='tool', right_on='tool_name', how='left').merge(datasetDataframe, left_on='dataset_accession', right_on='dataset_accession', how='left')

	# Set error if tool not available
	if annotatedDataframe['tool_fk'].isnull().any():
	    raise ValueError('Tool(s) '+', '.join(annotatedDataframe.ix[annotatedDataframe['tool_fk'].isnull(), 'tool_fk']).unique+'not in database!')

	# Add new datasets
	# engine.execute('SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE dataset;')
	datasetsToUpload = annotatedDataframe.loc[[np.isnan(x) for x in annotatedDataframe['dataset_fk']], 'dataset_accession'].unique()
	if len(datasetsToUpload) > 0:
		newDatasetDataframe = pd.DataFrame({x: db.annotate(x) for x in datasetsToUpload}).T.reset_index().rename(columns={'title': 'dataset_title', 'summary': 'dataset_description', 'index': 'dataset_accession'})[['dataset_accession', 'repository_name', 'dataset_title', 'dataset_description', 'dataset_landing_url']]
		newDatasetDataframe = db.insertData(newDatasetDataframe.merge(repositoryDataframe, on='repository_name', how='left').drop('repository_name', axis=1), 'dataset', connection)
		datasetIdDict = {rowData['dataset_accession']:rowData['id'] for index, rowData in newDatasetDataframe.iterrows()}
		for dataset in datasetsToUpload:
		    annotatedDataframe.loc[annotatedDataframe['dataset_accession'] == dataset, 'dataset_fk'] = datasetIdDict[dataset]

	# Add new analyses
	# engine.execute('SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE canned_analysis;')
	analysisDataframe = db.insertData(annotatedDataframe[['dataset_fk', 'tool_fk', 'canned_analysis_url']], 'canned_analysis', connection)
	analysisIdDict = {index:rowData['id'] for index, rowData in analysisDataframe.iterrows()}

	# Create metadata dataframe
	annotatedDataframe['metadata'] = [json.loads(x) for x in annotatedDataframe['metadata']]
	metadataDataframe = pd.DataFrame([{'canned_analysis_fk': analysisIdDict[index], 'term_name': variable, 'value': value} for index, metadataDict in annotatedDataframe['metadata'].iteritems() for variable, value in metadataDict.iteritems()])
	metadataDataframe['term_name'] = [x.lower() for x in metadataDataframe['term_name']]
	metadataDataframe = metadataDataframe.merge(termDataframe, on='term_name', how='left')

	# Add new terms
	# engine.execute('SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE term;')
	termsToUpload = metadataDataframe.loc[[np.isnan(x) for x in metadataDataframe['term_fk']], 'term_name'].unique()
	if len(termsToUpload) > 0:
		newTermDataframe = db.insertData(pd.DataFrame([[term, ''] for term in termsToUpload], columns=['term_name', 'term_description']), 'term', connection)
		termIdDict = {rowData['term_name']:rowData['id'] for index, rowData in newTermDataframe.iterrows()}
		for term in termsToUpload:
		    metadataDataframe.loc[metadataDataframe['term_name'] == term, 'term_fk'] = termIdDict[term]
	metadataDataframe = metadataDataframe[['canned_analysis_fk', 'term_fk', 'value']]

	# Save
	if len(datasetsToUpload) > 0:
		newDatasetDataframe.iloc[:,[5, 0, 4, 3, 1, 2]].to_csv(datasetOutfile, sep='\t', index=False)
		os.system('touch '+datasetOutfile)
	analysisDataframe.iloc[:, [3, 0, 1, 2]].to_csv(analysisOutfile, sep='\t', index=False)
	metadataDataframe.iloc[:, [0, 2, 1]].to_csv(metadataOutfile, sep='\t', index=False)
	if len(termsToUpload) > 0:
		newTermDataframe.to_csv(termOutfile, sep='\t', index=False)
	else:
		os.system('touch '+termOutfile)
	                                              
	# Confirm
	nb = raw_input('Confirm submission for '+os.path.dirname(loadOutfile)+'? (y/n) ')
	if nb == 'y':
	    # Commit
	    transaction.commit()
	    
	    # Add metadata
	    metadataDataframe.to_sql('canned_analysis_metadata', engine, index=False, if_exists='append')
	    
	    # Outfile
	    os.system('touch '+loadOutfile)
	else:
	    transaction.rollback()
	    for outfile in [datasetOutfile, analysisOutfile, metadataOutfile, termOutfile]:
	    	os.unlink(outfile)


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
#######################################################
pipeline_run([sys.argv[-1]], multiprocess=1, verbose=1)
print('Done!')
