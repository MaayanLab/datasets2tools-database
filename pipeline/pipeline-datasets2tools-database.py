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
import glob, sys, os, time, json, requests, random, sqlalchemy
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import date, timedelta

##### 2. Custom modules #####
# Pipeline running
sys.path.append('pipeline/scripts')
import PipelineDatasets2toolsDatabase as P
import db
from CannedAnalysisTable import CannedAnalysisTable

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
creedsAnalyses = glob.glob('../datasets2tools-canned-analyses/f1-creeds.dir/*/*v1.0-canned_analyses.txt')
archs4Analyses = ['../datasets2tools-canned-analyses/f2-archs4.dir/archs4-canned_analyses.txt']
genemaniaAnalyses = glob.glob('../datasets2tools-canned-analyses/f5-genemania.dir/*canned_analyses.txt')

# Processed datasets
processedDatasetFile = 'f7-processed_datasets.dir/processed_datasets.txt'
scriptsFile = 'f8-scripts.dir/scripts.xlsx'

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
	host, username, password = db.connect(connectionFile, 'phpmyadmin', returnData=True)

	# Get command
	commandString = ''' mysql --user='%(username)s' --password='%(password)s' --host='%(host)s' < %(schemaFile)s; touch %(outfile)s; ''' % locals()

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

	# Add date
	toolDataframe['date'] = '2017-05-22'

	# Select columns
	selectedColumns = ['id', 'tool_name', 'tool_icon_url', 'tool_homepage_url', 'tool_description', 'tool_screenshot_url', 'date']

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools')

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
	        resultDict[row.find('a').text] = {'repository_icon_url': row.find('img').attrs['src'].replace('./', 'https://datamed.org/'),
	                                          'repository_description': row.find_all('td')[-1].text,
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

	# Add date
	repositoryDataframe['date'] = '2017-05-22'

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools')

	# Truncate
	engine.execute('SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE repository; SET FOREIGN_KEY_CHECKS = 1;')

	# Send to SQL
	repositoryDataframe.to_sql('repository', engine, if_exists='append', index=False)

	# Outfile
	os.system('touch %(outfile)s' % locals())

#######################################################
#######################################################
########## S4. Datasets
#######################################################
#######################################################

#############################################
########## 1. Annotate Datasets
#############################################

@follows(mkdir('f4-datasets.dir'))

@transform(creedsAnalyses,
		   regex(r'.*/(.*).txt'),
		   r'f4-datasets.dir/\1-datasets.txt')

def annotateGeoDatasets(infile, outfile):

	# Read infile
	cannedAnalysisDataframe = pd.read_table(infile)

	# Dataset accessions
	datasetAccessions = cannedAnalysisDataframe['dataset_accession'].unique()

	# Annotate
	datasetAnnotationDict = {(i+1): P.annotateDataset(e) for i, e in enumerate(datasetAccessions)}

	# Convert to dataframe
	datasetAnnotationDataframe = pd.DataFrame(datasetAnnotationDict).T

	# Rename columns
	datasetAnnotationDataframe.rename(columns={'title': 'dataset_title', 'summary': 'dataset_description'}, inplace=True)

	# Drop repository name
	datasetAnnotationDataframe.drop('repository_name', axis=1, inplace=True)

	# Add repository FK
	datasetAnnotationDataframe['repository_fk'] = 20

	# Save
	datasetAnnotationDataframe.to_csv(outfile, sep='\t', index=False)
	
#############################################
########## 2. Get LINCS Datasets
#############################################

@files(None,
	   'f4-datasets.dir/lincs-datasets.txt')

def getLincsDatasets(infile, outfile):

	responseDict = requests.post('http://dev3.ccs.miami.edu:8080/dcic/api/fetchdata?searchTerm=*&limit=300').json()
	datasetDataframe = pd.DataFrame([{x: y[x] if x in y.keys() else '-' for x in ['datasetid', 'datasetname', 'description', 'ldplink']} for y in responseDict['results']['documents']])
	datasetDataframe['repository_fk'] = 27
	datasetDataframe.rename(columns={'datasetid': 'dataset_accession', 'datasetname': 'dataset_title', 'description': 'dataset_description', 'ldplink': 'dataset_landing_url'}, inplace=True)
	datasetDataframe.to_csv(outfile, sep='\t', index=False)
	
#############################################
########## 3. Merge Datasets
#############################################

@merge(glob.glob('f4-datasets.dir/*-datasets.txt'),
	   'f4-datasets.dir/datasets.txt')

def mergeDatasets(infiles, outfile):

	# Read infile
	datasetDataframe = pd.concat([pd.read_table(x) for x in infiles]).drop_duplicates('dataset_accession')

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools')

	# Save
	datasetDataframe.to_csv(outfile, sep='\t', index=False)
	
#############################################
########## 4. Upload Datasets
#############################################

@follows(loadRepositories)

@transform('f4-datasets.dir/datasets.txt',#mergeDatasets,
		   suffix('.txt'),
		   '.load')

def loadDatasets(infile, outfile):

	# Read infile
	datasetDataframe = pd.read_table(infile)

	# Add date
	datasetDataframe['date'] = '2017-05-22'

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools')

	# Truncate
	engine.execute('SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE dataset; SET FOREIGN_KEY_CHECKS = 1;')

	# Send to SQL
	datasetDataframe.to_sql('dataset', engine, if_exists='append', index=False)

	# Outfile
	os.system('touch %(outfile)s' % locals())

#######################################################
#######################################################
########## S5. Load Analyses
#######################################################
#######################################################

#############################################
########## 1. Load Analyses
#############################################

@follows(mkdir('f5-analyses.dir'))

@transform(archs4Analyses,
		   regex(r'.*/(.*).txt'),
		   r'f5-analyses.dir/\1.load')

def loadAnalyses(infile, outfile):

	# Read data
	cannedAnalysisDataframe = pd.read_table(infile)

	# Prepare POST request
	url = 'http://localhost:5000/datasets2tools/api/upload'
	data = cannedAnalysisDataframe.to_json()
	headers = {'content-type':'application/json'}

	# Make request
	response = requests.post(url, data=data, headers=headers)

	# Write outfile
	with open(outfile, 'w') as openfile:
		openfile.write(response.text)

#######################################################
#######################################################
########## S6. Featured Objects
#######################################################
#######################################################

#############################################
########## 1. Featured Analyses
#############################################

@follows(mkdir('f6-featured.dir'))

@files(None,
	   'f6-featured.dir/featured-analysis.txt')

def getFeaturedAnalyses(infile, outfile):

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools')

	# Define dict
	featured_analysis_dict = {}

	# Get N
	N = 1500

	# Get IDs
	canned_analysis_ids = pd.read_sql_query('SELECT DISTINCT id FROM canned_analysis', engine)['id'].tolist()
	random.shuffle(canned_analysis_ids)
	featured_analysis_dict['canned_analysis_fk'] = canned_analysis_ids[:N]

	# Get dates
	startdate = date(2017, 5, 1)
	featured_analysis_dict['day'] = [startdate+timedelta(days=i+1) for i in range(N)]

	# Get dataframe
	featured_analysis_dataframe = pd.DataFrame(featured_analysis_dict)

	# Save
	featured_analysis_dataframe.to_csv(outfile, sep='\t', index=False)

#############################################
########## 2. Featured Datasets
#############################################

@files(None,
	   'f6-featured.dir/featured-dataset.txt')

def getFeaturedDatasets(infile, outfile):

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools')

	# Define dict
	featured_dataset_dict = {}

	# Get N
	N = 1500

	# Get IDs
	dataset_ids = pd.read_sql_query('SELECT DISTINCT dataset_fk FROM canned_analysis ca LEFT JOIN dataset d ON d.id=ca.dataset_fk WHERE dataset_title IS NOT NULL', engine)['dataset_fk'].tolist()
	random.shuffle(dataset_ids)
	featured_dataset_dict['dataset_fk'] = dataset_ids[:N]

	# Get dates
	startdate = date(2017, 5, 1)
	featured_dataset_dict['day'] = [startdate+timedelta(days=i) for i in range(N)]

	# Get dataframe
	featured_dataset_dataframe = pd.DataFrame(featured_dataset_dict)

	# Save
	featured_dataset_dataframe.to_csv(outfile, sep='\t', index=False)

#############################################
########## 3. Featured Tools
#############################################

@files(None,
	   'f6-featured.dir/featured-tool.txt')

def getFeaturedTools(infile, outfile):

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools')

	# Define dict
	featured_tool_dict = {'tool_fk': []}

	# Get IDs
	tool_ids = pd.read_sql_query('SELECT DISTINCT tool_fk FROM canned_analysis', engine)['tool_fk'].tolist()
	for i in range(50):
		random.shuffle(tool_ids)
		featured_tool_dict['tool_fk'] += tool_ids

	# Get dates
	startdate = date(2017, 5, 1)
	featured_tool_dict['start_day'] = [startdate+timedelta(days=i*7) for i in range(len(featured_tool_dict['tool_fk']))]
	featured_tool_dict['end_day'] = [x+timedelta(days=7) for x in featured_tool_dict['start_day']]

	# Get dataframe
	featured_tool_dataframe = pd.DataFrame(featured_tool_dict)

	# Save
	featured_tool_dataframe.to_csv(outfile, sep='\t', index=False)

#############################################
########## 4. Upload Tables
#############################################

@transform((getFeaturedAnalyses, getFeaturedDatasets, getFeaturedTools),
		   suffix('.txt'),
	       '.load')

def loadFeaturedTables(infile, outfile):

	# Read infile
	featuredDataframe = pd.read_table(infile)

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools')

	# Get table name
	tableName = os.path.basename(outfile).split('.')[0].replace('-', '_')

	# Get dtype
	dtype = {x: sqlalchemy.types.Integer if 'fk' in x else sqlalchemy.types.Date for x in featuredDataframe.columns}

	# Upload
	featuredDataframe.to_sql(tableName, engine, if_exists='append', index=False, dtype=dtype)

	# Create outfile
	os.system('touch '+outfile)

#######################################################
#######################################################
########## S7. Processed Datasets
#######################################################
#######################################################

#############################################
########## 1. Upload
#############################################

@transform(processedDatasetFile,
		   suffix('.txt'),
		   '.load')

def loadProcessedDatasets(infile, outfile):

	# Read table
	processed_dataset_dataframe = pd.read_table(infile)

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools_dev')

	# Load
	processed_dataset_dataframe.to_sql('processed_dataset', engine, if_exists='append', index=False)

	# Create outfile
	os.system('touch {outfile}'.format(**locals()))

#######################################################
#######################################################
########## S8. Scripts
#######################################################
#######################################################

#############################################
########## 1. Upload
#############################################

@transform(scriptsFile,
		   suffix('.xlsx'),
		   '.load')

def loadScripts(infile, outfile):

	# Read table
	scripts_dataframe = pd.read_excel(infile)

	# Get engine
	engine = db.connect(connectionFile, 'phpmyadmin', 'datasets2tools_dev')

	# Load
	scripts_dataframe['id'] = [x+1 for x in scripts_dataframe.index]
	scripts_dataframe.to_sql('script', engine, if_exists='replace', index=False)

	# Create outfile
	os.system('touch {outfile}'.format(**locals()))


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
