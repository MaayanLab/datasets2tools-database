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

	# Read dataframe
	inputAnalysisDataframe = pd.read_table(analysisFile, index_col='index').dropna().rename(columns={'geo_id': 'dataset_accession', 'link': 'canned_analysis_url', 'tool': 'tool_name'})

	# Connect
	engine = db.connect(connectionFile, 'localhost', 'datasets2tools')

	# Create object
	analysisTable = CannedAnalysisTable(inputAnalysisDataframe, engine)
	analysisTable.load_data()
	analysisTable.write_files(outfiles)
	analysisTable.commit_transaction(outfiles)
	
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
