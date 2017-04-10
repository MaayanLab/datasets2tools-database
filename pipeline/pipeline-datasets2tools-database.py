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
import db

#############################################
########## 2. General setup
#############################################
##### 1. Default variables #####
# DB Files
schemaFile = 'f1-mysql.dir/schema.sql'
connectionFile = 'f1-mysql.dir/conn.json'

# Canned Analyses
creedsAnalyses = glob.glob('../datasets2tools-canned-analyses/f1-creeds.dir/*/*-canned_analyses.txt')
archsAnalyses = '../datasets2tools-canned-analyses/f1-creeds.dir/archs-canned_analyses.txt'
clustergrammerAnalyses = glob.glob('../datasets2tools-canned-analyses/f3-geo.dir/*/*/*-canned_analyses.txt')

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
########## S2. Load Tools
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
########## S2. Load Canned Analyses
#######################################################
#######################################################

#############################################
########## 1. Create Links
#############################################

# @follows(mkdir('f3-analyses.dir'))





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
