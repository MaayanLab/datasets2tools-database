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
########## S1. Database Connection
#######################################################
#######################################################

#############################################
########## 1.1 Get Connection String
#############################################

def getConnectionString(label, jsonfile, dbname):

	# Read File
	with open(jsonfile, 'r') as openfile:

		# Read to dictionary
		connectionDict = json.load(openfile)

	# Get string
	connectionString = connectionDict[label]

	# Add database
	if dbname:
		connectionString = os.path.join(connectionString, dbname)

	# Return result
	return connectionString

#############################################
########## 1.2 Setup Connection
#############################################

def createEngine(connectionString):

	# Get Engine
	engine = sqlalchemy.create_engine(connectionString)

	# Return Engine
	return engine

#############################################
########## 1.3 Wrapper
#############################################

def create(label, jsonfile, dbname=False):

	# Get Connection String
	connectionString = getConnectionString(label, jsonfile, dbname)

	# Create Engine
	engine = createEngine(connectionString)

	# Return Engine
	return engine

#######################################################
#######################################################
########## S2. Data Manipulation
#######################################################
#######################################################

#############################################
########## 2.1 Execute Query
#############################################

def executeQuery(queryString, engine):

	# Get Table
	result_dataframe = pd.read_sql_query(queryString, engine)

	# Return Dataframe
	return result_dataframe

#############################################
########## 2.2 Execute Command
#############################################

def executeCommand(commandString, engine):

	# Get Connection
	connection = engine.connect()

	# Execute Command
	connection.execute(commandString)

	# Close connection
	connection.close()

#######################################################
#######################################################
########## S. 
#######################################################
#######################################################

#############################################
########## . 
#############################################


