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

#############################################
########## 2.3 Load Tables
#############################################

def uploadTable(dataframe, engine, table_name, if_exists='append', index_label=None, index=True):

	# Upload
	dataframe.to_sql(table_name, engine, if_exists=if_exists, index_label=index_label, index=index)

##############################
##### 2.2.1 Insert data
##############################

def insertData(insertCommandString, mysql, returnInsertId=True):
	
	# Create connection
	mysqlConnection = mysql.engine.connect()

	# Insert data
	mysqlConnection.execute(insertCommandString)

	# Return ID
	if returnInsertId:

		# Get insert ID
		lastInsertId = int(mysqlConnection.execute('SELECT LAST_INSERT_ID()').fetchone()[0])

		# Return
		return lastInsertId


##############################
##### 2.2.1 Execute Query
##############################

def insertDataframe(dataframe, tableName, mysql, if_exists='append', index=False, index_label=None):
	
	# Get query result dataframe
	dataframe.to_sql(tableName, mysql, if_exists=if_exists, index=index, index_label=index_label)

	# Get column names
# def insertDataframe(dataframe, tableName, mysql, n=500):

# 	# Get column names
# 	colNames = dataframe.columns

# 	# Set columns string
# 	columnsString = "`, `".join(colNames)

# 	# Convert to matrix
# 	matrix = dataframe[colNames].as_matrix()

# 	# Loop
# 	for i in range(0, len(dataframe.index), n):

# 		print 'insertCommandString'
# 		# Get matrix chunk
# 		matrixChunk = matrix[i:i+n, :]

# 		# Set values string
# 		valuesString = "(" + "),(".join([", ".join(['"%(y)s"' % locals() for y in x]) for x in matrixChunk]) + ")"

# 		# Create insert command
# 		insertCommandString = ''' INSERT IGNORE INTO %(tableName)s
# 			                      (`%(columnsString)s`)
# 			                      VALUES %(valuesString)s''' % locals()

# 		print insertCommandString

# 	    # Insert data
# 		insertData(insertCommandString, mysql, returnInsertId=False)


#######################################################
#######################################################
########## S. 
#######################################################
#######################################################

#############################################
########## . 
#############################################



