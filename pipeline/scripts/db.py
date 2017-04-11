# -*- coding: utf-8 -*-
import json
from sqlalchemy import *
import xml.etree.ElementTree as ET
import urllib

def connect(connectionFile, hostLabel, database=False, returnData=False):

	# Read info
	with open(connectionFile, 'r') as openfile:

		connectionDict = json.load(openfile)

	# Extract info
	host = connectionDict[hostLabel]['host']
	username = connectionDict[hostLabel]['username']
	password = connectionDict[hostLabel]['password']

	# Return data
	if returnData:

		return (host, username, password)

	else:

		# Get string
		if database:
			connectionString = 'mysql://%(username)s:%(password)s@%(host)s/%(database)s' % locals()
		else:
			connectionString = 'mysql://%(username)s:%(password)s@%(host)s' % locals()
			
		# Get engine
		engine = create_engine(connectionString)

		# Return
		return engine



def annotate(geoAccession, attributes = ['title', 'summary', 'taxon', 'gdsType']):
	geoId = ET.fromstring(urllib.urlopen('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gds&term={geoAccession}%5BAccession%20ID%5D'.format(**locals())).read()).findall('IdList')[0][0].text
	root = ET.fromstring(urllib.urlopen('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gds&id={geoId}'.format(**locals())).read())
	annotDict = {x.attrib['Name']: x.text for x in root.find('DocSum') if 'Name' in x.attrib.keys() and x.attrib['Name'] in attributes}
	annotDict['dataset_landing_url'] = 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc='+geoAccession if geoAccession[:3] == 'GDS' else 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc='+geoAccession
	annotDict['repository_name'] = 'gene expression omnibus'
	return annotDict



def insertData(dataframe, tableName, connection):
    for index, rowData in dataframe.iterrows():
        insertCommand = 'INSERT INTO ' + tableName + '(`' + '`, `'.join(rowData.index) + '`) VALUES ("' + '", "'.join([str(x).replace('%', '%%') for x in rowData.values]) + '");'
        connection.execute(insertCommand)
        dataframe.loc[index, 'id'] = int(connection.execute('SELECT LAST_INSERT_ID();').fetchall()[0][0])
    return dataframe