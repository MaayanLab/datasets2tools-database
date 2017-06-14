#################################################################
#################################################################
############### Datasets2Tools Database Support #################
#################################################################
#################################################################
##### Author: Denis Torre
##### Affiliation: Ma'ayan Laboratory,
##### Icahn School of Medicine at Mount Sinai

#############################################
########## 1. Load libraries
#############################################
##### 1. Python modules #####
import sys, urllib
import xml.etree.ElementTree as ET

##### 2. Custom modules #####
# Pipeline running
sys.path.append('/Users/denis/Documents/Projects/scripts')
import Support as S 

#############################################
########## 2. General Setup
#############################################
##### 1. Variables #####

#######################################################
#######################################################
########## S1. Dataset
#######################################################
#######################################################

#############################################
########## 1. Annotate Dataset 
#############################################

def annotateDataset(dataset_accession, attributes = ['title', 'summary']):
    if dataset_accession[:3] in ['GDS', 'GSE']:
        try:
            geoId = ET.fromstring(urllib.urlopen('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gds&term={dataset_accession}%5BAccession%20ID%5D'.format(**locals())).read()).findall('IdList')[0][0].text
            root = ET.fromstring(urllib.urlopen('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gds&id={geoId}'.format(**locals())).read())
            annotDict = {x.attrib['Name']: x.text.encode('ascii', 'ignore').replace('%', '%%').replace('"', "'") for x in root.find('DocSum') if 'Name' in x.attrib.keys() and x.attrib['Name'] in attributes}
            annotDict['dataset_landing_url'] = 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc='+dataset_accession if dataset_accession[:3] == 'GDS' else 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc='+dataset_accession
            annotDict['repository_name'] = 'gene expression omnibus'
            annotDict['dataset_accession'] = dataset_accession
        except:
            annotDict = {'title': '', 'summary': '', 'repository_name': 'gene expression omnibus', 'dataset_landing_url': '', 'dataset_accession': dataset_accession}
    else:
        annotDict = {'title': '', 'summary': '', 'repository_name': '', 'dataset_landing_url': '', 'dataset_accession': dataset_accession}
    return annotDict

#######################################################
#######################################################
########## S. 
#######################################################
#######################################################

#############################################
########## . 
#############################################

