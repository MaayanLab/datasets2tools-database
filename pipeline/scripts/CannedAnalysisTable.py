# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import urllib, json, os, warnings, time

warnings.filterwarnings("ignore")

class CannedAnalysisTable:
    
    def __init__(self, inputAnalysisDataframe, engine, verbose=1):
        cols = ['dataset_accession', 'tool_name', 'canned_analysis_url', 'metadata']
        if not all([x in inputAnalysisDataframe.columns for x in cols]):
            raise ValueError('Dataframe columns must contain all of the following: ' + ', '.join(cols) + '.  Instead, they are: ' + ', '.join(inputAnalysisDataframe.columns) + '.')
        self.input_df = inputAnalysisDataframe.dropna()
        self.engine = engine
        self.verbose = verbose
        
    def fetch_tables(self):
        self.tool_df = pd.read_sql_query('SELECT id AS tool_fk, LCASE(tool_name) AS tool_name FROM tool', self.engine)
        self.dataset_df = pd.read_sql_query('SELECT id AS dataset_fk, dataset_accession AS dataset_accession FROM dataset', self.engine)
        self.repo_df = pd.read_sql_query('SELECT id AS repository_fk, LCASE(repository_name) AS repository_name FROM repository', self.engine)
        self.term_df = pd.read_sql_query('SELECT id AS term_fk, LCASE(term_name) AS term_name FROM term', self.engine)
        self.input_df['tool_name'] = [x.lower() for x in self.input_df['tool_name']]
        self.annotated_df = self.input_df.merge(self.tool_df, on='tool_name', how='left').merge(self.dataset_df, on='dataset_accession', how='left')
        self.connection = self.engine.connect()
        self.transaction = self.connection.begin()
        self.repo_df['repository_name'] = [x.replace('\xc2\xa0', ' ') for x in self.repo_df['repository_name']]

    def insert_dataframe(self, dataframe, tableName, connection):
        for index, rowData in dataframe.iterrows():
            insertCommand = 'INSERT INTO ' + tableName + '(`' + '`, `'.join(rowData.index).encode('ascii', 'ignore') + '`) VALUES ("' + '", "'.join([str(x).encode('ascii', 'ignore').replace('%', '%%') if type(x) != int else str(x) for x in rowData.values]) + '");'
            connection.execute(insertCommand)
            dataframe.loc[index, 'id'] = connection.execute('SELECT LAST_INSERT_ID();').fetchall()[0][0]
        dataframe['id'] = dataframe['id'].astype(int)
        return dataframe

    def annotate_dataset(self, dataset_accession, attributes = ['title', 'summary']):
        if dataset_accession[:3] in ['GDS', 'GSE']:
            try:
                geoId = ET.fromstring(urllib.urlopen('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gds&term={dataset_accession}%5BAccession%20ID%5D'.format(**locals())).read()).findall('IdList')[0][0].text
                root = ET.fromstring(urllib.urlopen('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gds&id={geoId}'.format(**locals())).read())
                annotDict = {x.attrib['Name']: x.text.encode('ascii', 'ignore').replace('%', '%%').replace('"', "'") for x in root.find('DocSum') if 'Name' in x.attrib.keys() and x.attrib['Name'] in attributes}
                annotDict['dataset_landing_url'] = 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc='+dataset_accession if dataset_accession[:3] == 'GDS' else 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc='+dataset_accession
                annotDict['repository_name'] = 'gene expression omnibus'
            except:
                annotDict = {'title': '', 'summary': '', 'repository_name': 'gene expression omnibus', 'dataset_landing_url': ''}
        else:
            annotDict = {'title': '', 'summary': '', 'repository_name': '', 'dataset_landing_url': ''}
        return annotDict
    
    def check_tools(self):
        null_tools = self.annotated_df['tool_fk'].isnull()
        if null_tools.any():
            raise ValueError('Tool(s) ' + ', '.join(self.annotated_df.loc[null_tools, 'tool_name'].unique()) + ' not in database.  Please add them to proceed.')
        else:
            if self.verbose == 1: print 'All ' + str(len(self.annotated_df['tool_fk'].unique())) + ' tools in database.'
            
    def check_datasets(self):
        self.missing_datasets = self.annotated_df.loc[self.annotated_df['dataset_fk'].isnull(), 'dataset_accession'].unique()
        if len(self.missing_datasets) == 0:
            if self.verbose == 1: print 'All ' + str(len(self.annotated_df['dataset_fk'].unique())) + ' datasets in database.'
            self.new_dataset_df = pd.DataFrame()
        else:
            if self.verbose == 1: print 'Adding missing datasets (' + str(len(self.missing_datasets)) + '/' + str(len(self.annotated_df['dataset_accession'].unique())) + '): ' + ', '.join(self.missing_datasets) + '.'
            self.new_dataset_df = pd.DataFrame({x: self.annotate_dataset(x) for x in self.missing_datasets}).T.reset_index().rename(columns={'title': 'dataset_title', 'summary': 'dataset_description', 'index': 'dataset_accession'})
            self.new_dataset_df = self.insert_dataframe(self.new_dataset_df.merge(self.repo_df, on='repository_name', how='left').drop('repository_name', axis=1), 'dataset', self.connection)
            datasetIdDict = {rowData['dataset_accession']:rowData['id'] for index, rowData in self.new_dataset_df.iterrows()}
            for dataset in self.missing_datasets:
                self.annotated_df.loc[self.annotated_df['dataset_accession'] == dataset, 'dataset_fk'] = datasetIdDict[dataset]
                
    def check_terms(self):
        self.missing_terms = self.metadata_df.loc[self.metadata_df['term_fk'].isnull(), 'term_name'].unique()
        if len(self.missing_terms) == 0:
            if self.verbose == 1: print 'All ' + str(len(self.metadata_df['term_fk'].unique())) + ' metadata terms in database.'
            self.new_term_df = pd.DataFrame()
        else:
            if self.verbose == 1: print 'Adding missing metadata terms (' + str(len(self.missing_terms)) + '/' + str(len(self.metadata_df['term_name'].unique())) + '): ' + ', '.join(self.missing_terms) + '.'
            self.new_term_df = self.insert_dataframe(pd.DataFrame([[term, ''] for term in self.missing_terms], columns=['term_name', 'term_description']), 'term', self.connection)
            termIdDict = {rowData['term_name']:rowData['id'] for index, rowData in self.new_term_df.iterrows()}
            for term in self.missing_terms:
                self.metadata_df.loc[self.metadata_df['term_name'] == term, 'term_fk'] = termIdDict[term]
        self.metadata_df = self.metadata_df[['canned_analysis_fk', 'term_fk', 'value']]
    
    def load_analyses(self):
        if self.verbose == 1: print 'Adding ' + str(len(self.annotated_df.index)) + ' canned analyses.'
        self.analysis_df = self.insert_dataframe(self.annotated_df[['dataset_fk', 'tool_fk', 'canned_analysis_url', 'canned_analysis_title', 'canned_analysis_description', 'canned_analysis_preview_url']], 'canned_analysis', self.connection)
        analysisIdDict = {index:rowData['id'] for index, rowData in self.analysis_df.iterrows()}
        self.annotated_df['metadata'] = [json.loads(x) for x in self.annotated_df['metadata']]
        self.metadata_df = pd.DataFrame([{'canned_analysis_fk': analysisIdDict[index], 'term_name': variable.encode('ascii', 'ignore'), 'value': str(value).encode('ascii', 'ignore')} for index, metadataDict in self.annotated_df['metadata'].iteritems() for variable, value in metadataDict.iteritems()])
        self.metadata_df = self.metadata_df.merge(self.term_df, on='term_name', how='left')
        self.check_terms()

    def commit_transaction(self, outfiles):
        confirm = raw_input('\nCommit? (y/n) ')
        if confirm == 'y':
            self.transaction.commit()
            for metadata_df_split in np.array_split(self.metadata_df, max(len(self.metadata_df.index)/5000, 1)):
                metadata_df_split.to_sql('canned_analysis_metadata', self.engine, index=False, if_exists='append')
            os.system('touch '+outfiles[-1])
        else:
            self.transaction.rollback()
            for outfile in outfiles[:4]:
                os.unlink(outfile)
            
    def write_files(self, outfiles):
        self.new_dataset_df.to_csv(outfiles[0], sep='\t', index=False)
        self.analysis_df.to_csv(outfiles[1], sep='\t', index=False)
        self.metadata_df.to_csv(outfiles[2], sep='\t', index=False)
        self.new_term_df.to_csv(outfiles[3], sep='\t', index=False)
            
    def load_data(self):
        self.fetch_tables()
        self.check_tools()
        self.check_datasets()
        self.load_analyses()