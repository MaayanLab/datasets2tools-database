############################################################
############################################################
############### Datasets2Tools Initial Upload ##############
############################################################
############################################################

#######################################################
########## 1. Setup ###################################
#######################################################

##############################
##### 1.1 Select Database
##############################a

### Use database
USE `datasets2tools`;

### Disable foreign key checks
SET FOREIGN_KEY_CHECKS=0;

#######################################################
########## 1. Add data ################################
#######################################################

##############################
##### 2.1 Database
##############################

INSERT INTO `db` (`db_name`, `db_url`, `db_icon_url`) VALUES
	('GEO', 'https://www.ncbi.nlm.nih.gov/geo/', 'https://www.ncbi.nlm.nih.gov/geo/img/geo_main.gif'),
    ('LINCS', 'http://lincsportal.ccs.miami.edu/dcic-portal/', 'http://lincs-dcic.org/libs/lincs-direct-access-tools/tool-icons/portal.png'),
    ('BioProject', 'https://www.ncbi.nlm.nih.gov/bioproject/', 'http://www.tast.com.tw/eng/images/www.ncbi.nlm.nih.gov.jpg'),
    ('PDB', 'http://www.rcsb.org/pdb/home/home.do', 'http://cdn.rcsb.org/rcsb-pdb/v2/common/images/rcsb_logo.png'),
    ('SRA', 'https://www.ncbi.nlm.nih.gov/sra', 'http://www.tast.com.tw/eng/images/www.ncbi.nlm.nih.gov.jpg');
#	('ClinicalTrials'),
#	('BioProject'),
#	('PDB'),
#	('GEO'),
#	('Dryad'),
#	('ArrayExpress'),
#	('Dataverse'),
#	('SRA'),
#	('NeuroMorpho.Org'),
#	('GEMMA'),
#	('Proteomexchange'),
#	('dbGaP'),
#	('NURSA'),
#	('LINCS'),
#	('MPD'),
#	('NIDDKCR'),
#	('PeptideAtlas'),
#	('PhysioBank'),
#	('TCIA'),
#	('CTN'),
#	('openFMRI'),
#	('CVRG'),
#	('YPED');


##############################
##### 2.2 Tool
##############################

INSERT INTO `tool` (`tool_name`, `tool_icon_url`, `tool_url`, `tool_description`) VALUES
	('Geo2Enrichr', 'http://amp.pharm.mssm.edu/g2e/static/image/logo-50x50.png', 'http://amp.pharm.mssm.edu/g2e/', 'GEO2Enrichr is a tool for extracting differentially expressed gene sets from GEO and analyzing those sets with Enrichr.'),
	('Enrichr', 'http://amp.pharm.mssm.edu/Enrichr/images/enrichr-icon.png', 'http://amp.pharm.mssm.edu/Enrichr/', 'Enrichr is an easy to use intuitive enrichment analysis web-based tool providing various types of visualization summaries of collective functions of gene lists.'),
	('Clustergrammer', 'http://amp.pharm.mssm.edu/clustergrammer/static/icons/graham_cracker_70.png', 'http://amp.pharm.mssm.edu/clustergrammer/', 'Clustergrammer is a visualization tool that enables users to easily generate highly interactive and shareable clustergram/heatmap visualizations from a matrix of their own data.'),
	('L1000CDS2', 'http://amp.pharm.mssm.edu/L1000CDS2/CSS/images/sigine.png', 'http://amp.pharm.mssm.edu/L1000CDS2/', 'LINCS L1000 characteristic direction signature search engine is a tool which enables users to find consensus L1000 small molecule signatures that match user input signatures.'),
	('Slicr', 'http://labs.icahn.mssm.edu/maayanlab/wp-content/uploads/sites/75/2014/10/slicr2.fw_.png', 'http://amp.pharm.mssm.edu/Slicr', 'Slicr is a metadata search engine that searches for LINCS L1000 gene expression profiles and signatures matching users’ input parameters.');

##############################
##### 2.3 Attribute
##############################

INSERT INTO `attribute` (`tool_fk`, `attribute_name`, `attribute_description`) VALUES
	('1', 'de_method', 'Differential expression method to compute the gene expression signature.'),
	('1', 'cutoff', 'Gene number cutoff to identify genesets, when the differential expression method is Characteristic Direction.'),
	('1', 'correction_method', 'P-value correction method, when the differential expression method is a T-test.'),
	('1', 'pvalue_threshold', 'P-value cutoff threshold, when the differential expresison method is a T-test.');

##############################
##### 2.4 Attribute value
##############################

INSERT INTO `attribute_value` (`attribute_fk`, `value`) VALUES
	('1', 'Characteristic Direction'),
	('1', 'T-test'),
	('2', '1000'),
	('2', '500'),
	('2', '200'),
	('3', 'Bonferroni'),
	('3', 'Benjamini-Hochberg'),
	('4', '0.05'),
	('4', '0.01');
