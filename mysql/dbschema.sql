############################################################
############################################################
############### Datasets2Tools Schema Definition ###########
############################################################
############################################################

#######################################################
########## 1. Create Tables ###########################
#######################################################

##############################
##### 1.1 Dataset
##############################

DROP TABLE IF EXISTS `dataset`;
CREATE TABLE `dataset` (
	# Fields
	`id` INT AUTO_INCREMENT PRIMARY KEY,
	`accession` VARCHAR(30) NOT NULL
);

##############################
##### 1.2 Tool
##############################

DROP TABLE IF EXISTS `tool`;
CREATE TABLE `tool` (
	# Fields
	`id` INT AUTO_INCREMENT PRIMARY KEY,
	`tool_name` VARCHAR(30) NOT NULL,
	`tool_icon_url` VARCHAR(100) NOT NULL,
	`tool_homepage_url` VARCHAR(100) NOT NULL,
	`tool_reference_url` VARCHAR(100),
	`tool_description` TEXT
);

##############################
##### 1.3 Canned Analysis
##############################

DROP TABLE IF EXISTS `canned_analysis`;
CREATE TABLE `canned_analysis` (
	# Fields
	`id` INT AUTO_INCREMENT PRIMARY KEY,
	`dataset_fk` INT NOT NULL,
	`tool_fk` INT NOT NULL,
	`canned_analysis_url` TEXT NOT NULL
);

##############################
##### 1.4 Metadata
##############################

DROP TABLE IF EXISTS `canned_analysis_metadata`;
CREATE TABLE `canned_analysis_metadata` (
	# Fields
	`id` INT AUTO_INCREMENT PRIMARY KEY,
	`canned_analysis_fk` INT NOT NULL,
	`variable` VARCHAR(50) NOT NULL,
	`value` VARCHAR(100) NOT NULL
);
