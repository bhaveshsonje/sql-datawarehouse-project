/*
==========================================================
DDL Script: Create Bronze Tables
==========================================================

Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables
==========================================================
*/USE bronze;
    
    SET @start_time = NOW();
	LOAD DATA LOCAL INFILE '/Users/bhaveshsonje/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	INTO TABLE crm_cust_info
	FIELDS TERMINATED BY ','
	IGNORE 1 ROWS;
    SET @end_time = NOW();
    SELECT CONCAT('LOAD DURATION: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time),' seconds') AS message;
    
    SET @start_time = NOW();
	LOAD DATA LOCAL INFILE '/Users/bhaveshsonje/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	INTO TABLE crm_prd_info
	FIELDS TERMINATED BY ','
	IGNORE 1 ROWS;
    SET @end_time = NOW();
    SELECT CONCAT('LOAD DURATION: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time),' seconds') AS message;
 
    SET @start_time = NOW();
	LOAD DATA LOCAL INFILE '/Users/bhaveshsonje/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	INTO TABLE crm_sales_details
	FIELDS TERMINATED BY ','
	IGNORE 1 ROWS;
    SET @end_time = NOW();
    SELECT CONCAT('LOAD DURATION: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time),' seconds') AS message;
    
    SET @start_time = NOW();
	LOAD DATA LOCAL INFILE '/Users/bhaveshsonje/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
	INTO TABLE erp_cust_az12
	FIELDS TERMINATED BY ','
	IGNORE 1 ROWS;
    SET @end_time = NOW();
    SELECT CONCAT('LOAD DURATION: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time),' seconds') AS message;
    
    SET @start_time = NOW();
	LOAD DATA LOCAL INFILE '/Users/bhaveshsonje/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
	INTO TABLE erp_loc_a101
	FIELDS TERMINATED BY ','
	IGNORE 1 ROWS;
    SET @end_time = NOW();
    SELECT CONCAT('LOAD DURATION: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time),' seconds') AS message;
    
    
    SET @start_time = NOW();
	LOAD DATA LOCAL INFILE '/Users/bhaveshsonje/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
	INTO TABLE erp_px_cat_g1v2
	FIELDS TERMINATED BY ','
	IGNORE 1 ROWS;
    SET @end_time = NOW();
    SELECT CONCAT('LOAD DURATION: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time),' seconds') AS message;
    
  








