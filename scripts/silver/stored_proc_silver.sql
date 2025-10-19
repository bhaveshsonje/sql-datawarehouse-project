/*
============================================================
Stored Procedure: load_silver
============================================================

Procedure Purpose:
  This stored procedure automates the transformation and loading 
  of data from the 'bronze' layer to the 'silver' layer within 
  the data warehouse.

  It performs the following operations:
    - Cleanses, standardizes, and enriches source data
    - Applies business transformation logic to CRM and ERP tables
    - Recreates target Silver tables before inserting processed data
    - Tracks and logs individual table load durations and total runtime

  Run this procedure to refresh the complete Silver data layer 
  after Bronze data ingestion.

============================================================
*/

DELIMITER $$

DROP PROCEDURE IF EXISTS load_silver $$
CREATE PROCEDURE load_silver()
BEGIN
  DECLARE start_time   DATETIME;
  DECLARE end_time     DATETIME;
  DECLARE batch_start  DATETIME;
  DECLARE batch_end    DATETIME;

  CREATE TEMPORARY TABLE _etl_log (
    seq INT AUTO_INCREMENT PRIMARY KEY,
    ts  DATETIME DEFAULT NOW(),
    msg TEXT
  );

  SET batch_start = NOW();
  INSERT INTO _etl_log (msg) VALUES
    ('Loading Silver Layer'),
    ('----------------------------------------'),
    ('Loading CRM Tables'),
    ('----------------------------------------');

  /* crm_cust_info */
  SET @old_sql_mode = @@sql_mode;
  SET sql_mode = 'NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

  SET start_time = NOW();
  INSERT INTO _etl_log (msg) VALUES ('>> Recreating: silver.crm_cust_info');

  DROP TABLE IF EXISTS silver.crm_cust_info;
  CREATE TABLE silver.crm_cust_info AS
  SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    CASE
      WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
      WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
      ELSE 'n/a'
    END AS cst_marital_status,
    CASE
      WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
      WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
      ELSE 'n/a'
    END AS cst_gender,
    IF(CAST(cst_create_date AS CHAR) = '0000-00-00', NULL, cst_create_date) AS cst_create_date
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY cst_id
        ORDER BY IF(CAST(cst_create_date AS CHAR) = '0000-00-00', '9999-12-31', cst_create_date) DESC
      ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
  ) t
  WHERE flag_last = 1;

  SET end_time = NOW();
  INSERT INTO _etl_log (msg)
  SELECT CONCAT('>> Load Duration (crm_cust_info): ',
                TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  SET sql_mode = @old_sql_mode;

  /* crm_prd_info */
  SET start_time = NOW();
  INSERT INTO _etl_log (msg) VALUES ('>> Recreating: silver.crm_prd_info');

  DROP TABLE IF EXISTS silver.crm_prd_info;
  CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  INSERT INTO silver.crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
  )
  SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7)                       AS prd_key,
    prd_nm,
    COALESCE(prd_cost, 0)                       AS prd_cost,
    CASE UPPER(TRIM(prd_line))
      WHEN 'M' THEN 'Mountain'
      WHEN 'R' THEN 'Road'
      WHEN 'S' THEN 'Other Sales'
      WHEN 'T' THEN 'Touring'
      ELSE 'n/a'
    END                                         AS prd_line,
    CAST(prd_start_dt AS DATE)                  AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (
          PARTITION BY prd_key ORDER BY prd_start_dt
        ) - INTERVAL 1 DAY AS DATE)             AS prd_end_dt
  FROM bronze.crm_prd_info;

  INSERT INTO _etl_log (msg) SELECT CONCAT('(', ROW_COUNT(), ' rows affected)');
  SET end_time = NOW();
  INSERT INTO _etl_log (msg)
  SELECT CONCAT('>> Load Duration (crm_prd_info): ',
                TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  /* crm_sales_details */
  SET start_time = NOW();
  INSERT INTO _etl_log (msg) VALUES ('>> Recreating: silver.crm_sales_details');

  DROP TABLE IF EXISTS silver.crm_sales_details;
  CREATE TABLE silver.crm_sales_details (
    sls_ord_num   VARCHAR(50),
    sls_prd_key   VARCHAR(50),
    sls_cust_id   VARCHAR(50),
    sls_order_dt  DATE,
    sls_ship_dt   DATE,
    sls_due_dt    DATE,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  INSERT INTO silver.crm_sales_details (
    sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
  )
  SELECT
    sls_ord_num,
    sls_prd_key,
    CAST(sls_cust_id AS CHAR(50)),
    STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d'),
    STR_TO_DATE(CAST(sls_ship_dt  AS CHAR), '%Y%m%d'),
    STR_TO_DATE(CAST(sls_due_dt   AS CHAR), '%Y%m%d'),
    CASE
      WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
      ELSE sls_sales
    END,
    sls_quantity,
    ABS(sls_price)
  FROM bronze.crm_sales_details;

  INSERT INTO _etl_log (msg) SELECT CONCAT('(', ROW_COUNT(), ' rows affected)');
  SET end_time = NOW();
  INSERT INTO _etl_log (msg)
  SELECT CONCAT('>> Load Duration (crm_sales_details): ',
                TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  /* erp_cust_az12 */
  INSERT INTO _etl_log (msg) VALUES ('----------------------------------------'),
                                    ('Loading ERP Tables'),
                                    ('----------------------------------------');

  SET start_time = NOW();
  INSERT INTO _etl_log (msg) VALUES ('>> Recreating: silver.erp_cust_az12');

  DROP TABLE IF EXISTS silver.erp_cust_az12;
  CREATE TABLE silver.erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gender VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  INSERT INTO silver.erp_cust_az12 (cid, bdate, gender)
  SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
    CASE WHEN bdate IS NULL OR bdate > CURDATE() THEN NULL ELSE bdate END,
    CASE
      WHEN gender IS NULL OR TRIM(gender) = '' THEN 'n/a'
      WHEN UPPER(REPLACE(REPLACE(TRIM(gender), '\r', ''), '\n', '')) IN ('F','FEMALE') THEN 'Female'
      WHEN UPPER(REPLACE(REPLACE(TRIM(gender), '\r', ''), '\n', '')) IN ('M','MALE') THEN 'Male'
      ELSE 'n/a'
    END
  FROM bronze.erp_cust_az12;

  INSERT INTO _etl_log (msg) SELECT CONCAT('(', ROW_COUNT(), ' rows affected)');
  SET end_time = NOW();
  INSERT INTO _etl_log (msg)
  SELECT CONCAT('>> Load Duration (erp_cust_az12): ',
                TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  /* erp_loc_a101 */
  SET start_time = NOW();
  INSERT INTO _etl_log (msg) VALUES ('>> Recreating: silver.erp_loc_a101');

  DROP TABLE IF EXISTS silver.erp_loc_a101;
  CREATE TABLE silver.erp_loc_a101 AS
  SELECT * FROM bronze.erp_loc_a101;

  INSERT INTO _etl_log (msg) SELECT CONCAT('(', ROW_COUNT(), ' rows affected)');
  ALTER TABLE silver.erp_loc_a101
    ADD COLUMN dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP;

  SET end_time = NOW();
  INSERT INTO _etl_log (msg)
  SELECT CONCAT('>> Load Duration (erp_loc_a101): ',
                TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  /* erp_px_cat_g1v2 */
  SET start_time = NOW();
  INSERT INTO _etl_log (msg) VALUES ('>> Recreating: silver.erp_px_cat_g1v2');

  DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
  CREATE TABLE silver.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(100),
    subcat VARCHAR(100),
    maintenance VARCHAR(10),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
  SELECT id, cat, subcat, maintenance
  FROM bronze.erp_px_cat_g1v2;

  INSERT INTO _etl_log (msg) SELECT CONCAT('(', ROW_COUNT(), ' rows affected)');
  SET end_time = NOW();
  INSERT INTO _etl_log (msg)
  SELECT CONCAT('>> Load Duration (erp_px_cat_g1v2): ',
                TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  SET batch_end = NOW();
  INSERT INTO _etl_log (msg) VALUES ('========================================');
  INSERT INTO _etl_log (msg) SELECT CONCAT('Batch Start: ', batch_start);
  INSERT INTO _etl_log (msg) SELECT CONCAT('Batch End  : ', batch_end);
  INSERT INTO _etl_log (msg) SELECT CONCAT('Total Secs : ',
                                           TIMESTAMPDIFF(SECOND, batch_start, batch_end));
  INSERT INTO _etl_log (msg) VALUES ('========================================');

  SELECT ts, msg FROM _etl_log ORDER BY seq;
END $$

DELIMITER ;

CALL load_silver();
