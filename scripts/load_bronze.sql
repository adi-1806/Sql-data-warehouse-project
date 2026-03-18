-- ============================================================
-- File: load_bronze.sql
-- Purpose:
--   Reload all Bronze layer tables from CSV files.
--
-- Notes:
--   1. This script must be executed from mysql CLI using:
--      mysql --local-infile=1 -u root -p < 03_load_bronze.sql
--   2. LOAD DATA LOCAL INFILE is not allowed inside stored procedures.
--   3. This script records start time, end time, and duration for:
--        - each table load
--        - total bronze batch load
--   4. If any statement fails, mysql usually stops execution unless
--      the script is run with --force.
--   5. We can use "stored procedures" if we execut it in microsoft sql server.
--   6. In mysql, it is not allowed to load in stored procedures.
-- ============================================================

USE DWBarra_bronze;

-- ============================================================
-- Batch start time
-- ============================================================
SET @bronze_batch_start = NOW(6);

SELECT 'Bronze batch load started' AS message, @bronze_batch_start AS batch_start_time;

-- ============================================================
-- 1. Load crm_cust_info
-- ============================================================
SET @t1_start = NOW(6);

TRUNCATE TABLE DWBarra_bronze.crm_cust_info;

LOAD DATA LOCAL INFILE '/Users/peddiadithyavardhan/Downloads/MySQL/MySQL with Baraa/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE DWBarra_bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date);

SET @t1_end = NOW(6);

SELECT
    'crm_cust_info loaded successfully' AS message,
    @t1_start AS load_start_time,
    @t1_end AS load_end_time,
    TIMESTAMPDIFF(MICROSECOND, @t1_start, @t1_end) / 1000000 AS load_duration_seconds,
    (SELECT COUNT(*) FROM DWBarra_bronze.crm_cust_info) AS row_count;

-- ============================================================
-- 2. Load crm_prd_info
-- ============================================================
SET @t2_start = NOW(6);

TRUNCATE TABLE DWBarra_bronze.crm_prd_info;

LOAD DATA LOCAL INFILE '/Users/peddiadithyavardhan/Downloads/MySQL/MySQL with Baraa/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE DWBarra_bronze.crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @t2_end = NOW(6);

SELECT
    'crm_prd_info loaded successfully' AS message,
    @t2_start AS load_start_time,
    @t2_end AS load_end_time,
    TIMESTAMPDIFF(MICROSECOND, @t2_start, @t2_end) / 1000000 AS load_duration_seconds,
    (SELECT COUNT(*) FROM DWBarra_bronze.crm_prd_info) AS row_count;

-- ============================================================
-- 3. Load crm_sales_details
-- ============================================================
SET @t3_start = NOW(6);

TRUNCATE TABLE DWBarra_bronze.crm_sales_details;

LOAD DATA LOCAL INFILE '/Users/peddiadithyavardhan/Downloads/MySQL/MySQL with Baraa/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE DWBarra_bronze.crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @t3_end = NOW(6);

SELECT
    'crm_sales_details loaded successfully' AS message,
    @t3_start AS load_start_time,
    @t3_end AS load_end_time,
    TIMESTAMPDIFF(MICROSECOND, @t3_start, @t3_end) / 1000000 AS load_duration_seconds,
    (SELECT COUNT(*) FROM DWBarra_bronze.crm_sales_details) AS row_count;

-- ============================================================
-- 4. Load erp_cust_az12
-- ============================================================
SET @t4_start = NOW(6);

TRUNCATE TABLE DWBarra_bronze.erp_cust_az12;

LOAD DATA LOCAL INFILE '/Users/peddiadithyavardhan/Downloads/MySQL/MySQL with Baraa/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE DWBarra_bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @t4_end = NOW(6);

SELECT
    'erp_cust_az12 loaded successfully' AS message,
    @t4_start AS load_start_time,
    @t4_end AS load_end_time,
    TIMESTAMPDIFF(MICROSECOND, @t4_start, @t4_end) / 1000000 AS load_duration_seconds,
    (SELECT COUNT(*) FROM DWBarra_bronze.erp_cust_az12) AS row_count;

-- ============================================================
-- 5. Load erp_loc_a101
-- ============================================================
SET @t5_start = NOW(6);

TRUNCATE TABLE DWBarra_bronze.erp_loc_a101;

LOAD DATA LOCAL INFILE '/Users/peddiadithyavardhan/Downloads/MySQL/MySQL with Baraa/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE DWBarra_bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @t5_end = NOW(6);

SELECT
    'erp_loc_a101 loaded successfully' AS message,
    @t5_start AS load_start_time,
    @t5_end AS load_end_time,
    TIMESTAMPDIFF(MICROSECOND, @t5_start, @t5_end) / 1000000 AS load_duration_seconds,
    (SELECT COUNT(*) FROM DWBarra_bronze.erp_loc_a101) AS row_count;

-- ============================================================
-- 6. Load erp_px_cat_g1v2
-- ============================================================
SET @t6_start = NOW(6);

TRUNCATE TABLE DWBarra_bronze.erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE '/Users/peddiadithyavardhan/Downloads/MySQL/MySQL with Baraa/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE DWBarra_bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET @t6_end = NOW(6);

SELECT
    'erp_px_cat_g1v2 loaded successfully' AS message,
    @t6_start AS load_start_time,
    @t6_end AS load_end_time,
    TIMESTAMPDIFF(MICROSECOND, @t6_start, @t6_end) / 1000000 AS load_duration_seconds,
    (SELECT COUNT(*) FROM DWBarra_bronze.erp_px_cat_g1v2) AS row_count;

-- ============================================================
-- Batch end time and total duration
-- ============================================================
SET @bronze_batch_end = NOW(6);

SELECT
    'Bronze batch load completed' AS message,
    @bronze_batch_start AS batch_start_time,
    @bronze_batch_end AS batch_end_time,
    TIMESTAMPDIFF(MICROSECOND, @bronze_batch_start, @bronze_batch_end) / 1000000 AS total_batch_duration_seconds;
