-- =====================================================
-- Medallion Architecture Setup (MySQL)
-- Bronze | Silver | Gold Databases Creation Script
-- =====================================================

-- -----------------------------
-- 1. Drop existing databases (if they exist)
-- -----------------------------
DROP DATABASE IF EXISTS bronze;
DROP DATABASE IF EXISTS silver;
DROP DATABASE IF EXISTS gold;

-- -----------------------------
-- 2. Create fresh databases
-- -----------------------------
CREATE DATABASE bronze;
CREATE DATABASE silver;
CREATE DATABASE gold;

-- -----------------------------
-- 3. Verify creation
-- -----------------------------
SHOW DATABASES;

-- -----------------------------
-- 4. (Optional) Set default database
-- -----------------------------
-- USE bronze;
-- USE silver;
-- USE gold;

-- =====================================================
-- Notes:
-- - Bronze  : Raw data layer
-- - Silver  : Cleaned/processed data layer
-- - Gold    : Aggregated/business layer
-- - In MySQL, DATABASE = SCHEMA
-- =====================================================
