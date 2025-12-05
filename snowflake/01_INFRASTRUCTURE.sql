-- ============================================================================
-- FTFP V1 - PHASE 1: INFRASTRUCTURE SETUP
-- ============================================================================
-- Creates database, schemas, tables, and stage for seed data
-- 
-- AFTER running this script:
--   1. Upload seed data files to the stage (see instructions at end)
--   2. Run 02_LOAD_DATA_AND_DEPLOY.sql
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- CONFIGURATION
-- ============================================================================
SET DB_NAME = 'FTFP_V1';
SET WH_NAME = 'FTFP_V1_WH';
SET POOL_NAME = 'FTFP_V1_POOL';

SELECT '🚀 FTFP V1 - Phase 1: Infrastructure Setup' AS STATUS;
SELECT 'Database: ' || $DB_NAME AS CONFIG;

-- ============================================================================
-- STEP 1: CREATE WAREHOUSE
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($WH_NAME)
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'FTFP V1 Demo Warehouse';

USE WAREHOUSE IDENTIFIER($WH_NAME);
SELECT '✅ Warehouse created: ' || $WH_NAME AS STATUS;

-- ============================================================================
-- STEP 2: CREATE DATABASE AND SCHEMAS
-- ============================================================================
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB_NAME)
    COMMENT = 'Fleet Telemetry Failure Prediction V1';

USE DATABASE IDENTIFIER($DB_NAME);

CREATE SCHEMA IF NOT EXISTS FTFP COMMENT = 'Main data tables and views';
CREATE SCHEMA IF NOT EXISTS ML COMMENT = 'ML UDFs';
CREATE SCHEMA IF NOT EXISTS IMAGES COMMENT = 'Container images';
CREATE SCHEMA IF NOT EXISTS SERVICE COMMENT = 'SPCS service';

SELECT '✅ Database and schemas created' AS STATUS;

-- ============================================================================
-- STEP 3: CREATE DATA TABLES
-- ============================================================================
USE SCHEMA FTFP;

-- Telemetry and seed tables
CREATE TABLE IF NOT EXISTS TELEMETRY (
    TIMESTAMP TIMESTAMP_NTZ(9), ENTITY_ID VARCHAR(100),
    ENGINE_TEMP FLOAT, TRANS_OIL_PRESSURE FLOAT, BATTERY_VOLTAGE FLOAT,
    STATUS VARCHAR(20) DEFAULT 'NORMAL'
);

CREATE TABLE IF NOT EXISTS NORMAL_SEED (
    ENTITY_ID VARCHAR(100), EPOCH NUMBER(38,0),
    ENGINE_TEMP FLOAT, TRANS_OIL_PRESSURE FLOAT, BATTERY_VOLTAGE FLOAT
);

CREATE TABLE IF NOT EXISTS ENGINE_FAILURE_SEED (
    EPOCH NUMBER(38,0), ENGINE_TEMP FLOAT, TRANS_OIL_PRESSURE FLOAT, BATTERY_VOLTAGE FLOAT
);

CREATE TABLE IF NOT EXISTS TRANSMISSION_FAILURE_SEED (
    EPOCH NUMBER(38,0), ENGINE_TEMP FLOAT, TRANS_OIL_PRESSURE FLOAT, BATTERY_VOLTAGE FLOAT
);

CREATE TABLE IF NOT EXISTS ELECTRICAL_FAILURE_SEED (
    EPOCH NUMBER(38,0), ENGINE_TEMP FLOAT, TRANS_OIL_PRESSURE FLOAT, BATTERY_VOLTAGE FLOAT
);

-- State tracking tables
CREATE TABLE IF NOT EXISTS STREAM_STATE (
    STREAM_NAME VARCHAR(100) NOT NULL PRIMARY KEY, START_TS TIMESTAMP_NTZ(9),
    NEXT_EPOCH NUMBER(38,0), STEP_SECONDS NUMBER(38,0), LAST_UPDATED TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS FAILURE_CONFIG (
    ENTITY_ID VARCHAR(100) NOT NULL PRIMARY KEY, FAILURE_TYPE VARCHAR(50),
    ENABLED BOOLEAN, EFFECTIVE_FROM_EPOCH NUMBER(38,0), FAILURE_NEXT_EPOCH NUMBER(38,0),
    CREATED_AT TIMESTAMP_NTZ(9), UPDATED_AT TIMESTAMP_NTZ(9)
);

-- ML cache tables
CREATE TABLE IF NOT EXISTS PREDICTION_CACHE (
    ENTITY_ID VARCHAR(100) NOT NULL PRIMARY KEY, PREDICTION_TIMESTAMP TIMESTAMP_NTZ(9) NOT NULL,
    PREDICTED_FAILURE_TYPE VARCHAR(50), PREDICTED_HOURS_TO_FAILURE FLOAT,
    TTF_MODEL_USED VARCHAR(50), CURRENT_ENGINE_TEMP FLOAT, CURRENT_TRANS_PRESSURE FLOAT,
    CURRENT_BATTERY_VOLTAGE FLOAT, LAST_UPDATED TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS ACTIVE_FAILURES (
    ENTITY_ID VARCHAR(100) NOT NULL PRIMARY KEY, FAILURE_TYPE VARCHAR(50),
    STARTED_AT TIMESTAMP_NTZ(9), EPOCH_STARTED NUMBER(38,0), LAST_UPDATED TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS FIRST_FAILURE_MARKERS (
    ENTITY_ID VARCHAR(100) NOT NULL PRIMARY KEY, FIRST_FAILURE_TIME TIMESTAMP_NTZ(9),
    FAILURE_TYPE VARCHAR(50), LAST_UPDATED TIMESTAMP_NTZ(9)
);

-- Initialize stream state
INSERT INTO STREAM_STATE (STREAM_NAME, START_TS, STEP_SECONDS, NEXT_EPOCH, LAST_UPDATED)
SELECT 'NORMAL_TO_TELEMETRY', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 5, 0, CURRENT_TIMESTAMP()
WHERE NOT EXISTS (SELECT 1 FROM STREAM_STATE WHERE STREAM_NAME = 'NORMAL_TO_TELEMETRY');

SELECT '✅ Data tables created' AS STATUS;

-- ============================================================================
-- STEP 4: CREATE IMAGE REPOSITORY
-- ============================================================================
USE SCHEMA IMAGES;

CREATE IMAGE REPOSITORY IF NOT EXISTS FTFP_REPO COMMENT = 'FTFP container images';

SELECT '✅ Image repository created' AS STATUS;

-- Get and display repository URL for Docker push
SHOW IMAGE REPOSITORIES IN SCHEMA IMAGES;
SELECT '📋 DOCKER IMAGE PATH: ' || "repository_url" || '/ftfp_v1:v1' AS SAVE_THIS_FOR_DOCKER_PUSH
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" = 'FTFP_REPO';

-- ============================================================================
-- STEP 5: CREATE STAGES
-- ============================================================================

-- Seed data stage (CSV files)
USE SCHEMA FTFP;
CREATE STAGE IF NOT EXISTS SEED_STAGE
    FILE_FORMAT = (TYPE = CSV COMPRESSION = GZIP FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    COMMENT = 'Stage for seed data CSV files';

SELECT '✅ Seed data stage created: @FTFP_V1.FTFP.SEED_STAGE' AS STATUS;

-- ML models stage (PKL files)
USE SCHEMA ML;
CREATE STAGE IF NOT EXISTS MODELS
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for XGBoost ML model files';

SELECT '✅ ML models stage created: @FTFP_V1.ML.MODELS' AS STATUS;

-- ============================================================================
-- STEP 6: CREATE COMPUTE POOL
-- ============================================================================
CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($POOL_NAME)
    MIN_NODES = 1 MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 3600
    COMMENT = 'FTFP V1 Compute Pool';

SELECT '✅ Compute pool created: ' || $POOL_NAME AS STATUS;

-- ============================================================================
-- ✅ PHASE 1 COMPLETE
-- ============================================================================
SELECT '============================================================================' AS SEP;
SELECT '🎉 PHASE 1 COMPLETE - Infrastructure Created!' AS STATUS;
SELECT '============================================================================' AS SEP;

SELECT '' AS BLANK;
SELECT '📋 NEXT STEP: Upload seed data AND ML model files to stages' AS INSTRUCTIONS;
SELECT '' AS BLANK;
SELECT '=== UPLOAD SEED DATA (CSV files) ===' AS SECTION1;
SELECT 'snow stage copy seed_data/NORMAL_SEED_FULL.csv.gz @FTFP_V1.FTFP.SEED_STAGE --overwrite --connection YOUR_CONN' AS CMD1;
SELECT 'snow stage copy seed_data/ENGINE_FAILURE_SEED.csv.gz @FTFP_V1.FTFP.SEED_STAGE --overwrite --connection YOUR_CONN' AS CMD2;
SELECT 'snow stage copy seed_data/TRANSMISSION_FAILURE_SEED.csv.gz @FTFP_V1.FTFP.SEED_STAGE --overwrite --connection YOUR_CONN' AS CMD3;
SELECT 'snow stage copy seed_data/ELECTRICAL_FAILURE_SEED.csv.gz @FTFP_V1.FTFP.SEED_STAGE --overwrite --connection YOUR_CONN' AS CMD4;
SELECT '' AS BLANK2;
SELECT '=== UPLOAD ML MODELS (PKL files) ===' AS SECTION2;
SELECT 'snow stage copy seed_data/classifier_v1_0_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONN' AS CMD5;
SELECT 'snow stage copy seed_data/regression_v1_0_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONN' AS CMD6;
SELECT 'snow stage copy seed_data/regression_temporal_v1_1_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONN' AS CMD7;
SELECT 'snow stage copy seed_data/label_mapping_v1_0_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONN' AS CMD8;
SELECT 'snow stage copy seed_data/feature_columns_v1_0_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONN' AS CMD9;
SELECT 'snow stage copy seed_data/feature_columns_temporal_v1_1_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONN' AS CMD10;
SELECT '' AS BLANK3;
SELECT '=== VERIFY UPLOADS ===' AS SECTION3;
SELECT 'LIST @FTFP_V1.FTFP.SEED_STAGE;  -- Should show 4 CSV files' AS VERIFY1;
SELECT 'LIST @FTFP_V1.ML.MODELS;        -- Should show 6 PKL files' AS VERIFY2;
SELECT '' AS BLANK4;
SELECT 'After uploading ALL files, run: 02_LOAD_DATA_AND_DEPLOY.sql' AS NEXT;

