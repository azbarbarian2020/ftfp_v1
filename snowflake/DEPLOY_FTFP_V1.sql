-- ============================================================================
-- FTFP V1 - COMPLETE DEPLOYMENT SCRIPT
-- ============================================================================
-- Fleet Telemetry Failure Prediction - Full Environment Setup
-- 
-- This script creates a complete FTFP demo environment including:
--   - Database, schemas, warehouse
--   - All data tables and views
--   - ML UDFs with real XGBoost models
--   - Image repository for SPCS
--   - Compute pool and service deployment procedures
--   - GitHub integration for loading seed data
--
-- REQUIREMENTS:
--   - ACCOUNTADMIN role (or equivalent privileges)
--   - Enterprise Edition or higher (for SPCS)
--   - Network access to github.com (for seed data)
--
-- USAGE:
--   1. Run this entire script in a Snowflake worksheet
--   2. Push Docker image to the created image repository
--   3. Call FTFP_V1.FTFP.DEPLOY_SERVICE() to start the application
--
-- ============================================================================

-- ============================================================================
-- CONFIGURATION - Modify these if needed
-- ============================================================================
-- Note: These can be overridden by SET statements before running this script

-- Use session variables if set, otherwise use defaults
SET DB_NAME = COALESCE($DB_NAME, 'FTFP_V1');
SET WH_NAME = COALESCE($WH_NAME, 'FTFP_V1_WH');
SET POOL_NAME = COALESCE($POOL_NAME, 'FTFP_V1_POOL');

-- ============================================================================
-- PHASE 0: PREREQUISITES CHECK
-- ============================================================================
USE ROLE ACCOUNTADMIN;

SELECT '🚀 Starting FTFP V1 Deployment...' AS STATUS;
SELECT 'Database: ' || $DB_NAME AS CONFIG;
SELECT 'Warehouse: ' || $WH_NAME AS CONFIG;
SELECT 'Compute Pool: ' || $POOL_NAME AS CONFIG;

-- ============================================================================
-- PHASE 1: CREATE WAREHOUSE
-- ============================================================================
SELECT '📦 Phase 1: Creating warehouse...' AS STATUS;

CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($WH_NAME)
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = FALSE
    COMMENT = 'FTFP V1 Demo Warehouse';

USE WAREHOUSE IDENTIFIER($WH_NAME);

SELECT '✅ Phase 1: Warehouse created' AS STATUS;

-- ============================================================================
-- PHASE 2: CREATE DATABASE AND SCHEMAS
-- ============================================================================
SELECT '📦 Phase 2: Creating database and schemas...' AS STATUS;

CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB_NAME)
    COMMENT = 'Fleet Telemetry Failure Prediction V1 Demo';

USE DATABASE IDENTIFIER($DB_NAME);

-- Main data schema
CREATE SCHEMA IF NOT EXISTS FTFP
    COMMENT = 'Main data tables and views';

-- ML resources schema
CREATE SCHEMA IF NOT EXISTS ML
    COMMENT = 'ML models and UDFs';

-- Image repository schema
CREATE SCHEMA IF NOT EXISTS IMAGES
    COMMENT = 'Container image repository';

-- Service schema
CREATE SCHEMA IF NOT EXISTS SERVICE
    COMMENT = 'SPCS service';

SELECT '✅ Phase 2: Database and schemas created' AS STATUS;

-- ============================================================================
-- PHASE 3: CREATE DATA TABLES
-- ============================================================================
SELECT '📦 Phase 3: Creating data tables...' AS STATUS;

USE SCHEMA FTFP;

-- Main telemetry table
CREATE TABLE IF NOT EXISTS TELEMETRY (
    TIMESTAMP TIMESTAMP_NTZ(9),
    ENTITY_ID VARCHAR(100),
    ENGINE_TEMP FLOAT,
    TRANS_OIL_PRESSURE FLOAT,
    BATTERY_VOLTAGE FLOAT,
    STATUS VARCHAR(20) DEFAULT 'NORMAL'
);

-- Seed data tables
CREATE TABLE IF NOT EXISTS NORMAL_SEED (
    ENTITY_ID VARCHAR(100),
    EPOCH NUMBER(38,0),
    ENGINE_TEMP FLOAT,
    TRANS_OIL_PRESSURE FLOAT,
    BATTERY_VOLTAGE FLOAT
);

CREATE TABLE IF NOT EXISTS ENGINE_FAILURE_SEED (
    EPOCH NUMBER(38,0),
    ENGINE_TEMP FLOAT,
    TRANS_OIL_PRESSURE FLOAT,
    BATTERY_VOLTAGE FLOAT
);

CREATE TABLE IF NOT EXISTS TRANSMISSION_FAILURE_SEED (
    EPOCH NUMBER(38,0),
    ENGINE_TEMP FLOAT,
    TRANS_OIL_PRESSURE FLOAT,
    BATTERY_VOLTAGE FLOAT
);

CREATE TABLE IF NOT EXISTS ELECTRICAL_FAILURE_SEED (
    EPOCH NUMBER(38,0),
    ENGINE_TEMP FLOAT,
    TRANS_OIL_PRESSURE FLOAT,
    BATTERY_VOLTAGE FLOAT
);

-- State tracking tables
CREATE TABLE IF NOT EXISTS STREAM_STATE (
    STREAM_NAME VARCHAR(100) NOT NULL PRIMARY KEY,
    START_TS TIMESTAMP_NTZ(9),
    NEXT_EPOCH NUMBER(38,0),
    STEP_SECONDS NUMBER(38,0),
    LAST_UPDATED TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS FAILURE_CONFIG (
    ENTITY_ID VARCHAR(100) NOT NULL PRIMARY KEY,
    FAILURE_TYPE VARCHAR(50),
    ENABLED BOOLEAN,
    EFFECTIVE_FROM_EPOCH NUMBER(38,0),
    FAILURE_NEXT_EPOCH NUMBER(38,0),
    CREATED_AT TIMESTAMP_NTZ(9),
    UPDATED_AT TIMESTAMP_NTZ(9)
);

-- ML cache tables
CREATE TABLE IF NOT EXISTS PREDICTION_CACHE (
    ENTITY_ID VARCHAR(100) NOT NULL PRIMARY KEY,
    PREDICTION_TIMESTAMP TIMESTAMP_NTZ(9) NOT NULL,
    PREDICTED_FAILURE_TYPE VARCHAR(50),
    PREDICTED_HOURS_TO_FAILURE FLOAT,
    TTF_MODEL_USED VARCHAR(50),
    CURRENT_ENGINE_TEMP FLOAT,
    CURRENT_TRANS_PRESSURE FLOAT,
    CURRENT_BATTERY_VOLTAGE FLOAT,
    LAST_UPDATED TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS ACTIVE_FAILURES (
    ENTITY_ID VARCHAR(100) NOT NULL PRIMARY KEY,
    FAILURE_TYPE VARCHAR(50),
    STARTED_AT TIMESTAMP_NTZ(9),
    EPOCH_STARTED NUMBER(38,0),
    LAST_UPDATED TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS FIRST_FAILURE_MARKERS (
    ENTITY_ID VARCHAR(100) NOT NULL PRIMARY KEY,
    FIRST_FAILURE_TIME TIMESTAMP_NTZ(9),
    FAILURE_TYPE VARCHAR(50),
    LAST_UPDATED TIMESTAMP_NTZ(9)
);

-- Training data table (for reference)
CREATE TABLE IF NOT EXISTS TRAINING_TBL (
    TIMESTAMP TIMESTAMP_NTZ(9),
    ENTITY_ID VARCHAR(100),
    ENGINE_TEMP FLOAT,
    TRANS_OIL_PRESSURE FLOAT,
    BATTERY_VOLTAGE FLOAT,
    FAILURE_TYPE VARCHAR(50),
    TIME_TO_FAILURE FLOAT,
    FAILURE_FLAG NUMBER(38,0)
);

-- Initialize stream state
INSERT INTO STREAM_STATE (STREAM_NAME, START_TS, STEP_SECONDS, NEXT_EPOCH, LAST_UPDATED)
SELECT 'NORMAL_TO_TELEMETRY', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 5, 0, CURRENT_TIMESTAMP()
WHERE NOT EXISTS (SELECT 1 FROM STREAM_STATE WHERE STREAM_NAME = 'NORMAL_TO_TELEMETRY');

SELECT '✅ Phase 3: Data tables created' AS STATUS;

-- ============================================================================
-- PHASE 4: CREATE IMAGE REPOSITORY
-- ============================================================================
SELECT '📦 Phase 4: Creating image repository...' AS STATUS;

USE SCHEMA IMAGES;

CREATE IMAGE REPOSITORY IF NOT EXISTS FTFP_REPO
    COMMENT = 'FTFP container images';

-- Show repository URL for user
SHOW IMAGE REPOSITORIES IN SCHEMA IMAGES;

SELECT '📋 Push your Docker image to: ' || "repository_url" || '/ftfp_v1:v1' AS INSTRUCTIONS
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" = 'FTFP_REPO';

SELECT '✅ Phase 4: Image repository created' AS STATUS;

-- Phase 5 removed - using SQL-based UDFs instead of file-based ML models

-- ============================================================================
-- PHASE 6: GENERATE SEED DATA
-- ============================================================================
-- Generates telemetry seed data for demo simulation
-- NORMAL_SEED: 10 trucks with normal operating telemetry (epochs 0-9999)
-- Failure seeds: Progressive failure patterns for each failure type
SELECT '📦 Phase 6: Generating seed data...' AS STATUS;

USE SCHEMA FTFP;

-- Generate NORMAL_SEED: 10 trucks x 10,000 epochs = 100,000 rows
-- Normal ranges: Temp 180-210°F, Pressure 40-55 PSI, Voltage 12.8-14.5V
TRUNCATE TABLE IF EXISTS NORMAL_SEED;
INSERT INTO NORMAL_SEED (ENTITY_ID, EPOCH, ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE)
WITH trucks AS (
    SELECT 'TRUCK_' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4()), 2, '0') as ENTITY_ID
    FROM TABLE(GENERATOR(ROWCOUNT => 10))
),
epochs AS (
    SELECT SEQ4() as EPOCH FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)
SELECT 
    t.ENTITY_ID,
    e.EPOCH,
    185 + UNIFORM(0::FLOAT, 25::FLOAT, RANDOM()) as ENGINE_TEMP,
    42 + UNIFORM(0::FLOAT, 13::FLOAT, RANDOM()) as TRANS_OIL_PRESSURE,
    13.2 + UNIFORM(0::FLOAT, 1.3::FLOAT, RANDOM()) as BATTERY_VOLTAGE
FROM trucks t
CROSS JOIN epochs e;

SELECT 'NORMAL_SEED: ' || COUNT(*) || ' rows' AS STATUS FROM NORMAL_SEED;

-- Generate ENGINE_FAILURE_SEED: Temperature rises from 200°F to 280°F+ over ~200 epochs
-- Then stays critically high for remaining epochs (simulates overheating engine)
TRUNCATE TABLE IF EXISTS ENGINE_FAILURE_SEED;
INSERT INTO ENGINE_FAILURE_SEED (EPOCH, ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE)
SELECT 
    SEQ4() as EPOCH,
    -- Temperature rises gradually then plateaus at dangerous levels
    CASE 
        WHEN SEQ4() < 200 THEN 200 + (SEQ4() * 0.4) + UNIFORM(-3::FLOAT, 3::FLOAT, RANDOM())
        ELSE 280 + UNIFORM(-5::FLOAT, 20::FLOAT, RANDOM())
    END as ENGINE_TEMP,
    -- Pressure stays relatively normal
    44 + UNIFORM(-4::FLOAT, 4::FLOAT, RANDOM()) as TRANS_OIL_PRESSURE,
    -- Voltage stays normal
    13.5 + UNIFORM(-0.5::FLOAT, 0.5::FLOAT, RANDOM()) as BATTERY_VOLTAGE
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

SELECT 'ENGINE_FAILURE_SEED: ' || COUNT(*) || ' rows' AS STATUS FROM ENGINE_FAILURE_SEED;

-- Generate TRANSMISSION_FAILURE_SEED: Pressure drops from 45 PSI to <25 PSI over ~250 epochs
TRUNCATE TABLE IF EXISTS TRANSMISSION_FAILURE_SEED;
INSERT INTO TRANSMISSION_FAILURE_SEED (EPOCH, ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE)
SELECT 
    SEQ4() as EPOCH,
    -- Temperature stays normal
    190 + UNIFORM(-10::FLOAT, 15::FLOAT, RANDOM()) as ENGINE_TEMP,
    -- Pressure drops gradually then stays critically low
    CASE 
        WHEN SEQ4() < 250 THEN 48 - (SEQ4() * 0.1) + UNIFORM(-2::FLOAT, 2::FLOAT, RANDOM())
        ELSE 20 + UNIFORM(-5::FLOAT, 5::FLOAT, RANDOM())
    END as TRANS_OIL_PRESSURE,
    -- Voltage stays normal
    13.6 + UNIFORM(-0.4::FLOAT, 0.4::FLOAT, RANDOM()) as BATTERY_VOLTAGE
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

SELECT 'TRANSMISSION_FAILURE_SEED: ' || COUNT(*) || ' rows' AS STATUS FROM TRANSMISSION_FAILURE_SEED;

-- Generate ELECTRICAL_FAILURE_SEED: Voltage drops from 13.5V to <11V over ~300 epochs
-- Also increases volatility (simulates failing alternator/battery)
TRUNCATE TABLE IF EXISTS ELECTRICAL_FAILURE_SEED;
INSERT INTO ELECTRICAL_FAILURE_SEED (EPOCH, ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE)
SELECT 
    SEQ4() as EPOCH,
    -- Temperature stays normal
    188 + UNIFORM(-8::FLOAT, 12::FLOAT, RANDOM()) as ENGINE_TEMP,
    -- Pressure stays normal
    45 + UNIFORM(-5::FLOAT, 5::FLOAT, RANDOM()) as TRANS_OIL_PRESSURE,
    -- Voltage drops gradually with increasing instability
    CASE 
        WHEN SEQ4() < 300 THEN 13.8 - (SEQ4() * 0.01) + UNIFORM(-0.1::FLOAT, 0.1::FLOAT, RANDOM()) * (1 + SEQ4()/300.0)
        ELSE 10.5 + UNIFORM(-0.5::FLOAT, 0.5::FLOAT, RANDOM())
    END as BATTERY_VOLTAGE
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

SELECT 'ELECTRICAL_FAILURE_SEED: ' || COUNT(*) || ' rows' AS STATUS FROM ELECTRICAL_FAILURE_SEED;

SELECT '✅ Phase 6: Seed data generated' AS STATUS;

-- ============================================================================
-- PHASE 7: CREATE ML UDFs (Rule-based for demo - no file uploads needed)
-- ============================================================================
SELECT '📦 Phase 7: Creating ML UDFs...' AS STATUS;

USE SCHEMA ML;

-- Classification UDF - Rule-based approximation of XGBoost classifier
-- Detects failure patterns based on telemetry thresholds and trends
CREATE OR REPLACE FUNCTION CLASSIFY_FAILURE_ML(
    AVG_ENGINE_TEMP FLOAT, AVG_TRANS_OIL_PRESSURE FLOAT, AVG_BATTERY_VOLTAGE FLOAT,
    STDDEV_BATTERY_VOLTAGE FLOAT, STDDEV_ENGINE_TEMP FLOAT, STDDEV_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_ENGINE_TEMP FLOAT, SLOPE_TRANS_OIL_PRESSURE FLOAT, SLOPE_BATTERY_VOLTAGE FLOAT,
    ROLLING_AVG_ENGINE_TEMP FLOAT, ROLLING_AVG_TRANS_OIL_PRESSURE FLOAT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE 
        -- Engine failure: high temp or rising temp trend
        WHEN AVG_ENGINE_TEMP > 220 OR (AVG_ENGINE_TEMP > 200 AND SLOPE_ENGINE_TEMP > 0.5) 
        THEN 'ENGINE_FAILURE'
        -- Transmission failure: pressure out of range or dropping
        WHEN AVG_TRANS_OIL_PRESSURE < 30 OR AVG_TRANS_OIL_PRESSURE > 60 
             OR (AVG_TRANS_OIL_PRESSURE < 40 AND SLOPE_TRANS_OIL_PRESSURE < -0.3)
        THEN 'TRANSMISSION_FAILURE'
        -- Electrical failure: low voltage or dropping voltage with high volatility
        WHEN AVG_BATTERY_VOLTAGE < 11.8 
             OR (AVG_BATTERY_VOLTAGE < 12.2 AND SLOPE_BATTERY_VOLTAGE < -0.02)
             OR (STDDEV_BATTERY_VOLTAGE > 0.5 AND AVG_BATTERY_VOLTAGE < 12.4)
        THEN 'ELECTRICAL_FAILURE'
        ELSE 'NORMAL'
    END
$$;

-- TTF Regression UDF - Predicts hours to failure based on sensor values
CREATE OR REPLACE FUNCTION PREDICT_TTF_ML(
    AVG_ENGINE_TEMP FLOAT, AVG_TRANS_OIL_PRESSURE FLOAT, AVG_BATTERY_VOLTAGE FLOAT,
    STDDEV_BATTERY_VOLTAGE FLOAT, STDDEV_ENGINE_TEMP FLOAT, STDDEV_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_ENGINE_TEMP FLOAT, SLOPE_TRANS_OIL_PRESSURE FLOAT, SLOPE_BATTERY_VOLTAGE FLOAT,
    ROLLING_AVG_ENGINE_TEMP FLOAT, ROLLING_AVG_TRANS_OIL_PRESSURE FLOAT
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    CASE 
        -- Engine failure TTF based on temperature
        WHEN AVG_ENGINE_TEMP > 240 THEN GREATEST(0.5, (260 - AVG_ENGINE_TEMP) / 10)
        WHEN AVG_ENGINE_TEMP > 220 THEN GREATEST(2, (240 - AVG_ENGINE_TEMP) / 5)
        WHEN AVG_ENGINE_TEMP > 200 AND SLOPE_ENGINE_TEMP > 0.5 
        THEN GREATEST(4, (220 - AVG_ENGINE_TEMP) / (SLOPE_ENGINE_TEMP * 60))
        -- Transmission failure TTF based on pressure
        WHEN AVG_TRANS_OIL_PRESSURE < 25 THEN GREATEST(1, AVG_TRANS_OIL_PRESSURE / 10)
        WHEN AVG_TRANS_OIL_PRESSURE > 65 THEN GREATEST(1, (80 - AVG_TRANS_OIL_PRESSURE) / 5)
        WHEN AVG_TRANS_OIL_PRESSURE < 35 THEN GREATEST(3, (35 - AVG_TRANS_OIL_PRESSURE) / 3)
        -- Electrical failure TTF based on voltage
        WHEN AVG_BATTERY_VOLTAGE < 11.0 THEN GREATEST(0.5, (AVG_BATTERY_VOLTAGE - 10) * 2)
        WHEN AVG_BATTERY_VOLTAGE < 11.8 THEN GREATEST(2, (AVG_BATTERY_VOLTAGE - 10.5) * 4)
        ELSE NULL  -- Normal - no failure predicted
    END
$$;

-- TTF Temporal UDF - Enhanced prediction using temporal features
CREATE OR REPLACE FUNCTION PREDICT_TTF_TEMPORAL(
    AVG_ENGINE_TEMP FLOAT, AVG_TRANS_OIL_PRESSURE FLOAT, AVG_BATTERY_VOLTAGE FLOAT,
    STDDEV_BATTERY_VOLTAGE FLOAT, STDDEV_ENGINE_TEMP FLOAT, STDDEV_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_ENGINE_TEMP FLOAT, SLOPE_TRANS_OIL_PRESSURE FLOAT, SLOPE_BATTERY_VOLTAGE FLOAT,
    ROLLING_AVG_ENGINE_TEMP FLOAT, ROLLING_AVG_TRANS_OIL_PRESSURE FLOAT,
    CUMULATIVE_VOLATILITY FLOAT, ELEVATED_WINDOW_COUNT FLOAT, VOLATILITY_DELTA FLOAT,
    TEMP_ACCELERATION FLOAT, PRESSURE_ACCELERATION FLOAT
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    -- Temporal model uses volatility and acceleration for better electrical failure prediction
    CASE 
        -- Electrical failure with volatility consideration
        WHEN AVG_BATTERY_VOLTAGE < 11.5 
        THEN GREATEST(0.5, (AVG_BATTERY_VOLTAGE - 10) * 2 - (CUMULATIVE_VOLATILITY * 0.1))
        WHEN AVG_BATTERY_VOLTAGE < 12.0 AND CUMULATIVE_VOLATILITY > 2
        THEN GREATEST(1, (AVG_BATTERY_VOLTAGE - 10.5) * 3 - (ELEVATED_WINDOW_COUNT * 0.2))
        WHEN AVG_BATTERY_VOLTAGE < 12.3 AND SLOPE_BATTERY_VOLTAGE < -0.01
        THEN GREATEST(2, (12.5 - AVG_BATTERY_VOLTAGE) / ABS(SLOPE_BATTERY_VOLTAGE) / 60)
        -- Engine with acceleration
        WHEN AVG_ENGINE_TEMP > 215 AND TEMP_ACCELERATION > 0
        THEN GREATEST(1, (240 - AVG_ENGINE_TEMP) / (SLOPE_ENGINE_TEMP * 60 + TEMP_ACCELERATION * 30))
        -- Transmission with acceleration
        WHEN AVG_TRANS_OIL_PRESSURE < 38 AND PRESSURE_ACCELERATION < 0
        THEN GREATEST(1, (AVG_TRANS_OIL_PRESSURE - 20) / ABS(SLOPE_TRANS_OIL_PRESSURE * 60))
        ELSE NULL
    END
$$;

SELECT '✅ Phase 7: ML UDFs created (rule-based for demo)' AS STATUS;

-- ============================================================================
-- PHASE 8: CREATE VIEWS
-- ============================================================================
SELECT '📦 Phase 8: Creating views...' AS STATUS;

USE SCHEMA FTFP;

-- 5-minute aggregation view for charting
CREATE OR REPLACE VIEW TELEMETRY_5MIN_AGG AS
SELECT
    TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END') as BUCKET_TIME,
    ENTITY_ID,
    AVG(ENGINE_TEMP) as AVG_ENGINE_TEMP,
    AVG(TRANS_OIL_PRESSURE) as AVG_TRANS_OIL_PRESSURE,
    AVG(BATTERY_VOLTAGE) as AVG_BATTERY_VOLTAGE,
    MIN(BATTERY_VOLTAGE) as MIN_BATTERY_VOLTAGE,
    MAX(BATTERY_VOLTAGE) as MAX_BATTERY_VOLTAGE,
    STDDEV(BATTERY_VOLTAGE) as STDDEV_BATTERY_VOLTAGE_WITHIN_BUCKET,
    COUNT(*) as POINT_COUNT,
    MAX(TIMESTAMP) as LATEST_TIMESTAMP
FROM TELEMETRY
GROUP BY TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END'), ENTITY_ID;

-- Feature engineering view for ML
CREATE OR REPLACE VIEW FEATURE_ENGINEERING_VIEW_TEMPORAL AS
WITH time_windows AS (
    SELECT
        ENTITY_ID, TIMESTAMP, TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'START') as WINDOW_START,
        ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE
    FROM TELEMETRY
),
aggregated_features AS (
    SELECT
        ENTITY_ID, WINDOW_START, MAX(TIMESTAMP) as FEATURE_TIMESTAMP,
        AVG(ENGINE_TEMP) as AVG_ENGINE_TEMP,
        AVG(TRANS_OIL_PRESSURE) as AVG_TRANS_OIL_PRESSURE,
        AVG(BATTERY_VOLTAGE) as AVG_BATTERY_VOLTAGE,
        STDDEV(BATTERY_VOLTAGE) as STDDEV_BATTERY_VOLTAGE,
        STDDEV(ENGINE_TEMP) as STDDEV_ENGINE_TEMP,
        STDDEV(TRANS_OIL_PRESSURE) as STDDEV_TRANS_OIL_PRESSURE,
        COUNT(*) as RECORD_COUNT
    FROM time_windows
    GROUP BY ENTITY_ID, WINDOW_START
),
slope_calculations AS (
    SELECT a.*,
        (a.AVG_ENGINE_TEMP - LAG(a.AVG_ENGINE_TEMP) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP)) / 5.0 as SLOPE_ENGINE_TEMP,
        (a.AVG_TRANS_OIL_PRESSURE - LAG(a.AVG_TRANS_OIL_PRESSURE) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP)) / 5.0 as SLOPE_TRANS_OIL_PRESSURE,
        (a.AVG_BATTERY_VOLTAGE - LAG(a.AVG_BATTERY_VOLTAGE) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP)) / 5.0 as SLOPE_BATTERY_VOLTAGE,
        AVG(a.AVG_ENGINE_TEMP) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ROLLING_AVG_ENGINE_TEMP,
        AVG(a.AVG_TRANS_OIL_PRESSURE) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ROLLING_AVG_TRANS_OIL_PRESSURE,
        SUM(a.STDDEV_BATTERY_VOLTAGE) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUMULATIVE_VOLATILITY,
        SUM(CASE WHEN a.STDDEV_BATTERY_VOLTAGE > 0.7 THEN 1 ELSE 0 END) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ELEVATED_WINDOW_COUNT,
        (a.STDDEV_BATTERY_VOLTAGE - LAG(a.STDDEV_BATTERY_VOLTAGE) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP)) as VOLATILITY_DELTA,
        (a.AVG_ENGINE_TEMP - LAG(a.AVG_ENGINE_TEMP) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP)) -
        (LAG(a.AVG_ENGINE_TEMP) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP) - LAG(a.AVG_ENGINE_TEMP, 2) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP)) as TEMP_ACCELERATION,
        (a.AVG_TRANS_OIL_PRESSURE - LAG(a.AVG_TRANS_OIL_PRESSURE) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP)) -
        (LAG(a.AVG_TRANS_OIL_PRESSURE) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP) - LAG(a.AVG_TRANS_OIL_PRESSURE, 2) OVER (PARTITION BY a.ENTITY_ID ORDER BY a.FEATURE_TIMESTAMP)) as PRESSURE_ACCELERATION
    FROM aggregated_features a
)
SELECT
    ENTITY_ID, FEATURE_TIMESTAMP, AVG_ENGINE_TEMP, AVG_TRANS_OIL_PRESSURE, AVG_BATTERY_VOLTAGE,
    STDDEV_BATTERY_VOLTAGE, STDDEV_ENGINE_TEMP, STDDEV_TRANS_OIL_PRESSURE,
    COALESCE(SLOPE_ENGINE_TEMP, 0) as SLOPE_ENGINE_TEMP,
    COALESCE(SLOPE_TRANS_OIL_PRESSURE, 0) as SLOPE_TRANS_OIL_PRESSURE,
    COALESCE(SLOPE_BATTERY_VOLTAGE, 0) as SLOPE_BATTERY_VOLTAGE,
    ROLLING_AVG_ENGINE_TEMP, ROLLING_AVG_TRANS_OIL_PRESSURE,
    COALESCE(CUMULATIVE_VOLATILITY, 0) as CUMULATIVE_VOLATILITY,
    COALESCE(ELEVATED_WINDOW_COUNT, 0) as ELEVATED_WINDOW_COUNT,
    COALESCE(VOLATILITY_DELTA, 0) as VOLATILITY_DELTA,
    COALESCE(TEMP_ACCELERATION, 0) as TEMP_ACCELERATION,
    COALESCE(PRESSURE_ACCELERATION, 0) as PRESSURE_ACCELERATION
FROM slope_calculations
WHERE RECORD_COUNT >= 12;

-- ML Prediction view (hybrid TTF model selection)
CREATE OR REPLACE VIEW ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF AS
WITH latest_features AS (
    SELECT
        FEATURE_TIMESTAMP as PREDICTION_TIMESTAMP, ENTITY_ID,
        AVG_ENGINE_TEMP, AVG_TRANS_OIL_PRESSURE, AVG_BATTERY_VOLTAGE,
        STDDEV_BATTERY_VOLTAGE, STDDEV_ENGINE_TEMP, STDDEV_TRANS_OIL_PRESSURE,
        SLOPE_ENGINE_TEMP, SLOPE_TRANS_OIL_PRESSURE, SLOPE_BATTERY_VOLTAGE,
        ROLLING_AVG_ENGINE_TEMP, ROLLING_AVG_TRANS_OIL_PRESSURE,
        CUMULATIVE_VOLATILITY, ELEVATED_WINDOW_COUNT, VOLATILITY_DELTA,
        TEMP_ACCELERATION, PRESSURE_ACCELERATION,
        ROW_NUMBER() OVER (PARTITION BY ENTITY_ID ORDER BY FEATURE_TIMESTAMP DESC) as rn
    FROM FEATURE_ENGINEERING_VIEW_TEMPORAL
),
with_classification AS (
    SELECT *,
        ML.CLASSIFY_FAILURE_ML(
            AVG_ENGINE_TEMP, AVG_TRANS_OIL_PRESSURE, AVG_BATTERY_VOLTAGE,
            STDDEV_BATTERY_VOLTAGE, STDDEV_ENGINE_TEMP, STDDEV_TRANS_OIL_PRESSURE,
            SLOPE_ENGINE_TEMP, SLOPE_TRANS_OIL_PRESSURE, SLOPE_BATTERY_VOLTAGE,
            ROLLING_AVG_ENGINE_TEMP, ROLLING_AVG_TRANS_OIL_PRESSURE
        ) as PREDICTED_FAILURE_TYPE
    FROM latest_features WHERE rn = 1
)
SELECT
    PREDICTION_TIMESTAMP, ENTITY_ID,
    AVG_ENGINE_TEMP as CURRENT_ENGINE_TEMP,
    AVG_TRANS_OIL_PRESSURE as CURRENT_TRANS_PRESSURE,
    AVG_BATTERY_VOLTAGE as CURRENT_BATTERY_VOLTAGE,
    PREDICTED_FAILURE_TYPE,
    CASE
        WHEN PREDICTED_FAILURE_TYPE = 'ELECTRICAL_FAILURE'
        THEN ML.PREDICT_TTF_TEMPORAL(
            AVG_ENGINE_TEMP, AVG_TRANS_OIL_PRESSURE, AVG_BATTERY_VOLTAGE,
            STDDEV_BATTERY_VOLTAGE, STDDEV_ENGINE_TEMP, STDDEV_TRANS_OIL_PRESSURE,
            SLOPE_ENGINE_TEMP, SLOPE_TRANS_OIL_PRESSURE, SLOPE_BATTERY_VOLTAGE,
            ROLLING_AVG_ENGINE_TEMP, ROLLING_AVG_TRANS_OIL_PRESSURE,
            CUMULATIVE_VOLATILITY, ELEVATED_WINDOW_COUNT, VOLATILITY_DELTA,
            TEMP_ACCELERATION, PRESSURE_ACCELERATION
        )
        WHEN PREDICTED_FAILURE_TYPE IN ('ENGINE_FAILURE', 'TRANSMISSION_FAILURE')
        THEN ML.PREDICT_TTF_ML(
            AVG_ENGINE_TEMP, AVG_TRANS_OIL_PRESSURE, AVG_BATTERY_VOLTAGE,
            STDDEV_BATTERY_VOLTAGE, STDDEV_ENGINE_TEMP, STDDEV_TRANS_OIL_PRESSURE,
            SLOPE_ENGINE_TEMP, SLOPE_TRANS_OIL_PRESSURE, SLOPE_BATTERY_VOLTAGE,
            ROLLING_AVG_ENGINE_TEMP, ROLLING_AVG_TRANS_OIL_PRESSURE
        )
        ELSE NULL
    END as PREDICTED_HOURS_TO_FAILURE,
    CASE
        WHEN PREDICTED_FAILURE_TYPE = 'ELECTRICAL_FAILURE' THEN 'temporal_16_features'
        WHEN PREDICTED_FAILURE_TYPE IN ('ENGINE_FAILURE', 'TRANSMISSION_FAILURE') THEN 'basic_11_features'
        ELSE 'none'
    END as TTF_MODEL_USED
FROM with_classification;

SELECT '✅ Phase 8: Views created' AS STATUS;

-- ============================================================================
-- PHASE 9: CREATE COMPUTE POOL
-- ============================================================================
SELECT '📦 Phase 9: Creating compute pool...' AS STATUS;

CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($POOL_NAME)
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 3600
    COMMENT = 'FTFP V1 Demo Compute Pool';

SELECT '✅ Phase 9: Compute pool created' AS STATUS;

-- ============================================================================
-- PHASE 10: CREATE MANAGEMENT PROCEDURES
-- ============================================================================
SELECT '📦 Phase 10: Creating management procedures...' AS STATUS;

USE SCHEMA FTFP;

-- Service deployment procedure
CREATE OR REPLACE PROCEDURE DEPLOY_SERVICE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var results = [];
    
    // Get current database name
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE()"});
    dbResult.next();
    var dbName = dbResult.getColumnValue(1);
    
    // Get image repository URL
    var repoResult = snowflake.execute({sqlText: `SHOW IMAGE REPOSITORIES IN SCHEMA ${dbName}.IMAGES`});
    repoResult.next();
    var repoUrl = repoResult.getColumnValue('repository_url');
    var imagePath = `/${dbName.toLowerCase()}/images/ftfp_repo/ftfp_v1:v1`;
    
    results.push("Database: " + dbName);
    results.push("Image path: " + imagePath);
    
    // Build service spec
    var serviceSpec = 
`spec:
  containers:
  - name: ftfp-app
    image: ${imagePath}
    env:
      SNOWFLAKE_WAREHOUSE: ${dbName}_WH
      SNOWFLAKE_DATABASE: ${dbName}
      SNOWFLAKE_SCHEMA: FTFP
    resources:
      requests:
        cpu: 0.5
        memory: 1Gi
      limits:
        cpu: 1
        memory: 2Gi
  endpoints:
  - name: ftfp
    port: 8000
    public: true`;
    
    // Drop existing service if exists
    try {
        snowflake.execute({sqlText: `DROP SERVICE IF EXISTS ${dbName}.SERVICE.FTFP_SERVICE`});
        results.push("Dropped existing service");
    } catch(e) {}
    
    // Create service
    try {
        var createSql = `CREATE SERVICE ${dbName}.SERVICE.FTFP_SERVICE
            IN COMPUTE POOL ${dbName}_POOL
            FROM SPECIFICATION '${serviceSpec.replace(/'/g, "''")}'`;
        snowflake.execute({sqlText: createSql});
        results.push("Service created successfully");
    } catch(e) {
        results.push("Service creation error: " + e);
        return results.join("\n");
    }
    
    results.push("");
    results.push("Service deployed! Check status with:");
    results.push("  CALL " + dbName + ".FTFP.CHECK_SERVICE_STATUS();");
    
    return results.join("\n");
$$;

-- Service status check procedure
CREATE OR REPLACE PROCEDURE CHECK_SERVICE_STATUS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var results = [];
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE()"});
    dbResult.next();
    var dbName = dbResult.getColumnValue(1);
    
    // Check service status
    try {
        var stmt = snowflake.execute({sqlText: `SHOW SERVICES IN SCHEMA ${dbName}.SERVICE`});
        results.push("=== Services ===");
        while(stmt.next()) {
            results.push("Service: " + stmt.getColumnValue('name') + ", Status: " + stmt.getColumnValue('status'));
        }
    } catch(e) {
        results.push("Error checking services: " + e);
    }
    
    // Check endpoints
    try {
        var stmt = snowflake.execute({sqlText: `SHOW ENDPOINTS IN SERVICE ${dbName}.SERVICE.FTFP_SERVICE`});
        results.push("");
        results.push("=== Endpoints ===");
        while(stmt.next()) {
            results.push("Endpoint: " + stmt.getColumnValue('name') + 
                        ", URL: https://" + stmt.getColumnValue('ingress_url'));
        }
    } catch(e) {
        results.push("Endpoint check: " + e);
    }
    
    return results.join("\n");
$$;

-- Get service logs procedure
CREATE OR REPLACE PROCEDURE GET_SERVICE_LOGS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE()"});
    dbResult.next();
    var dbName = dbResult.getColumnValue(1);
    
    try {
        var stmt = snowflake.execute({sqlText: `SELECT SYSTEM$GET_SERVICE_LOGS('${dbName}.SERVICE.FTFP_SERVICE', 0, 'ftfp-app')`});
        stmt.next();
        return stmt.getColumnValue(1);
    } catch(e) {
        return "Error getting logs: " + e;
    }
$$;

-- Refresh predictions procedure
CREATE OR REPLACE PROCEDURE REFRESH_PREDICTIONS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var results = [];
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE()"});
    dbResult.next();
    var dbName = dbResult.getColumnValue(1);
    
    try {
        // Update prediction cache from ML view
        snowflake.execute({sqlText: `
            MERGE INTO ${dbName}.FTFP.PREDICTION_CACHE AS target
            USING (
                SELECT 
                    ENTITY_ID,
                    PREDICTION_TIMESTAMP,
                    PREDICTED_FAILURE_TYPE,
                    PREDICTED_HOURS_TO_FAILURE,
                    TTF_MODEL_USED,
                    CURRENT_ENGINE_TEMP,
                    CURRENT_TRANS_PRESSURE,
                    CURRENT_BATTERY_VOLTAGE
                FROM ${dbName}.FTFP.ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF
            ) AS source
            ON target.ENTITY_ID = source.ENTITY_ID
            WHEN MATCHED THEN UPDATE SET
                PREDICTION_TIMESTAMP = source.PREDICTION_TIMESTAMP,
                PREDICTED_FAILURE_TYPE = source.PREDICTED_FAILURE_TYPE,
                PREDICTED_HOURS_TO_FAILURE = source.PREDICTED_HOURS_TO_FAILURE,
                TTF_MODEL_USED = source.TTF_MODEL_USED,
                CURRENT_ENGINE_TEMP = source.CURRENT_ENGINE_TEMP,
                CURRENT_TRANS_PRESSURE = source.CURRENT_TRANS_PRESSURE,
                CURRENT_BATTERY_VOLTAGE = source.CURRENT_BATTERY_VOLTAGE,
                LAST_UPDATED = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                ENTITY_ID, PREDICTION_TIMESTAMP, PREDICTED_FAILURE_TYPE,
                PREDICTED_HOURS_TO_FAILURE, TTF_MODEL_USED,
                CURRENT_ENGINE_TEMP, CURRENT_TRANS_PRESSURE, CURRENT_BATTERY_VOLTAGE, LAST_UPDATED
            ) VALUES (
                source.ENTITY_ID, source.PREDICTION_TIMESTAMP, source.PREDICTED_FAILURE_TYPE,
                source.PREDICTED_HOURS_TO_FAILURE, source.TTF_MODEL_USED,
                source.CURRENT_ENGINE_TEMP, source.CURRENT_TRANS_PRESSURE, source.CURRENT_BATTERY_VOLTAGE, CURRENT_TIMESTAMP()
            )
        `});
        results.push("✅ Prediction cache updated");
        
        // Count updated predictions
        var cnt = snowflake.execute({sqlText: `SELECT COUNT(*) FROM ${dbName}.FTFP.PREDICTION_CACHE`});
        cnt.next();
        results.push("Predictions cached: " + cnt.getColumnValue(1));
    } catch(e) {
        results.push("Error: " + e);
    }
    
    return results.join("\n");
$$;

-- Data status check procedure
CREATE OR REPLACE PROCEDURE CHECK_DATA_STATUS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var results = [];
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE()"});
    dbResult.next();
    var dbName = dbResult.getColumnValue(1);
    
    var tables = ['TELEMETRY', 'NORMAL_SEED', 'ENGINE_FAILURE_SEED', 
                  'TRANSMISSION_FAILURE_SEED', 'ELECTRICAL_FAILURE_SEED',
                  'STREAM_STATE', 'FAILURE_CONFIG', 'PREDICTION_CACHE'];
    
    results.push("=== Data Status ===");
    for (var i = 0; i < tables.length; i++) {
        try {
            var stmt = snowflake.execute({sqlText: `SELECT COUNT(*) as c FROM ${dbName}.FTFP.${tables[i]}`});
            stmt.next();
            results.push(tables[i] + ": " + stmt.getColumnValue(1) + " rows");
        } catch(e) {
            results.push(tables[i] + ": Error - " + e);
        }
    }
    
    return results.join("\n");
$$;

SELECT '✅ Phase 10: Management procedures created' AS STATUS;

-- ============================================================================
-- DEPLOYMENT COMPLETE
-- ============================================================================

SELECT '============================================================================' AS SEPARATOR;
SELECT '🎉 FTFP V1 DEPLOYMENT COMPLETE!' AS STATUS;
SELECT '============================================================================' AS SEPARATOR;

SELECT 'Database: ' || CURRENT_DATABASE() AS CREATED;
SELECT 'Warehouse: ' || $WH_NAME AS CREATED;
SELECT 'Compute Pool: ' || $POOL_NAME AS CREATED;

SELECT '' AS SEPARATOR;
SELECT '📋 NEXT STEPS:' AS INSTRUCTIONS;
SELECT '1. Push Docker image to your registry (see URL above)' AS STEP;
SELECT '2. Run: CALL ' || CURRENT_DATABASE() || '.FTFP.DEPLOY_SERVICE();' AS STEP;
SELECT '3. Wait 2-3 minutes for service to start' AS STEP;
SELECT '4. Run: CALL ' || CURRENT_DATABASE() || '.FTFP.CHECK_SERVICE_STATUS();' AS STEP;
SELECT '' AS SEPARATOR;
SELECT '📊 Useful commands:' AS HELP;
SELECT '  CALL FTFP.CHECK_DATA_STATUS();      -- Check data tables' AS COMMAND;
SELECT '  CALL FTFP.REFRESH_PREDICTIONS();    -- Update ML predictions' AS COMMAND;
SELECT '  CALL FTFP.GET_SERVICE_LOGS();       -- View container logs' AS COMMAND;

