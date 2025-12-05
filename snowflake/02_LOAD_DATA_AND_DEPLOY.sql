-- ============================================================================
-- FTFP V1 - PHASE 2: LOAD DATA AND DEPLOY
-- ============================================================================
-- Loads seed data from stage, creates ML UDFs, views, and service procedures
-- 
-- PREREQUISITES:
--   1. Run 01_INFRASTRUCTURE.sql first
--   2. Upload seed data CSV files to @FTFP_V1.FTFP.SEED_STAGE
--
-- AFTER running this script:
--   1. Push Docker image to your Snowflake registry
--   2. Call FTFP_V1.FTFP.DEPLOY_SERVICE() to start the application
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE FTFP_V1;
USE WAREHOUSE FTFP_V1_WH;

SELECT '🚀 FTFP V1 - Phase 2: Load Data and Deploy' AS STATUS;

-- ============================================================================
-- STEP 1: VERIFY STAGE FILES
-- ============================================================================
USE SCHEMA FTFP;

SELECT '📦 Checking seed data files in stage...' AS STATUS;
LIST @SEED_STAGE;

-- ============================================================================
-- STEP 2: LOAD SEED DATA
-- ============================================================================
SELECT '📦 Loading seed data...' AS STATUS;

-- Load NORMAL_SEED (1.2M rows)
TRUNCATE TABLE IF EXISTS NORMAL_SEED;
COPY INTO NORMAL_SEED (ENTITY_ID, EPOCH, ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE)
FROM @SEED_STAGE/NORMAL_SEED_FULL.csv.gz
ON_ERROR = CONTINUE;

SELECT 'NORMAL_SEED: ' || COUNT(*) || ' rows loaded' AS STATUS FROM NORMAL_SEED;

-- Load ENGINE_FAILURE_SEED
TRUNCATE TABLE IF EXISTS ENGINE_FAILURE_SEED;
COPY INTO ENGINE_FAILURE_SEED (EPOCH, ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE)
FROM @SEED_STAGE/ENGINE_FAILURE_SEED.csv.gz
ON_ERROR = CONTINUE;

SELECT 'ENGINE_FAILURE_SEED: ' || COUNT(*) || ' rows loaded' AS STATUS FROM ENGINE_FAILURE_SEED;

-- Load TRANSMISSION_FAILURE_SEED
TRUNCATE TABLE IF EXISTS TRANSMISSION_FAILURE_SEED;
COPY INTO TRANSMISSION_FAILURE_SEED (EPOCH, ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE)
FROM @SEED_STAGE/TRANSMISSION_FAILURE_SEED.csv.gz
ON_ERROR = CONTINUE;

SELECT 'TRANSMISSION_FAILURE_SEED: ' || COUNT(*) || ' rows loaded' AS STATUS FROM TRANSMISSION_FAILURE_SEED;

-- Load ELECTRICAL_FAILURE_SEED
TRUNCATE TABLE IF EXISTS ELECTRICAL_FAILURE_SEED;
COPY INTO ELECTRICAL_FAILURE_SEED (EPOCH, ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE)
FROM @SEED_STAGE/ELECTRICAL_FAILURE_SEED.csv.gz
ON_ERROR = CONTINUE;

SELECT 'ELECTRICAL_FAILURE_SEED: ' || COUNT(*) || ' rows loaded' AS STATUS FROM ELECTRICAL_FAILURE_SEED;

SELECT '✅ Seed data loaded' AS STATUS;

-- ============================================================================
-- STEP 3: UPLOAD ML MODELS AND CREATE UDFs
-- ============================================================================
SELECT '📦 Creating ML model stage and UDFs...' AS STATUS;

USE SCHEMA ML;

-- Create stage for ML models
CREATE STAGE IF NOT EXISTS MODELS DIRECTORY = (ENABLE = TRUE);

SELECT '⚠️ Upload ML model files to @FTFP_V1.ML.MODELS before continuing' AS NOTE;
SELECT 'Run: snow stage copy seed_data/<file>.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONNECTION' AS COMMAND;
SELECT 'Files: classifier_v1_0_0.pkl.gz, regression_v1_0_0.pkl.gz, regression_temporal_v1_1_0.pkl.gz,' AS FILES;
SELECT '       label_mapping_v1_0_0.pkl.gz, feature_columns_v1_0_0.pkl.gz, feature_columns_temporal_v1_1_0.pkl.gz' AS FILES2;

-- Verify models are uploaded
LIST @MODELS;

-- Classification UDF - XGBoost classifier model
CREATE OR REPLACE FUNCTION CLASSIFY_FAILURE_ML(
    AVG_ENGINE_TEMP FLOAT, AVG_TRANS_OIL_PRESSURE FLOAT, AVG_BATTERY_VOLTAGE FLOAT,
    STDDEV_BATTERY_VOLTAGE FLOAT, STDDEV_ENGINE_TEMP FLOAT, STDDEV_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_ENGINE_TEMP FLOAT, SLOPE_TRANS_OIL_PRESSURE FLOAT, SLOPE_BATTERY_VOLTAGE FLOAT,
    ROLLING_AVG_ENGINE_TEMP FLOAT, ROLLING_AVG_TRANS_OIL_PRESSURE FLOAT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('xgboost','numpy','pandas','scikit-learn','joblib')
HANDLER = 'classify'
IMPORTS = ('@FTFP_V1.ML.MODELS/classifier_v1_0_0.pkl.gz',
           '@FTFP_V1.ML.MODELS/label_mapping_v1_0_0.pkl.gz',
           '@FTFP_V1.ML.MODELS/feature_columns_v1_0_0.pkl.gz')
AS $$
import sys, joblib, numpy as np
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
clf_model = joblib.load(import_dir + "classifier_v1_0_0.pkl.gz")
label_info = joblib.load(import_dir + "label_mapping_v1_0_0.pkl.gz")
reverse_label_mapping = label_info["reverse_mapping"]
def classify(avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage, stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure, slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage, rolling_avg_engine_temp, rolling_avg_trans_oil_pressure):
    features = np.array([[avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage, stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure, slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage, rolling_avg_engine_temp, rolling_avg_trans_oil_pressure]])
    if np.isnan(features).any(): return "NORMAL"
    prediction = clf_model.predict(features)[0]
    return reverse_label_mapping[int(prediction)]
$$;

SELECT '✅ CLASSIFY_FAILURE_ML created (XGBoost)' AS STATUS;

-- TTF Regression UDF - XGBoost regression model (11 features)
CREATE OR REPLACE FUNCTION PREDICT_TTF_ML(
    AVG_ENGINE_TEMP FLOAT, AVG_TRANS_OIL_PRESSURE FLOAT, AVG_BATTERY_VOLTAGE FLOAT,
    STDDEV_BATTERY_VOLTAGE FLOAT, STDDEV_ENGINE_TEMP FLOAT, STDDEV_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_ENGINE_TEMP FLOAT, SLOPE_TRANS_OIL_PRESSURE FLOAT, SLOPE_BATTERY_VOLTAGE FLOAT,
    ROLLING_AVG_ENGINE_TEMP FLOAT, ROLLING_AVG_TRANS_OIL_PRESSURE FLOAT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('xgboost','numpy','pandas','scikit-learn','joblib')
HANDLER = 'predict_ttf'
IMPORTS = ('@FTFP_V1.ML.MODELS/regression_v1_0_0.pkl.gz',
           '@FTFP_V1.ML.MODELS/feature_columns_v1_0_0.pkl.gz')
AS $$
import sys, joblib, numpy as np
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
reg_model = joblib.load(import_dir + "regression_v1_0_0.pkl.gz")
def predict_ttf(avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage, stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure, slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage, rolling_avg_engine_temp, rolling_avg_trans_oil_pressure):
    features = np.array([[avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage, stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure, slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage, rolling_avg_engine_temp, rolling_avg_trans_oil_pressure]])
    if np.isnan(features).any(): return None
    prediction = reg_model.predict(features)[0]
    return max(0.0, float(prediction))
$$;

SELECT '✅ PREDICT_TTF_ML created (XGBoost)' AS STATUS;

-- TTF Temporal UDF - XGBoost regression model (16 temporal features)
CREATE OR REPLACE FUNCTION PREDICT_TTF_TEMPORAL(
    AVG_ENGINE_TEMP FLOAT, AVG_TRANS_OIL_PRESSURE FLOAT, AVG_BATTERY_VOLTAGE FLOAT,
    STDDEV_BATTERY_VOLTAGE FLOAT, STDDEV_ENGINE_TEMP FLOAT, STDDEV_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_ENGINE_TEMP FLOAT, SLOPE_TRANS_OIL_PRESSURE FLOAT, SLOPE_BATTERY_VOLTAGE FLOAT,
    ROLLING_AVG_ENGINE_TEMP FLOAT, ROLLING_AVG_TRANS_OIL_PRESSURE FLOAT,
    CUMULATIVE_VOLATILITY FLOAT, ELEVATED_WINDOW_COUNT FLOAT, VOLATILITY_DELTA FLOAT,
    TEMP_ACCELERATION FLOAT, PRESSURE_ACCELERATION FLOAT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('xgboost','numpy','pandas','scikit-learn','joblib')
HANDLER = 'predict_ttf_temporal'
IMPORTS = ('@FTFP_V1.ML.MODELS/regression_temporal_v1_1_0.pkl.gz',
           '@FTFP_V1.ML.MODELS/feature_columns_temporal_v1_1_0.pkl.gz')
AS $$
import sys, joblib, numpy as np
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
reg_model = joblib.load(import_dir + "regression_temporal_v1_1_0.pkl.gz")
def predict_ttf_temporal(avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage, stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure, slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage, rolling_avg_engine_temp, rolling_avg_trans_oil_pressure, cumulative_volatility, elevated_window_count, volatility_delta, temp_acceleration, pressure_acceleration):
    features = np.array([[avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage, stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure, slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage, rolling_avg_engine_temp, rolling_avg_trans_oil_pressure, cumulative_volatility, elevated_window_count, volatility_delta, temp_acceleration, pressure_acceleration]])
    if np.isnan(features).any(): return None
    prediction = reg_model.predict(features)[0]
    return max(0.0, float(prediction))
$$;

SELECT '✅ PREDICT_TTF_TEMPORAL created (XGBoost)' AS STATUS;
SELECT '✅ All ML UDFs created with real XGBoost models' AS STATUS;

-- ============================================================================
-- STEP 4: CREATE VIEWS
-- ============================================================================
SELECT '📦 Creating views...' AS STATUS;

USE SCHEMA FTFP;

-- 5-minute aggregation view
CREATE OR REPLACE VIEW TELEMETRY_5MIN_AGG AS
SELECT
    TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END') as BUCKET_TIME,
    ENTITY_ID,
    AVG(ENGINE_TEMP) as AVG_ENGINE_TEMP,
    AVG(TRANS_OIL_PRESSURE) as AVG_TRANS_OIL_PRESSURE,
    AVG(BATTERY_VOLTAGE) as AVG_BATTERY_VOLTAGE,
    COUNT(*) as POINT_COUNT,
    MAX(TIMESTAMP) as LATEST_TIMESTAMP
FROM TELEMETRY
GROUP BY TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END'), ENTITY_ID;

-- Feature engineering view
CREATE OR REPLACE VIEW FEATURE_ENGINEERING_VIEW_TEMPORAL AS
WITH time_windows AS (
    SELECT ENTITY_ID, TIMESTAMP, TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'START') as WINDOW_START,
        ENGINE_TEMP, TRANS_OIL_PRESSURE, BATTERY_VOLTAGE
    FROM TELEMETRY
),
aggregated_features AS (
    SELECT ENTITY_ID, WINDOW_START, MAX(TIMESTAMP) as FEATURE_TIMESTAMP,
        AVG(ENGINE_TEMP) as AVG_ENGINE_TEMP, AVG(TRANS_OIL_PRESSURE) as AVG_TRANS_OIL_PRESSURE,
        AVG(BATTERY_VOLTAGE) as AVG_BATTERY_VOLTAGE, STDDEV(BATTERY_VOLTAGE) as STDDEV_BATTERY_VOLTAGE,
        STDDEV(ENGINE_TEMP) as STDDEV_ENGINE_TEMP, STDDEV(TRANS_OIL_PRESSURE) as STDDEV_TRANS_OIL_PRESSURE,
        COUNT(*) as RECORD_COUNT
    FROM time_windows GROUP BY ENTITY_ID, WINDOW_START
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
SELECT ENTITY_ID, FEATURE_TIMESTAMP, AVG_ENGINE_TEMP, AVG_TRANS_OIL_PRESSURE, AVG_BATTERY_VOLTAGE,
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
FROM slope_calculations WHERE RECORD_COUNT >= 12;

-- ML Prediction view
CREATE OR REPLACE VIEW ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF AS
WITH latest_features AS (
    SELECT FEATURE_TIMESTAMP as PREDICTION_TIMESTAMP, ENTITY_ID,
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
SELECT PREDICTION_TIMESTAMP, ENTITY_ID,
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
            TEMP_ACCELERATION, PRESSURE_ACCELERATION)
        WHEN PREDICTED_FAILURE_TYPE IN ('ENGINE_FAILURE', 'TRANSMISSION_FAILURE')
        THEN ML.PREDICT_TTF_ML(
            AVG_ENGINE_TEMP, AVG_TRANS_OIL_PRESSURE, AVG_BATTERY_VOLTAGE,
            STDDEV_BATTERY_VOLTAGE, STDDEV_ENGINE_TEMP, STDDEV_TRANS_OIL_PRESSURE,
            SLOPE_ENGINE_TEMP, SLOPE_TRANS_OIL_PRESSURE, SLOPE_BATTERY_VOLTAGE,
            ROLLING_AVG_ENGINE_TEMP, ROLLING_AVG_TRANS_OIL_PRESSURE)
        ELSE NULL
    END as PREDICTED_HOURS_TO_FAILURE,
    CASE
        WHEN PREDICTED_FAILURE_TYPE = 'ELECTRICAL_FAILURE' THEN 'temporal_16_features'
        WHEN PREDICTED_FAILURE_TYPE IN ('ENGINE_FAILURE', 'TRANSMISSION_FAILURE') THEN 'basic_11_features'
        ELSE 'none'
    END as TTF_MODEL_USED
FROM with_classification;

SELECT '✅ Views created' AS STATUS;

-- ============================================================================
-- STEP 5: CREATE SERVICE MANAGEMENT PROCEDURES
-- ============================================================================
SELECT '📦 Creating management procedures...' AS STATUS;

-- Service deployment procedure
CREATE OR REPLACE PROCEDURE DEPLOY_SERVICE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var results = [];
    var dbName = 'FTFP_V1';
    var imagePath = '/' + dbName.toLowerCase() + '/images/ftfp_repo/ftfp_v1:v1';
    
    results.push("Database: " + dbName);
    results.push("Image path: " + imagePath);
    
    var serviceSpec = 
'spec:\n' +
'  containers:\n' +
'  - name: ftfp-app\n' +
'    image: ' + imagePath + '\n' +
'    env:\n' +
'      SNOWFLAKE_WAREHOUSE: FTFP_V1_WH\n' +
'      SNOWFLAKE_DATABASE: ' + dbName + '\n' +
'      SNOWFLAKE_SCHEMA: FTFP\n' +
'    resources:\n' +
'      requests:\n' +
'        cpu: 0.5\n' +
'        memory: 1Gi\n' +
'      limits:\n' +
'        cpu: 1\n' +
'        memory: 2Gi\n' +
'  endpoints:\n' +
'  - name: ftfp\n' +
'    port: 8000\n' +
'    public: true';
    
    try {
        snowflake.execute({sqlText: 'DROP SERVICE IF EXISTS ' + dbName + '.SERVICE.FTFP_SERVICE'});
        results.push("Dropped existing service");
    } catch(e) {}
    
    try {
        var createSql = "CREATE SERVICE " + dbName + ".SERVICE.FTFP_SERVICE " +
            "IN COMPUTE POOL FTFP_V1_POOL " +
            "FROM SPECIFICATION '" + serviceSpec.replace(/'/g, "''") + "'";
        snowflake.execute({sqlText: createSql});
        results.push("Service created successfully");
    } catch(e) {
        results.push("Service creation error: " + e);
        return results.join("\n");
    }
    
    results.push("");
    results.push("Service deployed! Check status with:");
    results.push("  CALL FTFP_V1.FTFP.CHECK_SERVICE_STATUS();");
    
    return results.join("\n");
$$;

-- Service status check
CREATE OR REPLACE PROCEDURE CHECK_SERVICE_STATUS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var results = [];
    var dbName = 'FTFP_V1';
    
    try {
        var stmt = snowflake.execute({sqlText: 'SHOW SERVICES IN SCHEMA ' + dbName + '.SERVICE'});
        results.push("=== Services ===");
        while(stmt.next()) {
            results.push("Service: " + stmt.getColumnValue('name') + ", Status: " + stmt.getColumnValue('status'));
        }
    } catch(e) {
        results.push("Error: " + e);
    }
    
    try {
        var stmt = snowflake.execute({sqlText: 'SHOW ENDPOINTS IN SERVICE ' + dbName + '.SERVICE.FTFP_SERVICE'});
        results.push("");
        results.push("=== Endpoints ===");
        while(stmt.next()) {
            results.push("URL: https://" + stmt.getColumnValue('ingress_url'));
        }
    } catch(e) {
        results.push("Endpoint: " + e);
    }
    
    return results.join("\n");
$$;

-- Service logs
CREATE OR REPLACE PROCEDURE GET_SERVICE_LOGS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        var stmt = snowflake.execute({sqlText: "SELECT SYSTEM$GET_SERVICE_LOGS('FTFP_V1.SERVICE.FTFP_SERVICE', 0, 'ftfp-app')"});
        stmt.next();
        return stmt.getColumnValue(1);
    } catch(e) {
        return "Error: " + e;
    }
$$;

-- Refresh predictions
CREATE OR REPLACE PROCEDURE REFRESH_PREDICTIONS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var dbName = 'FTFP_V1';
    try {
        snowflake.execute({sqlText: 
            "MERGE INTO " + dbName + ".FTFP.PREDICTION_CACHE AS target " +
            "USING (SELECT ENTITY_ID, PREDICTION_TIMESTAMP, PREDICTED_FAILURE_TYPE, " +
            "PREDICTED_HOURS_TO_FAILURE, TTF_MODEL_USED, CURRENT_ENGINE_TEMP, " +
            "CURRENT_TRANS_PRESSURE, CURRENT_BATTERY_VOLTAGE " +
            "FROM " + dbName + ".FTFP.ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF) AS source " +
            "ON target.ENTITY_ID = source.ENTITY_ID " +
            "WHEN MATCHED THEN UPDATE SET " +
            "PREDICTION_TIMESTAMP = source.PREDICTION_TIMESTAMP, " +
            "PREDICTED_FAILURE_TYPE = source.PREDICTED_FAILURE_TYPE, " +
            "PREDICTED_HOURS_TO_FAILURE = source.PREDICTED_HOURS_TO_FAILURE, " +
            "TTF_MODEL_USED = source.TTF_MODEL_USED, " +
            "CURRENT_ENGINE_TEMP = source.CURRENT_ENGINE_TEMP, " +
            "CURRENT_TRANS_PRESSURE = source.CURRENT_TRANS_PRESSURE, " +
            "CURRENT_BATTERY_VOLTAGE = source.CURRENT_BATTERY_VOLTAGE, " +
            "LAST_UPDATED = CURRENT_TIMESTAMP() " +
            "WHEN NOT MATCHED THEN INSERT VALUES (" +
            "source.ENTITY_ID, source.PREDICTION_TIMESTAMP, source.PREDICTED_FAILURE_TYPE, " +
            "source.PREDICTED_HOURS_TO_FAILURE, source.TTF_MODEL_USED, " +
            "source.CURRENT_ENGINE_TEMP, source.CURRENT_TRANS_PRESSURE, " +
            "source.CURRENT_BATTERY_VOLTAGE, CURRENT_TIMESTAMP())"
        });
        var cnt = snowflake.execute({sqlText: "SELECT COUNT(*) FROM " + dbName + ".FTFP.PREDICTION_CACHE"});
        cnt.next();
        return "✅ Predictions refreshed: " + cnt.getColumnValue(1) + " rows";
    } catch(e) {
        return "Error: " + e;
    }
$$;

SELECT '✅ Management procedures created' AS STATUS;

-- ============================================================================
-- ✅ PHASE 2 COMPLETE
-- ============================================================================
SELECT '============================================================================' AS SEP;
SELECT '🎉 PHASE 2 COMPLETE - Data Loaded and Ready!' AS STATUS;
SELECT '============================================================================' AS SEP;

-- Show data counts
SELECT 'NORMAL_SEED: ' || COUNT(*) || ' rows' AS DATA FROM FTFP.NORMAL_SEED
UNION ALL SELECT 'ENGINE_FAILURE_SEED: ' || COUNT(*) FROM FTFP.ENGINE_FAILURE_SEED
UNION ALL SELECT 'TRANSMISSION_FAILURE_SEED: ' || COUNT(*) FROM FTFP.TRANSMISSION_FAILURE_SEED
UNION ALL SELECT 'ELECTRICAL_FAILURE_SEED: ' || COUNT(*) FROM FTFP.ELECTRICAL_FAILURE_SEED;

SELECT '' AS BLANK;
SELECT '📋 NEXT STEP: Push Docker image and deploy service' AS INSTRUCTIONS;
SELECT '' AS BLANK;
SELECT '1. Get your image repository URL:' AS STEP1;
SELECT '   SHOW IMAGE REPOSITORIES IN SCHEMA FTFP_V1.IMAGES;' AS CMD1;
SELECT '' AS BLANK;
SELECT '2. Push Docker image (in terminal):' AS STEP2;
SELECT '   docker pull ghcr.io/azbarbarian2020/ftfp_v1:v1' AS CMD2A;
SELECT '   docker tag ghcr.io/azbarbarian2020/ftfp_v1:v1 YOUR_REPO_URL/ftfp_v1:v1' AS CMD2B;
SELECT '   docker login YOUR_REPO_URL' AS CMD2C;
SELECT '   docker push YOUR_REPO_URL/ftfp_v1:v1' AS CMD2D;
SELECT '' AS BLANK;
SELECT '3. Deploy service (back in Snowflake):' AS STEP3;
SELECT '   CALL FTFP_V1.FTFP.DEPLOY_SERVICE();' AS CMD3;
SELECT '' AS BLANK;
SELECT '4. Check status (wait 2-3 min):' AS STEP4;
SELECT '   CALL FTFP_V1.FTFP.CHECK_SERVICE_STATUS();' AS CMD4;

