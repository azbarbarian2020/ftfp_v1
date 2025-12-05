"""
FTFP React - FastAPI Backend
High-performance API for Fleet Telemetry Failure Prediction
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd
import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import List, Dict, Optional
import snowflake.connector
import threading
import time

# Configure logging to ensure output is visible in SPCS
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
from snowflake.snowpark.context import get_active_session
import os
from pathlib import Path

app = FastAPI(title="FTFP API", version="4.0.0-OPTIMIZED")

# Global refresh lock to prevent concurrent ML prediction refreshes
refresh_lock = threading.Lock()
refresh_in_progress = False
last_refresh_time = 0  # Track last refresh timestamp for cooldown

# 🔥 PERFORMANCE: Aggressive caching for high-frequency endpoints
cache_lock = threading.Lock()
telemetry_cache = {"data": None, "timestamp": 0}
predictions_cache = {"data": None, "timestamp": 0}
failures_cache = {"data": None, "timestamp": 0}
chart_cache = {"data": None, "timestamp": 0, "hours": None}
markers_cache = {"data": None, "timestamp": 0}
CACHE_TTL = 3  # Cache for 3 seconds (balance freshness vs performance)

# CORS middleware - must be added before routes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Snowflake connection - use Snowpark active session (works in Container Services)
session = None
conn = None

@app.on_event("startup")
async def startup_event():
    global session, conn
    
    logger.info("=" * 80)
    logger.info("🔍 STARTING SNOWFLAKE CONNECTION")
    logger.info("=" * 80)
    # Set schema prefix and update table constants
    global SCHEMA_PREFIX
    SCHEMA_PREFIX = get_schema_prefix()
    update_table_constants(SCHEMA_PREFIX)
    

    
    # Check if running locally or in SPCS
    token_file = "/snowflake/session/token"
    is_spcs = os.path.exists(token_file)
    
    logger.info(f"Token file exists: {is_spcs}")
    
    if is_spcs:
        logger.info("📦 Running in SPCS - using service OAuth token with internal connection")
        logger.info("📚 Reference: https://medium.com/snowflake/connecting-to-snowflake-from-snowpark-container-services-cfc3a133480e")
        
        try:
            # Read OAuth token
            with open(token_file, 'r') as f:
                token = f.read().strip()
            
            logger.info(f"🔑 OAuth token loaded ({len(token)} chars)")
            
            # Log Snowflake-provided environment variables
            snowflake_host = os.getenv('SNOWFLAKE_HOST')
            snowflake_port = os.getenv('SNOWFLAKE_PORT')
            snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
            snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
            snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
            snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
            
            logger.info(f"🔧 SNOWFLAKE_HOST: {snowflake_host}")
            logger.info(f"🔧 SNOWFLAKE_PORT: {snowflake_port}")
            logger.info(f"🔧 SNOWFLAKE_ACCOUNT: {snowflake_account}")
            logger.info(f"🔧 SNOWFLAKE_DATABASE: {snowflake_database}")
            logger.info(f"🔧 SNOWFLAKE_SCHEMA: {snowflake_schema}")
            logger.info(f"🔧 SNOWFLAKE_WAREHOUSE: {snowflake_warehouse}")
            
            # Use Snowflake-provided environment variables for internal routing
            logger.info("🔌 Connecting to Snowflake...")
            conn = snowflake.connector.connect(
                host=snowflake_host,
                port=snowflake_port,
                protocol="https",
                account=snowflake_account,
                authenticator="oauth",
                token=token,
                warehouse=snowflake_warehouse,
                database=snowflake_database,
                schema=snowflake_schema,
                client_session_keep_alive=True,
                autocommit=True
            )
            
            logger.info("✅ Snowflake connector established")
            
            # Create Snowpark session from connection
            logger.info("🔌 Creating Snowpark session...")
            from snowflake.snowpark import Session
            session = Session.builder.configs({
                "connection": conn
            }).create()
            
            logger.info("=" * 80)
            logger.info("✅ SPCS CONNECTION SUCCESSFUL")
            logger.info(f"   Database: {snowflake_database}")
            logger.info(f"   Schema: {snowflake_schema}")
            logger.info(f"   Warehouse: {snowflake_warehouse}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error(f"❌ SPCS CONNECTION FAILED: {e}")
            logger.error("=" * 80)
            import traceback
            traceback.print_exc()
            session = None
            conn = None
    else:
        print("💻 Running locally - using Snowpark session with PAT")
        # Use Snowpark Session.builder with PAT authentication
        try:
            from snowflake.snowpark import Session
            
            # Check for PAT in environment or use CLI connection
            snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
            
            if snowflake_password:
                # Use PAT authentication (recommended for local development)
                print("🔑 Using PAT authentication from SNOWFLAKE_PASSWORD")
                session = Session.builder.configs({
                    "account": os.getenv("SNOWFLAKE_ACCOUNT", "SFSENORTHAMERICA-AZUREBARBARIAN"),
                    "user": os.getenv("SNOWFLAKE_USER", "admin"),
                    "password": snowflake_password,
                    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "DEMO_WH"),
                    "database": os.getenv("SNOWFLAKE_DATABASE", "COMBO_PREDICT"),
                    "schema": os.getenv("SNOWFLAKE_SCHEMA", "FTFP"),
                }).create()
            else:
                # Fallback to CLI connection if no PAT provided
                print("🔌 No PAT found, using CLI connection: azurebarbarian")
                session = Session.builder.config("connection_name", "azurebarbarian").create()
            
            print("✅ Snowpark session created")
            # Get the underlying connection
            conn = session._conn._conn
            
        except Exception as e:
            print(f"❌ Local connection failed: {e}")
            print("💡 Set SNOWFLAKE_PASSWORD environment variable with your PAT:")
            print(f"   export SNOWFLAKE_PASSWORD='your_PAT_here'")
            import traceback
            traceback.print_exc()
            session = None
            conn = None

@app.on_event("shutdown")
async def shutdown_event():
    global conn
    if conn:
        try:
            conn.close()
        except:
            pass

def execute_query(query):
    """Execute SQL query and return pandas DataFrame - OPTIMIZED with connection reuse"""
    global session
    if not session:
        return pd.DataFrame()
    try:
        # Use Snowpark's to_pandas() with proper error handling
        # Connection is already pooled via session persistence
        result = session.sql(query)
        df = result.to_pandas()
        return df
    except Exception as e:
        logger.error(f"❌ Query error: {e}")
        # If pandas fails, try using collect() and convert manually
        try:
            result = session.sql(query)
            rows = result.collect()
            if not rows:
                return pd.DataFrame()
            # Convert Row objects to dictionaries
            data = [row.asDict() for row in rows]
            df = pd.DataFrame(data)
            return df
        except Exception as e2:
            logger.error(f"❌ Collect method also failed: {e2}")
            return pd.DataFrame()

def execute_sql(query):
    """Execute SQL query without return - OPTIMIZED with connection reuse"""
    global session
    if not session:
        return
    try:
        session.sql(query).collect()
    except Exception as e:
        logger.error(f"SQL error: {e}")


# =============================================================================
# 🔑 CRITICAL: Native App REFERENCE mechanism for database/schema resolution
# =============================================================================
def get_schema_prefix():
    """
    Get the fully qualified schema prefix for table names.
    In Native App context: reads SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA
    """
    db_name = os.getenv('SNOWFLAKE_DATABASE')
    schema_name = os.getenv('SNOWFLAKE_SCHEMA')
    
    if db_name and schema_name:
        prefix = f"{db_name}.{schema_name}"
        logger.info(f"📦 Using DB/SCHEMA env vars: {prefix}")
        return prefix
    
    # Fallback defaults
    logger.warning("⚠️ No DB/SCHEMA env vars - using defaults")
    return "FTFP_V1.FTFP"


def update_table_constants(schema_prefix):
    """Update all table name constants with the resolved schema prefix"""
    global TELEMETRY, NORMAL_SEED, ENGINE_FAILURE_SEED, TRANSMISSION_FAILURE_SEED
    global ELECTRICAL_FAILURE_SEED, FAILURE_CONFIG, STREAM_STATE
    global PREDICTION_CACHE, ACTIVE_FAILURES, FIRST_FAILURE_MARKERS
    global TELEMETRY_5MIN_AGG, ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF
    
    TELEMETRY = f"{schema_prefix}.TELEMETRY"
    NORMAL_SEED = f"{schema_prefix}.NORMAL_SEED"
    ENGINE_FAILURE_SEED = f"{schema_prefix}.ENGINE_FAILURE_SEED"
    TRANSMISSION_FAILURE_SEED = f"{schema_prefix}.TRANSMISSION_FAILURE_SEED"
    ELECTRICAL_FAILURE_SEED = f"{schema_prefix}.ELECTRICAL_FAILURE_SEED"
    FAILURE_CONFIG = f"{schema_prefix}.FAILURE_CONFIG"
    STREAM_STATE = f"{schema_prefix}.STREAM_STATE"
    PREDICTION_CACHE = f"{schema_prefix}.PREDICTION_CACHE"
    ACTIVE_FAILURES = f"{schema_prefix}.ACTIVE_FAILURES"
    FIRST_FAILURE_MARKERS = f"{schema_prefix}.FIRST_FAILURE_MARKERS"
    TELEMETRY_5MIN_AGG = f"{schema_prefix}.TELEMETRY_5MIN_AGG"
    ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF = f"{schema_prefix}.ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF"
    
    logger.info(f"📊 Table constants updated:")
    logger.info(f"   TELEMETRY = {TELEMETRY}")
    logger.info(f"   PREDICTION_CACHE = {PREDICTION_CACHE}")


# Global schema prefix - will be set in startup_event
SCHEMA_PREFIX = None

# Initialize table constants with defaults (will be updated in startup_event)
TELEMETRY = "FTFP_APP.DATA_SCHEMA.TELEMETRY"
NORMAL_SEED = "FTFP_APP.DATA_SCHEMA.NORMAL_SEED"
ENGINE_FAILURE_SEED = "FTFP_APP.DATA_SCHEMA.ENGINE_FAILURE_SEED"
TRANSMISSION_FAILURE_SEED = "FTFP_APP.DATA_SCHEMA.TRANSMISSION_FAILURE_SEED"
ELECTRICAL_FAILURE_SEED = "FTFP_APP.DATA_SCHEMA.ELECTRICAL_FAILURE_SEED"
FAILURE_CONFIG = "FTFP_APP.DATA_SCHEMA.FAILURE_CONFIG"
STREAM_STATE = "FTFP_APP.DATA_SCHEMA.STREAM_STATE"
PREDICTION_CACHE = "FTFP_APP.DATA_SCHEMA.PREDICTION_CACHE"
ACTIVE_FAILURES = "FTFP_APP.DATA_SCHEMA.ACTIVE_FAILURES"
FIRST_FAILURE_MARKERS = "FTFP_APP.DATA_SCHEMA.FIRST_FAILURE_MARKERS"
TELEMETRY_5MIN_AGG = "FTFP_APP.DATA_SCHEMA.TELEMETRY_5MIN_AGG"
ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF = "FTFP_APP.DATA_SCHEMA.ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF"
STREAM_NAME = "NORMAL_TO_TELEMETRY"

# API Endpoints

@app.get("/api/health")
async def health():
    return {"message": "FTFP API v4.0-OPTIMIZED", "status": "running"}

@app.get("/api/dashboard-data")
async def get_dashboard_data(include_charts: bool = False, hours: int = 1):
    """
    🔥 OPTIMIZED: Combined endpoint to fetch all dashboard data in ONE request
    Reduces 5 API calls to 1, eliminates connection overhead
    
    Returns:
    - telemetry: Latest telemetry for all trucks
    - predictions: Latest ML predictions from cache
    - failures: Active failures
    - chart_data: (optional) Chart telemetry data
    - markers: (optional) First failure markers
    """
    global telemetry_cache, predictions_cache, failures_cache, chart_cache, markers_cache
    
    current_time = time.time()
    response = {}
    
    # 1. Fetch telemetry (with caching)
    with cache_lock:
        if telemetry_cache["data"] is not None and (current_time - telemetry_cache["timestamp"]) < CACHE_TTL:
            response["telemetry"] = telemetry_cache["data"]
        else:
            telemetry_df = execute_query(f"""
                WITH latest_per_entity AS (
                    SELECT 
                        ENTITY_ID,
                        TIMESTAMP,
                        ENGINE_TEMP,
                        TRANS_OIL_PRESSURE,
                        BATTERY_VOLTAGE,
                        STATUS,
                        ROW_NUMBER() OVER (PARTITION BY ENTITY_ID ORDER BY TIMESTAMP DESC) as rn
                    FROM {TELEMETRY}
                )
                SELECT 
                    ENTITY_ID,
                    TIMESTAMP,
                    ENGINE_TEMP,
                    TRANS_OIL_PRESSURE,
                    BATTERY_VOLTAGE,
                    STATUS
                FROM latest_per_entity
                WHERE rn = 1
                ORDER BY ENTITY_ID
            """)
            telemetry_data = telemetry_df.to_dict('records')
            telemetry_cache["data"] = telemetry_data
            telemetry_cache["timestamp"] = current_time
            response["telemetry"] = telemetry_data
    
    # 2. Fetch predictions (with caching)
    with cache_lock:
        if predictions_cache["data"] is not None and (current_time - predictions_cache["timestamp"]) < CACHE_TTL:
            response["predictions"] = predictions_cache["data"]
        else:
            predictions_df = execute_query(f"""
                WITH cached_predictions AS (
                    SELECT 
                        ENTITY_ID,
                        PREDICTION_TIMESTAMP,
                        PREDICTED_FAILURE_TYPE,
                        PREDICTED_HOURS_TO_FAILURE,
                        TTF_MODEL_USED,
                        LAST_UPDATED
                    FROM {PREDICTION_CACHE}
                ),
                latest_telemetry AS (
                    SELECT 
                        ENTITY_ID,
                        MAX(TIMESTAMP) as LATEST_TELEMETRY_TIME
                    FROM {TELEMETRY}
                    GROUP BY ENTITY_ID
                )
                SELECT 
                    p.ENTITY_ID,
                    p.PREDICTION_TIMESTAMP,
                    p.PREDICTED_FAILURE_TYPE,
                    p.PREDICTED_HOURS_TO_FAILURE,
                    p.TTF_MODEL_USED,
                    p.LAST_UPDATED,
                    t.LATEST_TELEMETRY_TIME,
                    DATEDIFF('minute', p.PREDICTION_TIMESTAMP, t.LATEST_TELEMETRY_TIME) as AGE_MINUTES
                FROM cached_predictions p
                LEFT JOIN latest_telemetry t ON p.ENTITY_ID = t.ENTITY_ID
                ORDER BY p.ENTITY_ID
            """)
            predictions_data = predictions_df.to_dict('records')
            predictions_cache["data"] = predictions_data
            predictions_cache["timestamp"] = current_time
            response["predictions"] = predictions_data
    
    # 3. Fetch active failures (with caching)
    with cache_lock:
        if failures_cache["data"] is not None and (current_time - failures_cache["timestamp"]) < CACHE_TTL:
            response["failures"] = failures_cache["data"]
        else:
            failures_df = execute_query(f"""
                SELECT 
                    ENTITY_ID,
                    FAILURE_TYPE,
                    STARTED_AT,
                    LAST_UPDATED
                FROM {ACTIVE_FAILURES}
                ORDER BY STARTED_AT DESC
            """)
            failures_data = failures_df.to_dict('records')
            failures_cache["data"] = failures_data
            failures_cache["timestamp"] = current_time
            response["failures"] = failures_data
    
    # 4. Optionally fetch chart data (with caching)
    if include_charts:
        with cache_lock:
            if (chart_cache["data"] is not None and 
                chart_cache["hours"] == hours and
                (current_time - chart_cache["timestamp"]) < CACHE_TTL):
                response["chart_data"] = chart_cache["data"]
            else:
                # Optimized: Aggregate in SQL, not Python
                chart_df = execute_query(f"""
                    WITH time_buckets AS (
                        SELECT 
                            TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'START') as BUCKET_TIME,
                            ENTITY_ID,
                            AVG(ENGINE_TEMP) as ENGINE_TEMP,
                            AVG(TRANS_OIL_PRESSURE) as TRANS_OIL_PRESSURE,
                            AVG(BATTERY_VOLTAGE) as BATTERY_VOLTAGE
                        FROM {TELEMETRY}
                        WHERE TIMESTAMP >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
                        GROUP BY BUCKET_TIME, ENTITY_ID
                    )
                    SELECT 
                        BUCKET_TIME as TIMESTAMP,
                        ENTITY_ID,
                        ENGINE_TEMP,
                        TRANS_OIL_PRESSURE,
                        BATTERY_VOLTAGE
                    FROM time_buckets
                    ORDER BY BUCKET_TIME ASC
                    LIMIT 300
                """)
                chart_data = chart_df.to_dict('records')
                chart_cache["data"] = chart_data
                chart_cache["timestamp"] = current_time
                chart_cache["hours"] = hours
                response["chart_data"] = chart_data
        
        # 5. Fetch markers
        with cache_lock:
            if markers_cache["data"] is not None and (current_time - markers_cache["timestamp"]) < CACHE_TTL:
                response["markers"] = markers_cache["data"]
            else:
                markers_df = execute_query(f"""
                    SELECT 
                        ENTITY_ID,
                        FIRST_FAILURE_TIME,
                        FAILURE_TYPE,
                        LAST_UPDATED
                    FROM {FIRST_FAILURE_MARKERS}
                    ORDER BY FIRST_FAILURE_TIME
                """)
                markers_data = markers_df.to_dict('records')
                markers_cache["data"] = markers_data
                markers_cache["timestamp"] = current_time
                response["markers"] = markers_data
    
    return response

@app.post("/api/initialize")
async def initialize_database():
    """Initialize database tables and state"""
    try:
        # Create STREAM_STATE table
        execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {STREAM_STATE} (
                stream_name STRING PRIMARY KEY,
                start_ts TIMESTAMP_NTZ,
                step_seconds NUMBER(38,0),
                next_epoch NUMBER(38,0)
            )
        """)
        
        # Create FAILURE_CONFIG table
        execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {FAILURE_CONFIG} (
                entity_id STRING PRIMARY KEY,
                enabled BOOLEAN,
                failure_type STRING,
                failure_next_epoch NUMBER(38,0),
                effective_from_epoch NUMBER(38,0)
            )
        """)
        
        # Initialize STREAM_STATE if not exists
        execute_sql(f"""
            MERGE INTO {STREAM_STATE} t
            USING (SELECT '{STREAM_NAME}' AS stream_name) s
            ON t.stream_name = s.stream_name
            WHEN NOT MATCHED THEN INSERT (stream_name, start_ts, step_seconds, next_epoch)
            VALUES ('{STREAM_NAME}', CURRENT_TIMESTAMP(), 5, 0)
        """)
        
        # Update null values
        execute_sql(f"""
            UPDATE {STREAM_STATE}
            SET start_ts = COALESCE(start_ts, CURRENT_TIMESTAMP()),
                step_seconds = COALESCE(step_seconds, 5),
                next_epoch = COALESCE(next_epoch, 0)
            WHERE stream_name = '{STREAM_NAME}'
        """)
        
        return {"status": "success", "message": "Database initialized"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/telemetry/latest")
async def get_latest_telemetry():
    """Get latest telemetry data for all entities - v92 NO CACHE for accurate display"""
    # 🔥 REMOVED CACHING - we want real-time telemetry display
    query = f"""
    WITH latest_data AS (
      SELECT 
        entity_id, 
        timestamp, 
        engine_temp, 
        trans_oil_pressure, 
        battery_voltage,
        MAX(timestamp) OVER () as global_max_timestamp
      FROM {TELEMETRY}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY timestamp DESC) = 1
    ),
    all_entities AS (
      SELECT DISTINCT entity_id FROM {NORMAL_SEED}
    )
    SELECT 
      ae.entity_id,
      CASE 
        WHEN ld.timestamp IS NULL THEN 'OFFLINE'
        WHEN DATEDIFF('second', ld.timestamp, ld.global_max_timestamp) <= 10 THEN 'ONLINE'
        ELSE 'OFFLINE'
      END as status,
      ld.timestamp,
      ld.engine_temp,
      ld.trans_oil_pressure,
      ld.battery_voltage
    FROM all_entities ae
    LEFT JOIN latest_data ld ON ae.entity_id = ld.entity_id
    ORDER BY ae.entity_id
    """
    
    df = execute_query(query)
    result = json.loads(df.to_json(orient='records', date_format='iso'))
    return JSONResponse(content=result)

@app.get("/api/predictions/latest")
async def get_latest_predictions():
    """Get latest ML predictions from CACHE - 160x faster than querying view
    Also triggers auto-refresh if predictions are stale (age > 60 min)"""
    query = f"""
    WITH latest_telemetry AS (
        SELECT MAX(TIMESTAMP) as LATEST_TELEMETRY_TIME
        FROM {TELEMETRY}
    ),
    cached_predictions AS (
        SELECT 
            ENTITY_ID,
            PREDICTION_TIMESTAMP,
            PREDICTED_FAILURE_TYPE,
            PREDICTED_HOURS_TO_FAILURE,
            TTF_MODEL_USED,
            CURRENT_ENGINE_TEMP,
            CURRENT_TRANS_PRESSURE,
            CURRENT_BATTERY_VOLTAGE,
            LAST_UPDATED
        FROM {PREDICTION_CACHE}
    )
    SELECT 
        p.ENTITY_ID,
        p.PREDICTION_TIMESTAMP,
        p.PREDICTED_FAILURE_TYPE,
        p.PREDICTED_HOURS_TO_FAILURE,
        p.TTF_MODEL_USED,
        p.LAST_UPDATED,
        t.LATEST_TELEMETRY_TIME,
        -- Calculate age on-the-fly (fast math, no ML overhead)
        CASE
            WHEN t.LATEST_TELEMETRY_TIME >= p.PREDICTION_TIMESTAMP
            THEN DATEDIFF(MINUTE, p.PREDICTION_TIMESTAMP, t.LATEST_TELEMETRY_TIME)
            ELSE 0
        END as AGE_MINUTES,
        CASE
            WHEN t.LATEST_TELEMETRY_TIME >= p.PREDICTION_TIMESTAMP 
                AND DATEDIFF(MINUTE, p.PREDICTION_TIMESTAMP, t.LATEST_TELEMETRY_TIME) <= 5 
            THEN 'green'
            WHEN t.LATEST_TELEMETRY_TIME >= p.PREDICTION_TIMESTAMP 
                AND DATEDIFF(MINUTE, p.PREDICTION_TIMESTAMP, t.LATEST_TELEMETRY_TIME) < 60 
            THEN 'orange'
            WHEN t.LATEST_TELEMETRY_TIME < p.PREDICTION_TIMESTAMP
            THEN 'green'
            ELSE 'red'
        END as AGE_COLOR,
        -- CRITICAL: Return prediction data with a UNIQUE key that changes
        -- This forces React to see it as NEW data and re-render
        CONCAT(p.ENTITY_ID, '_', TO_VARCHAR(p.LAST_UPDATED, 'YYYY-MM-DD HH24:MI:SS.FF3')) as PREDICTION_KEY
    FROM cached_predictions p
    CROSS JOIN latest_telemetry t
    ORDER BY p.ENTITY_ID
    """
    
    try:
        df = execute_query(query)
        
        # Check if cache is empty (after reset) - need initial refresh
        if df.empty:
            logger.info("📭 Cache empty - returning empty array")
            return JSONResponse(content=[])
        
        # Check if any predictions are stale (age >= 60 min) and trigger auto-refresh
        max_age = df['AGE_MINUTES'].max()
        logger.debug(f"📊 Max prediction age: {max_age} minutes")
        
        if max_age >= 60:
            logger.info(f"🔄 Auto-refresh triggered: max age = {max_age} minutes")
            # Call refresh directly in background (asyncio.create_task might not work in SPCS)
            try:
                # Run refresh in background without awaiting
                import threading
                thread = threading.Thread(target=trigger_refresh_sync)
                thread.start()
                logger.info("✅ Auto-refresh thread started")
            except Exception as refresh_error:
                logger.error(f"❌ Failed to start auto-refresh: {refresh_error}")
        
        # Add cache-busting timestamp to force frontend update
        response_data = json.loads(df.to_json(orient='records', date_format='iso'))
        
        return JSONResponse(
            content=response_data,
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    except Exception as e:
        logger.error(f"❌ Error fetching ML predictions: {e}")
        return JSONResponse(content=[])

def trigger_refresh_sync():
    """Synchronous refresh trigger - directly execute the refresh logic with lock and cooldown"""
    global refresh_in_progress, last_refresh_time
    import time
    
    # Check cooldown - don't allow refreshes more than once every 15 seconds
    current_time = time.time()
    if current_time - last_refresh_time < 15:
        logger.warning(f"⚠️ Refresh cooldown active - skipping (last refresh {int(current_time - last_refresh_time)}s ago)")
        return
    
    # Check if refresh is already running
    if refresh_in_progress:
        logger.warning("⚠️ Refresh already in progress - skipping duplicate request")
        return
    
    with refresh_lock:
        refresh_in_progress = True
        last_refresh_time = current_time
        try:
            logger.info("🔄 Background refresh starting...")
            
            # Step 1: Update cache with latest ML predictions
            logger.info("📊 Querying ML predictions...")
            execute_sql(f"""
                MERGE INTO {PREDICTION_CACHE} AS target
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
                    FROM {ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF}
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ENTITY_ID ORDER BY PREDICTION_TIMESTAMP DESC) = 1
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
                    ENTITY_ID,
                    PREDICTION_TIMESTAMP,
                    PREDICTED_FAILURE_TYPE,
                    PREDICTED_HOURS_TO_FAILURE,
                    TTF_MODEL_USED,
                    CURRENT_ENGINE_TEMP,
                    CURRENT_TRANS_PRESSURE,
                    CURRENT_BATTERY_VOLTAGE,
                    LAST_UPDATED
                ) VALUES (
                    source.ENTITY_ID,
                    source.PREDICTION_TIMESTAMP,
                    source.PREDICTED_FAILURE_TYPE,
                    source.PREDICTED_HOURS_TO_FAILURE,
                    source.TTF_MODEL_USED,
                    source.CURRENT_ENGINE_TEMP,
                    source.CURRENT_TRANS_PRESSURE,
                    source.CURRENT_BATTERY_VOLTAGE,
                    CURRENT_TIMESTAMP()
                )
            """)
            logger.info("✅ Cache updated")
            
            # Step 2: Update markers - only insert NEW failures
            logger.info("🎯 Updating first failure markers...")
            execute_sql(f"""
                INSERT INTO {FIRST_FAILURE_MARKERS} (
                    ENTITY_ID, 
                    FIRST_FAILURE_TIME, 
                    FAILURE_TYPE,
                    LAST_UPDATED
                )
                SELECT 
                    p.ENTITY_ID,
                    TIME_SLICE(p.PREDICTION_TIMESTAMP, 5, 'MINUTE', 'START') as FIRST_FAILURE_TIME,
                    p.PREDICTED_FAILURE_TYPE as FAILURE_TYPE,
                    CURRENT_TIMESTAMP() as LAST_UPDATED
                FROM {PREDICTION_CACHE} p
                WHERE p.PREDICTED_FAILURE_TYPE != 'NORMAL'
                AND NOT EXISTS (
                    SELECT 1 FROM {FIRST_FAILURE_MARKERS} m
                    WHERE m.ENTITY_ID = p.ENTITY_ID 
                    AND m.FAILURE_TYPE = p.PREDICTED_FAILURE_TYPE
                )
            """)
            
            # Remove markers for trucks that cleared
            execute_sql("""
                DELETE FROM {FIRST_FAILURE_MARKERS}
                WHERE ENTITY_ID NOT IN (
                    SELECT ENTITY_ID 
                    FROM {PREDICTION_CACHE}
                    WHERE PREDICTED_FAILURE_TYPE != 'NORMAL'
                )
            """)
            
            logger.info("✅ Background refresh completed")
            
            # 🔥 PERFORMANCE: Invalidate predictions and markers cache
            with cache_lock:
                predictions_cache["timestamp"] = 0
                markers_cache["timestamp"] = 0
        except Exception as e:
            logger.error(f"❌ Background refresh failed: {e}")
        finally:
            refresh_in_progress = False

@app.post("/api/predictions/refresh")
async def refresh_predictions():
    """Update prediction cache and first-failure markers with cooldown"""
    global refresh_in_progress, last_refresh_time
    import time
    
    # Check cooldown
    current_time = time.time()
    if current_time - last_refresh_time < 15:
        elapsed = int(current_time - last_refresh_time)
        logger.warning(f"⚠️ Refresh too soon - wait {15 - elapsed}s (cooldown)")
        return {"status": "throttled", "message": f"Please wait {15 - elapsed} seconds"}
    
    # Check if already running
    if refresh_in_progress:
        logger.warning("⚠️ Refresh already in progress")
        return {"status": "in_progress", "message": "Refresh already running"}
    
    # Use the same refresh logic
    trigger_refresh_sync()
    return {"status": "success"}

# Chart data endpoint moved below - v40 restoration

@app.post("/api/writer/start")
async def start_writer():
    """Start the telemetry writer"""
    # Note: Actual continuous writing will be handled by background task
    return {"status": "started"}

@app.post("/api/writer/stop")
async def stop_writer():
    """Stop the telemetry writer"""
    return {"status": "stopped"}

@app.post("/api/writer/write-epoch")
async def write_epoch():
    """Write one epoch of telemetry data - v91 FIXED epoch counting"""
    try:
        # 🚀 STEP 1: Get current epoch (read BEFORE modifying)
        st_row = execute_query(f"SELECT start_ts, step_seconds, next_epoch FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}'")
        if st_row.empty:
            return {"status": "error", "message": "Stream state not found"}
        
        step_seconds = int(st_row.iloc[0]["STEP_SECONDS"]) or 5
        current_epoch = int(st_row.iloc[0]["NEXT_EPOCH"])
        target_epoch = current_epoch + 1
        
        # 🚀 STEP 2: Write ALL data (normal + failures) in ONE INSERT using target_epoch
        execute_sql(f"""
            INSERT INTO {TELEMETRY} (Timestamp, entity_id, engine_temp, trans_oil_pressure, battery_voltage)
            WITH stream_info AS (
                SELECT start_ts, step_seconds, {target_epoch} AS target_epoch
                FROM {STREAM_STATE}
                WHERE stream_name = '{STREAM_NAME}'
            ),
            active_failures AS (
                SELECT fc.entity_id, fc.failure_type, 
                       COALESCE(fc.failure_next_epoch, 0) + 1 AS next_failure_epoch
                FROM {FAILURE_CONFIG} fc, stream_info si
                WHERE fc.enabled = true 
                  AND fc.effective_from_epoch <= si.target_epoch
            ),
            normal_data AS (
                SELECT 
                    DATEADD(second, si.target_epoch * si.step_seconds, si.start_ts) AS ts,
                    n.entity_id, n.engine_temp, n.trans_oil_pressure, n.battery_voltage
                FROM stream_info si
                CROSS JOIN {NORMAL_SEED} n
                LEFT JOIN active_failures af ON af.entity_id = n.entity_id
                WHERE n.epoch = si.target_epoch
                  AND af.entity_id IS NULL
            ),
            failure_data AS (
                SELECT 
                    DATEADD(second, si.target_epoch * si.step_seconds, si.start_ts) AS ts,
                    af.entity_id,
                    COALESCE(ef.engine_temp, tf.engine_temp, el.engine_temp) AS engine_temp,
                    COALESCE(ef.trans_oil_pressure, tf.trans_oil_pressure, el.trans_oil_pressure) AS trans_oil_pressure,
                    COALESCE(ef.battery_voltage, tf.battery_voltage, el.battery_voltage) AS battery_voltage
                FROM stream_info si
                CROSS JOIN active_failures af
                LEFT JOIN {ENGINE_FAILURE_SEED} ef 
                    ON af.failure_type = 'ENGINE' AND ef.epoch = af.next_failure_epoch
                LEFT JOIN {TRANSMISSION_FAILURE_SEED} tf 
                    ON af.failure_type = 'TRANSMISSION' AND tf.epoch = af.next_failure_epoch
                LEFT JOIN {ELECTRICAL_FAILURE_SEED} el 
                    ON af.failure_type = 'ELECTRICAL' AND el.epoch = af.next_failure_epoch
                WHERE COALESCE(ef.epoch, tf.epoch, el.epoch) IS NOT NULL
            ),
            all_data AS (
                SELECT * FROM normal_data
                UNION ALL
                SELECT * FROM failure_data
            )
            SELECT ad.ts, ad.entity_id, ad.engine_temp, ad.trans_oil_pressure, ad.battery_voltage
            FROM all_data ad
            LEFT JOIN {TELEMETRY} existing 
                ON existing.Timestamp = ad.ts AND existing.entity_id = ad.entity_id
            WHERE existing.entity_id IS NULL
        """)
        
        # 🚀 STEP 3: Update failure cursors
        execute_sql(f"""
            UPDATE {FAILURE_CONFIG} fc
            SET failure_next_epoch = COALESCE(fc.failure_next_epoch, 0) + 1
            WHERE fc.enabled = true
              AND fc.effective_from_epoch <= {target_epoch}
              AND EXISTS (
                  SELECT 1 FROM (
                      SELECT 'ENGINE' AS ft, epoch FROM {ENGINE_FAILURE_SEED}
                      UNION ALL SELECT 'TRANSMISSION', epoch FROM {TRANSMISSION_FAILURE_SEED}
                      UNION ALL SELECT 'ELECTRICAL', epoch FROM {ELECTRICAL_FAILURE_SEED}
                  ) seeds
                  WHERE seeds.ft = fc.failure_type 
                    AND seeds.epoch = COALESCE(fc.failure_next_epoch, 0) + 1
              )
        """)
        
        # 🚀 STEP 4: Advance global epoch to target_epoch
        execute_sql(f"""
            UPDATE {STREAM_STATE} 
            SET next_epoch = {target_epoch}
            WHERE stream_name = '{STREAM_NAME}'
        """)
        
        # 🔥 Invalidate telemetry cache
        with cache_lock:
            telemetry_cache["timestamp"] = 0
        
        logger.info(f"✅ Epoch {target_epoch} written")
        return {"status": "success", "epoch": target_epoch}
    except Exception as e:
        logger.error(f"❌ Write epoch failed: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/fast-forward/{hours}")
async def fast_forward(hours: int):
    """Fast forward simulation by hours - MATCHES FTFP GOLD V3 LOGIC"""
    try:
        logger.info(f"⏩ Fast forward requested: {hours} hours")
        
        st_row = execute_query(f"select start_ts, step_seconds, next_epoch from {STREAM_STATE} where stream_name = '{STREAM_NAME}'")
        if st_row.empty:
            return {"status": "error", "message": "Stream state not found"}
        
        step_seconds = int(st_row.iloc[0]["STEP_SECONDS"]) or 5
        ne = int(st_row.iloc[0]["NEXT_EPOCH"])
        epochs_to_write = int((hours * 3600) // step_seconds)
        
        if epochs_to_write <= 0:
            return {"status": "error", "message": "No epochs to write"}
        
        logger.info(f"⏩ Writing {epochs_to_write} epochs (from epoch {ne+1} to {ne + epochs_to_write})")
        
        # Get failure configuration
        cfg_df = execute_query(f"""
            SELECT entity_id, failure_type, failure_next_epoch, effective_from_epoch
            FROM {FAILURE_CONFIG}
            WHERE enabled = true
        """)
        
        failure_entities = cfg_df['ENTITY_ID'].tolist() if not cfg_df.empty else []
        
        # Build exclusion filter for failure entities
        in_filter = ""
        if failure_entities:
            ids = ",".join([f"'{e.replace(chr(39), chr(39)+chr(39))}'" for e in failure_entities])
            in_filter = f" AND n.entity_id NOT IN ({ids})"
        
        # Bulk insert NORMAL entities
        execute_sql(
            f"""
            INSERT INTO {TELEMETRY} (Timestamp, entity_id, engine_temp, trans_oil_pressure, battery_voltage)
            SELECT 
                DATEADD(second, (st.ne + g.seq + 1) * st.step_seconds, st.start_ts) as ts,
                n.entity_id, 
                n.engine_temp, 
                n.trans_oil_pressure, 
                n.battery_voltage
            FROM (SELECT next_epoch as ne, start_ts, step_seconds FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}') st
            CROSS JOIN (SELECT seq4() as seq FROM table(generator(rowcount => {epochs_to_write}))) g
            JOIN {NORMAL_SEED} n ON n.epoch = st.ne + g.seq + 1
            LEFT JOIN {TELEMETRY} d 
                ON d.Timestamp = DATEADD(second, (st.ne + g.seq + 1) * st.step_seconds, st.start_ts)
                AND d.entity_id = n.entity_id
            WHERE d.entity_id IS NULL{in_filter}
            """
        )
        
        # Bulk insert FAILURE entities (complex epoch calculation from FTFP GOLD V3)
        cursor_updates = []  # Batch cursor updates
        for _, row in cfg_df.iterrows():
            eid = row['ENTITY_ID']
            ftype = row['FAILURE_TYPE']
            cur = int(row['FAILURE_NEXT_EPOCH']) if row['FAILURE_NEXT_EPOCH'] is not None else 0
            eff = int(row['EFFECTIVE_FROM_EPOCH']) if row['EFFECTIVE_FROM_EPOCH'] is not None else 0
            
            # Get the appropriate failure seed table
            if ftype == 'ENGINE':
                seed_table = ENGINE_FAILURE_SEED
            elif ftype == 'TRANSMISSION':
                seed_table = TRANSMISSION_FAILURE_SEED
            else:  # ELECTRICAL
                seed_table = ELECTRICAL_FAILURE_SEED
            
            eid_esc = eid.replace("'", "''")
            
            # Insert failure data using the complex epoch calculation from Streamlit
            execute_sql(f"""
                INSERT INTO {TELEMETRY} (Timestamp, entity_id, engine_temp, trans_oil_pressure, battery_voltage)
                SELECT 
                    DATEADD(second, (st.ne + g.seq + 1) * st.step_seconds, st.start_ts) as ts,
                    '{eid_esc}', 
                    f.engine_temp, 
                    f.trans_oil_pressure, 
                    f.battery_voltage
                FROM (SELECT next_epoch as ne, start_ts, step_seconds FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}') st
                CROSS JOIN (SELECT seq4() as seq FROM table(generator(rowcount => {epochs_to_write}))) g
                JOIN {seed_table} f ON f.epoch = {cur} + (g.seq + 1 - GREATEST(0, {eff} - (st.ne + 1)))
                LEFT JOIN {TELEMETRY} d 
                    ON d.Timestamp = DATEADD(second, (st.ne + g.seq + 1) * st.step_seconds, st.start_ts)
                    AND d.entity_id = '{eid_esc}'
                WHERE (st.ne + g.seq + 1) >= {eff} AND d.entity_id IS NULL
            """)
            
            # Calculate epochs written (simple math - no query needed)
            # If eff is in the future, we write fewer epochs
            if ne + epochs_to_write >= eff:
                adv = min(epochs_to_write, ne + epochs_to_write - eff + 1) if eff > ne else epochs_to_write
                if adv > 0:
                    cursor_updates.append((eid_esc, adv))
                    logger.info(f"⏩ {eid} will advance cursor by {adv} epochs")
        
        # Batch update all failure cursors in one query
        if cursor_updates:
            # Build CASE statement for batch update
            when_clauses = "\n".join([f"WHEN entity_id = '{eid}' THEN COALESCE(failure_next_epoch, 0) + {adv}" 
                                      for eid, adv in cursor_updates])
            entity_list = ",".join([f"'{eid}'" for eid, _ in cursor_updates])
            execute_sql(f"""
                UPDATE {FAILURE_CONFIG}
                SET failure_next_epoch = CASE
                    {when_clauses}
                END
                WHERE entity_id IN ({entity_list})
            """)
            logger.info(f"✅ Batch updated {len(cursor_updates)} failure cursors")
        
        # Advance global epoch
        execute_sql(f"UPDATE {STREAM_STATE} SET next_epoch = next_epoch + {epochs_to_write} WHERE stream_name = '{STREAM_NAME}'")
        
        logger.info(f"✅ Fast forward complete: {epochs_to_write} epochs written")
        
        # Check if we need to trigger prediction refresh (if fast forward was >= 1 hour)
        if hours >= 1:
            logger.info("🔄 Fast forward >= 1 hour - triggering async prediction refresh...")
            try:
                # Run refresh in background thread - don't block the response
                import threading
                refresh_thread = threading.Thread(target=trigger_refresh_sync, daemon=True)
                refresh_thread.start()
                logger.info("✅ Prediction refresh started in background")
            except Exception as refresh_error:
                logger.error(f"❌ Failed to start background refresh: {refresh_error}")
        
        return {"status": "success", "epochs_inserted": epochs_to_write}
    except Exception as e:
        logger.error(f"❌ Fast forward failed: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/failure/activate")
async def activate_failure_endpoint(entity_id: str, failure_type: str):
    """Activate failure for an entity"""
    try:
        ft = failure_type.upper()
        eid_esc = entity_id.replace("'", "''")
        
        st_row = execute_query(f"select next_epoch from {STREAM_STATE} where stream_name = '{STREAM_NAME}'")
        ne = int(st_row.iloc[0]["NEXT_EPOCH"]) if not st_row.empty else 0
        eff = ne + 1

        execute_sql(f"""
            merge into {FAILURE_CONFIG} t
            using (select '{eid_esc}' as entity_id) s
            on t.entity_id = s.entity_id
            when matched then update set enabled = true, failure_type = '{ft}', failure_next_epoch = coalesce(t.failure_next_epoch, 0), effective_from_epoch = {eff}
            when not matched then insert(entity_id, enabled, failure_type, failure_next_epoch, effective_from_epoch) values('{eid_esc}', true, '{ft}', 0, {eff})
        """)
        
        # 🔥 PERFORMANCE: Invalidate failures cache immediately for instant UI update
        with cache_lock:
            failures_cache["timestamp"] = 0
        
        return {"status": "success", "message": f"{ft} failure activated for {entity_id}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/failures/active")
async def get_active_failures():
    """Get list of active failures with status (ACTIVE/OFFLINE) - v88 with caching"""
    current_time = time.time()
    
    # 🚀 Check cache first (3-second TTL)
    with cache_lock:
        if failures_cache["data"] is not None and (current_time - failures_cache["timestamp"]) < CACHE_TTL:
            logger.info("📦 Serving failures from cache")
            return JSONResponse(content=failures_cache["data"])
    
    try:
        # Get current epoch
        st_row = execute_query(f"select next_epoch from {STREAM_STATE} where stream_name = '{STREAM_NAME}'")
        current_epoch = int(st_row.iloc[0]["NEXT_EPOCH"]) if not st_row.empty else 0
        
        query = f"""
        WITH failure_seed_sizes AS (
            SELECT 'ENGINE' as failure_type, COUNT(*) as max_epochs FROM {ENGINE_FAILURE_SEED}
            UNION ALL
            SELECT 'TRANSMISSION', COUNT(*) FROM {TRANSMISSION_FAILURE_SEED}
            UNION ALL
            SELECT 'ELECTRICAL', COUNT(*) FROM {ELECTRICAL_FAILURE_SEED}
        )
        SELECT 
            fc.entity_id,
            fc.failure_type,
            fc.effective_from_epoch,
            fc.failure_next_epoch as current_failure_epoch,
            fss.max_epochs,
            CASE 
                WHEN fc.failure_next_epoch > fss.max_epochs THEN 'OFFLINE'
                ELSE 'ACTIVE'
            END as status
        FROM {FAILURE_CONFIG} fc
        JOIN failure_seed_sizes fss ON fss.failure_type = fc.failure_type
        WHERE fc.enabled = true
        ORDER BY fc.entity_id
        """
        
        df = execute_query(query)
        result = json.loads(df.to_json(orient='records'))
        
        # 💾 Update cache
        with cache_lock:
            failures_cache["data"] = result
            failures_cache["timestamp"] = current_time
        
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"❌ Get active failures failed: {e}")
        return JSONResponse(content=[])

@app.delete("/api/failure/clear")
async def clear_failures():
    """Clear all active failures"""
    try:
        execute_sql(f"DELETE FROM {FAILURE_CONFIG}")
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/chart-data/{hours}")
async def get_chart_data(hours: int):
    """Get chart data - v110: With inline aggregation fallback"""
    try:
        logger.info(f"📊 Fetching chart data for last {hours} hours")
        
        # Try using TELEMETRY_5MIN_AGG view first (faster if exists)
        try:
            query = f"""
            SELECT 
                TO_CHAR(bucket_time, 'YYYY-MM-DD HH24:MI:SS') as TIMESTAMP,
                entity_id as ENTITY_ID,
                avg_engine_temp as ENGINE_TEMP,
                avg_trans_oil_pressure as TRANS_OIL_PRESSURE,
                avg_battery_voltage as BATTERY_VOLTAGE
            FROM {TELEMETRY_5MIN_AGG}
            WHERE bucket_time >= DATEADD(hour, -{hours}, (SELECT MAX(bucket_time) FROM {TELEMETRY_5MIN_AGG}))
            ORDER BY bucket_time ASC
            LIMIT 2000
            """
            df = execute_query(query)
            if not df.empty:
                logger.info(f"✅ Chart data from view: {len(df)} rows")
                return JSONResponse(content=json.loads(df.to_json(orient='records')))
        except Exception as view_error:
            logger.warning(f"⚠️ TELEMETRY_5MIN_AGG view not available: {view_error}")
        
        # Fallback: inline aggregation from TELEMETRY table
        logger.info("📊 Using inline aggregation from TELEMETRY table")
        fallback_query = f"""
        SELECT 
            TO_CHAR(TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END'), 'YYYY-MM-DD HH24:MI:SS') as TIMESTAMP,
            ENTITY_ID,
            AVG(ENGINE_TEMP) as ENGINE_TEMP,
            AVG(TRANS_OIL_PRESSURE) as TRANS_OIL_PRESSURE,
            AVG(BATTERY_VOLTAGE) as BATTERY_VOLTAGE
        FROM {TELEMETRY}
        WHERE TIMESTAMP >= DATEADD(hour, -{hours}, (SELECT MAX(TIMESTAMP) FROM {TELEMETRY}))
        GROUP BY TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END'), ENTITY_ID
        ORDER BY TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END') ASC
        LIMIT 2000
        """
        df = execute_query(fallback_query)
        logger.info(f"✅ Chart data from inline aggregation: {len(df)} rows")
        
        return JSONResponse(content=json.loads(df.to_json(orient='records')))
    except Exception as e:
        logger.error(f"❌ Get chart data failed: {e}")
        return JSONResponse(content=[])

@app.get("/api/predictions/first-failure-markers")
async def get_first_failure_markers():
    """Get first failure prediction markers from simple table (fast!)"""
    try:
        logger.info("🎯 Fetching first failure markers from FIRST_FAILURE_MARKERS table")
        
        # Query simple table with same timestamp format as chart data
        query = f"""
        SELECT 
            ENTITY_ID,
            TO_CHAR(FIRST_FAILURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as FIRST_FAILURE_TIME,
            FAILURE_TYPE
        FROM {FIRST_FAILURE_MARKERS}
        ORDER BY ENTITY_ID
        """
        
        df = execute_query(query)
        logger.info(f"🎯 First failure markers: {len(df)} trucks")
        
        # Return as JSON records (timestamps already formatted as strings)
        return JSONResponse(content=json.loads(df.to_json(orient='records')))
    except Exception as e:
        logger.error(f"❌ Get first failure markers failed: {e}")
        return JSONResponse(content=[])

@app.post("/api/reset")
@app.get("/api/reset")
@app.post("/api/reset-all")
@app.get("/api/reset-all")
async def reset_all_data():
    """Reset all telemetry and failure data - complete clean slate"""
    try:
        logger.info("=" * 80)
        logger.info("🔄 FULL RESET - Resetting everything to clean slate...")
        logger.info("=" * 80)
        
        # Use TRUNCATE for all tables that support it (much faster, no contention)
        logger.info("Truncating TELEMETRY...")
        execute_sql(f"TRUNCATE TABLE IF EXISTS {TELEMETRY}")
        
        logger.info("Truncating FIRST_FAILURE_MARKERS...")
        execute_sql(f"TRUNCATE TABLE IF EXISTS {FIRST_FAILURE_MARKERS}")
        
        logger.info("Clearing ACTIVE_FAILURES (hybrid table - use DELETE)...")
        execute_sql(f"DELETE FROM {ACTIVE_FAILURES}")
        
        logger.info("Clearing PREDICTION_CACHE (hybrid table - use DELETE)...")
        execute_sql(f"DELETE FROM {PREDICTION_CACHE}")
        
        logger.info("Truncating FAILURE_CONFIG...")
        execute_sql(f"TRUNCATE TABLE IF EXISTS {FAILURE_CONFIG}")
        
        # Reset stream state to epoch 0 and current timestamp
        # Use DELETE + INSERT for hybrid table reliability
        logger.info("Resetting STREAM_STATE to epoch 0 and current timestamp...")
        execute_sql(f"DELETE FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}'")
        execute_sql(f"""
            INSERT INTO {STREAM_STATE} (stream_name, start_ts, step_seconds, next_epoch)
            VALUES ('{STREAM_NAME}', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 5, 0)
        """)
        
        # Verify the reset
        verify_df = execute_query(f"SELECT start_ts, next_epoch FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}'")
        if not verify_df.empty:
            logger.info("=" * 80)
            logger.info(f"✅ FULL RESET COMPLETE!")
            logger.info(f"   start_ts: {verify_df.iloc[0]['START_TS']}")
            logger.info(f"   epoch: {verify_df.iloc[0]['NEXT_EPOCH']}")
            logger.info("=" * 80)
        
        return {"status": "success", "message": "Complete reset - all data cleared"}
    except Exception as e:
        logger.error(f"❌ Reset failed: {e}")
        logger.exception("Reset exception details:")
        return {"status": "error", "message": str(e)}

@app.get("/api/writer/status")
async def get_writer_status():
    """Get current writer state"""
    try:
        df = execute_query(f"select start_ts, step_seconds, next_epoch from {STREAM_STATE} where stream_name = '{STREAM_NAME}'")
        if df.empty:
            return {"status": "not_initialized"}
        
        return {
            "status": "initialized",
            "epoch": int(df.iloc[0]["NEXT_EPOCH"]),
            "step_seconds": int(df.iloc[0]["STEP_SECONDS"])
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# WebSocket for real-time updates
active_connections: List[WebSocket] = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    
    try:
        while True:
            # Send updates every 5 seconds
            await asyncio.sleep(5)
            
            # Get latest telemetry
            telemetry_df = execute_query(f"""
                WITH latest AS (SELECT MAX(timestamp) AS ts FROM {TELEMETRY})
                SELECT * FROM {TELEMETRY}
                WHERE timestamp = (SELECT ts FROM latest)
                ORDER BY entity_id
            """)
            
            await websocket.send_json({
                "type": "telemetry_update",
                "data": json.loads(telemetry_df.to_json(orient='records', date_format='iso'))
            })
            
    except WebSocketDisconnect:
        active_connections.remove(websocket)

# Mount React static files AFTER all API routes are defined
# In Docker container: main.py is at /app/main.py, frontend at /app/frontend/build
frontend_build = Path(__file__).parent / "frontend" / "build"
logger.info(f"📁 Looking for frontend at: {frontend_build}")
if frontend_build.exists():
    logger.info(f"✅ Frontend found, mounting static files")
    app.mount("/", StaticFiles(directory=str(frontend_build), html=True), name="static")
else:
    logger.warning(f"⚠️ Frontend not found at {frontend_build}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

