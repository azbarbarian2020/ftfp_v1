# 🤖 Cursor Rules: Snowflake SPCS & Native App Development

**Project:** Fleet Telemetry Failure Prediction (FTFP)  
**Compiled From:** 20+ cursor projects spanning 3+ months of development  
**Last Updated:** December 2025

---

## 📋 Table of Contents

1. [Snowflake CLI & Authentication](#1-snowflake-cli--authentication)
2. [Docker & Container Images](#2-docker--container-images)
3. [Snowpark Container Services (SPCS)](#3-snowpark-container-services-spcs)
4. [Database Cloning & Independence](#4-database-cloning--independence)
5. [Frontend/Backend Communication](#5-frontendbackend-communication)
6. [Native App Limitations](#6-native-app-limitations)
7. [ML Models & Python UDFs](#7-ml-models--python-udfs)
8. [GitHub Integration & Deployment](#8-github-integration--deployment)
9. [Data Loading & Stages](#9-data-loading--stages)
10. [Debugging & Troubleshooting](#10-debugging--troubleshooting)

---

## 1. Snowflake CLI & Authentication

### Setting Up Connections

```bash
# Add a connection
snow connection add

# Test connection
snow connection test --connection CONNECTION_NAME

# List connections
snow connection list
```

### Connection Configuration (~/.snowflake/config.toml)

```toml
[connections.myconnection]
account = "ORGNAME-ACCOUNTNAME"
user = "USERNAME"
password = "PASSWORD"  # Or use authenticator
warehouse = "WAREHOUSE_NAME"
database = "DATABASE_NAME"
schema = "SCHEMA_NAME"
role = "ACCOUNTADMIN"
```

### ✅ DO:
- Use `snow sql --connection NAME -q "SQL"` for inline queries
- Use `snow sql --connection NAME -f script.sql` for files
- Store connections in config file for reuse

### ❌ DON'T:
- Hardcode credentials in scripts
- Use `snowsql` (deprecated) - use `snow` CLI instead

---

## 2. Docker & Container Images

### Critical Rule: Platform Must Be linux/amd64

```bash
# ALWAYS specify platform for Snowflake SPCS
docker build --platform linux/amd64 -t image:tag .

# NEVER build without platform flag on Mac M1/M2
# Default Mac ARM builds WILL NOT WORK in Snowflake
```

### Image Tagging Format

```
<account>.registry.snowflakecomputing.com/<database>/<schema>/<repository>/<image>:<tag>
```

**Format Rules:**
- `database`, `schema`, `repository`: **LOWERCASE with underscores**
- `account`: as-is (may have hyphens)
- `image`: any valid string
- `tag`: any string (use semantic versioning)

**✅ CORRECT:**
```
sfsenorthamerica-awsbarbarian.registry.snowflakecomputing.com/ftfp_v1/images/ftfp_repo/ftfp_v1:v1
```

**❌ WRONG:**
```
sfsenorthamerica-awsbarbarian.registry.snowflakecomputing.com/FTFP_V1/IMAGES/FTFP_REPO/ftfp_v1:v1
```

### Docker Login to Snowflake Registry

```bash
# Login to JUST the registry host (not full path)
docker login sfsenorthamerica-awsbarbarian.registry.snowflakecomputing.com

# Username: Snowflake username
# Password: Snowflake password

# OR use Snowflake CLI
snow spcs image-registry login --connection CONNECTION_NAME
```

### Image Pull/Tag/Push Workflow

```bash
# 1. Pull from source (e.g., GHCR)
docker pull ghcr.io/username/image:tag

# 2. Tag for Snowflake (add /image:tag to repository URL)
docker tag ghcr.io/username/image:tag \
  account.registry.snowflakecomputing.com/db/schema/repo/image:tag

# 3. Login
docker login account.registry.snowflakecomputing.com

# 4. Push
docker push account.registry.snowflakecomputing.com/db/schema/repo/image:tag
```

---

## 3. Snowpark Container Services (SPCS)

### Compute Pool Creation

```sql
CREATE COMPUTE POOL IF NOT EXISTS POOL_NAME
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS  -- Use smallest that works
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 3600;
```

### Service Creation

```sql
CREATE SERVICE SERVICE_NAME
IN COMPUTE POOL POOL_NAME
FROM SPECIFICATION $$
spec:
  containers:
  - name: container-name
    image: /database/schema/repo/image:tag  -- Relative path, lowercase
    env:
      ENV_VAR: value
  endpoints:
  - name: endpoint-name
    port: 8000
    public: true
$$
MIN_INSTANCES = 1
MAX_INSTANCES = 1;
```

### Service Debugging Commands

```sql
-- Check service status
SELECT SYSTEM$GET_SERVICE_STATUS('SERVICE_NAME');

-- Get service logs
CALL SYSTEM$GET_SERVICE_LOGS('SERVICE_NAME', '0', 'container-name', 100);

-- Show endpoints
SHOW ENDPOINTS IN SERVICE SERVICE_NAME;

-- Get endpoint URL
SELECT "ingress_url" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

### Performance Insights

**Smaller is often faster for this workload:**
- XS warehouse/compute pool outperforms larger sizes for small queries
- Python UDFs don't parallelize across nodes
- Cold start is faster with smaller resources
- Auto-suspend/resume is quicker with XS

---

## 4. Database Cloning & Independence

### What DATABASE CLONE Includes

- ✅ Tables (structure + data via zero-copy)
- ✅ Views
- ✅ Schemas
- ✅ Materialized views

### What DATABASE CLONE Does NOT Include

- ❌ User-Defined Functions (UDFs)
- ❌ Stored Procedures
- ❌ Stages (and their contents)
- ❌ File formats
- ❌ Image repositories
- ❌ Services
- ❌ Compute pools

### Creating Independent Copy

```bash
# 1. Clone database
CREATE DATABASE TARGET_DB CLONE SOURCE_DB;

# 2. Create stage in target
CREATE STAGE @MODELS;

# 3. Copy models from source to target
# (Download to local, upload to target)
GET @SOURCE_DB.SCHEMA.MODELS file:///tmp/;
PUT file:///tmp/*.pkl.gz @TARGET_DB.SCHEMA.MODELS;

# 4. Create UDFs in target (they'll use target's stage)
# 5. Create image repository in target
# 6. Push image to target's repository
# 7. Deploy service
```

### Critical: Extract from Working Image

**NEVER use source files from disk. ALWAYS extract from working Docker image:**

```bash
docker pull <working_image>
CONTAINER_ID=$(docker create <working_image>)
docker cp $CONTAINER_ID:/app/backend/main.py ./backend/
docker cp $CONTAINER_ID:/app/frontend/build/ ./frontend/
docker rm $CONTAINER_ID
```

**Why:** Source files may be outdated. The working image contains exactly what works.

### Changing Database References

```bash
# Find all occurrences
grep -c "SOURCE_DB\.SCHEMA\." backend/main.py

# Replace ALL
sed 's/SOURCE_DB\.SCHEMA\./TARGET_DB.SCHEMA./g' input.py > output.py

# Verify none remain
grep -c "SOURCE_DB\." output.py  # Must be 0
```

### Verify Frontend with MD5

```bash
md5 frontend/build/static/js/main.*.js
# Must match the working image's frontend exactly
```

---

## 5. Frontend/Backend Communication

### SPCS Authentication Flow

1. User accesses SPCS endpoint URL
2. Snowflake OAuth handles authentication
3. Backend receives requests with Snowflake session context
4. Backend uses `snowflake.connector` to connect to Snowflake

### Backend Database Connection

```python
import snowflake.connector
import os

# In SPCS, use login_token from file
def get_connection():
    if os.path.exists('/snowflake/session/token'):
        # Running in SPCS container
        return snowflake.connector.connect(
            host=os.environ['SNOWFLAKE_HOST'],
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            token=open('/snowflake/session/token').read(),
            authenticator='oauth',
            database=os.environ.get('SNOWFLAKE_DATABASE'),
            schema=os.environ.get('SNOWFLAKE_SCHEMA'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE')
        )
```

### Frontend API Calls

```javascript
// Relative paths work in SPCS (same origin)
fetch('/api/telemetry/latest')
fetch('/api/predictions')

// No CORS issues when frontend served from same container
```

### Polling vs WebSockets

- **Polling (recommended):** Simple, reliable, works through Snowflake proxy
- **WebSockets:** More complex, may have issues with SPCS proxy

---

## 6. Native App Limitations

### What Native Apps CANNOT Do

| Feature | Status | Notes |
|---------|--------|-------|
| Bundle SPCS images | ❌ | No working syntax for image bundling |
| Create external databases | ❌ | Can only create within app namespace |
| Python UDFs in versioned schemas | ❌ | External packages not allowed |
| COPY INTO from package stage | ❌ | Files not accessible |
| Account-level operations | ❌ | Even with grants |

### What Native Apps CAN Do

- ✅ SQL procedures and functions
- ✅ JavaScript UDFs
- ✅ Tables, views in app schemas
- ✅ Compute pool creation (with grants)
- ✅ Service creation (if image accessible)

### Recommended Architecture for SPCS Apps

**Instead of Native App bundling:**
1. Create GitHub repository with deployment scripts
2. Push Docker image to public registry (GHCR)
3. Consumer runs SQL scripts + Docker push
4. Simpler, more reliable, full control

---

## 7. ML Models & Python UDFs

### Creating Python UDFs with Models

```sql
CREATE OR REPLACE FUNCTION CLASSIFY_FAILURE_ML(
    param1 FLOAT, param2 FLOAT, ...
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('xgboost','numpy','pandas','scikit-learn','joblib')
HANDLER = 'classify'
IMPORTS = (
    '@DATABASE.SCHEMA.MODELS/classifier.pkl.gz',
    '@DATABASE.SCHEMA.MODELS/label_mapping.pkl.gz'
)
AS $$
import sys, joblib, numpy as np

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

# Load models (happens once per warehouse session)
model = joblib.load(import_dir + "classifier.pkl.gz")
labels = joblib.load(import_dir + "label_mapping.pkl.gz")

def classify(param1, param2, ...):
    features = np.array([[param1, param2, ...]])
    if np.isnan(features).any():
        return "NORMAL"
    prediction = model.predict(features)[0]
    return labels["reverse_mapping"][int(prediction)]
$$;
```

### Model Files Required

| File | Purpose |
|------|---------|
| `classifier_v1_0_0.pkl.gz` | XGBoost classifier |
| `regression_v1_0_0.pkl.gz` | TTF regression (11 features) |
| `regression_temporal_v1_1_0.pkl.gz` | TTF regression (16 features) |
| `label_mapping_v1_0_0.pkl.gz` | Class labels |
| `feature_columns_v1_0_0.pkl.gz` | Feature names |
| `feature_columns_temporal_v1_1_0.pkl.gz` | Temporal feature names |

### UDF Performance Notes

- First call loads models (~5 seconds)
- Subsequent calls are fast (models cached in warehouse)
- Models reload after warehouse suspend/resume

---

## 8. GitHub Integration & Deployment

### Snowflake Cannot Directly Access GitHub

**What DOESN'T work:**
- `COPY INTO` from GitHub URLs
- External stages pointing to `https://github.com/...`
- Direct file access from Git repositories

### Deployment Pattern (What WORKS)

1. **Store files in GitHub** (code, SQL, seed data)
2. **User downloads/clones locally**
3. **User uploads to Snowflake stages** (via UI or CLI)
4. **User runs SQL scripts** to create objects
5. **User pushes Docker image** (from GHCR to Snowflake)

### GitHub Container Registry (GHCR)

```bash
# Build and push to GHCR
docker build --platform linux/amd64 -t ghcr.io/username/image:tag .
docker login ghcr.io -u USERNAME -p GITHUB_PAT
docker push ghcr.io/username/image:tag

# Make package public in GitHub settings
# Settings → Packages → image → Package settings → Change visibility → Public
```

---

## 9. Data Loading & Stages

### Creating Internal Stages

```sql
-- For CSV files
CREATE STAGE IF NOT EXISTS SEED_STAGE
    FILE_FORMAT = (
        TYPE = CSV 
        COMPRESSION = GZIP 
        FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
        SKIP_HEADER = 1
    );

-- For binary files (models)
CREATE STAGE IF NOT EXISTS MODELS
    DIRECTORY = (ENABLE = TRUE);
```

### Uploading Files

**Via Snowsight UI:**
1. Data → Databases → DB → Schema → Stages → STAGE_NAME
2. Click "+ Files"
3. Select files
4. Click "Upload"

**Via CLI:**
```bash
snow stage copy local_file.csv.gz @DB.SCHEMA.STAGE --overwrite --connection CONN
```

### Loading Data from Stage

```sql
COPY INTO TABLE_NAME
FROM @STAGE_NAME
FILE_FORMAT = (TYPE = CSV COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
```

### RESULT_SCAN Gotcha

**RESULT_SCAN must IMMEDIATELY follow the SHOW command:**

```sql
-- ✅ CORRECT
SHOW IMAGE REPOSITORIES IN SCHEMA IMAGES;
SELECT "repository_url" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" = 'REPO';

-- ❌ WRONG (SELECT in between)
SHOW IMAGE REPOSITORIES IN SCHEMA IMAGES;
SELECT 'Status message';  -- This becomes LAST_QUERY_ID!
SELECT "repository_url" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  -- FAILS
```

---

## 10. Debugging & Troubleshooting

### Service Won't Start

```sql
-- Check compute pool status
SHOW COMPUTE POOLS;  -- Must be ACTIVE or IDLE

-- Check service logs
CALL SYSTEM$GET_SERVICE_LOGS('SERVICE', '0', 'container', 100);

-- Verify image exists
SHOW IMAGES IN IMAGE REPOSITORY DB.SCHEMA.REPO;
```

### Predictions Not Working

```sql
-- Verify UDFs exist
SHOW USER FUNCTIONS IN SCHEMA ML;

-- Verify models uploaded
LIST @ML.MODELS;

-- Test UDF directly
SELECT CLASSIFY_FAILURE_ML(200.0, 45.0, 12.5, 0.1, 5.0, 2.0, 0.5, -0.1, -0.01, 195.0, 44.0);
```

### Service Writes to Wrong Database

1. Check backend code for hardcoded database references
2. Extract from working image, don't use disk files
3. Replace ALL occurrences (grep -c to verify)
4. Test with data: start writer, check row counts in BOTH databases

### Frontend Not Auto-Refreshing

1. Verify frontend was extracted from working image
2. Check MD5 hash matches
3. Check service logs for `/api/predictions/latest` calls
4. If missing → wrong frontend build

---

## 📝 Quick Reference Commands

### Snowflake CLI

```bash
snow connection test --connection NAME
snow sql --connection NAME -q "SELECT 1;"
snow sql --connection NAME -f script.sql
snow spcs image-registry login --connection NAME
snow stage copy file @STAGE --connection NAME
```

### Docker

```bash
docker build --platform linux/amd64 -t tag .
docker pull image:tag
docker tag source:tag target:tag
docker login registry
docker push image:tag
docker create image:tag  # Returns container ID
docker cp container:/path ./local/
docker rm container
```

### Verification

```bash
grep -c "pattern" file        # Count occurrences
md5 file                       # Hash for comparison
sed 's/old/new/g' in > out    # Find/replace
```

---

## 🎯 Golden Rules

1. **Platform:** Always `--platform linux/amd64` for Docker
2. **Source of truth:** Working Docker image, not disk files
3. **Database refs:** Change ALL occurrences, verify with grep
4. **Frontend:** Verify MD5 hash matches working version
5. **Stages:** Don't clone - must recreate and copy contents
6. **UDFs:** Don't clone - must recreate in target database
7. **RESULT_SCAN:** Must immediately follow SHOW command
8. **Image paths:** Database/schema/repo must be lowercase
9. **Independence:** Verify with actual data, not assumptions
10. **Smaller is faster:** XS warehouse/pool often outperforms larger

---

## 📚 Project History

| Project | Focus Area |
|---------|------------|
| Predict1-6 | Initial SPCS exploration |
| PredictiveFailure* | Frontend/backend integration |
| Streaming1 | Real-time data patterns |
| Combo_predict | Combined ML + streaming |
| Combo_predict_clean | Database independence |
| Native_app | Native App packaging attempts |
| ftfp_v1 | Final deployable package |

**Total development time:** ~3 months  
**Key breakthrough:** Extracting from working image, not source files

---

*This document should enable rapid development on future FTFP versions and similar SPCS projects.*

