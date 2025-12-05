# FTFP V1 - Fleet Telemetry Failure Prediction

A complete Snowflake demo showcasing **Snowpark Container Services (SPCS)** with a React frontend, FastAPI backend, and real-time ML predictions using XGBoost models.

![FTFP Dashboard](https://img.shields.io/badge/Snowflake-SPCS-blue) ![Python](https://img.shields.io/badge/Python-3.10-green) ![React](https://img.shields.io/badge/React-18-61DAFB)

---

## 📋 Prerequisites

- Snowflake account (**Enterprise Edition** or higher for SPCS)
- `ACCOUNTADMIN` role access
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/installation/installation) installed and configured
- Docker Desktop installed

---

## 🚀 Deployment Guide (4 Steps)

### Step 1: Run Infrastructure SQL

Open a Snowflake worksheet (or use `snow sql`) and run:

```
snowflake/01_INFRASTRUCTURE.sql
```

This creates:
- ✅ Database `FTFP_V1` with schemas (FTFP, ML, IMAGES, SERVICE)
- ✅ Tables for telemetry and seed data
- ✅ Warehouse `FTFP_V1_WH`
- ✅ Compute pool `FTFP_V1_POOL`
- ✅ Image repository `FTFP_REPO`
- ✅ Internal stages for data and models

**⚠️ IMPORTANT:** Save the `DOCKER_IMAGE_PATH` output - you'll need it in Step 4.

---

### Step 2: Upload Seed Data & ML Models

First, download the files to your computer, then upload them to Snowflake using **either** the CLI or the Snowsight UI.

#### 2a. Download the repository

**Option A - Git clone:**
```bash
git clone https://github.com/azbarbarian2020/ftfp_v1.git
```

**Option B - Download ZIP:**
1. Go to https://github.com/azbarbarian2020/ftfp_v1
2. Click green **Code** button → **Download ZIP**
3. Extract the ZIP file

The files you need are in the `seed_data/` folder.

---

#### 2b. Upload files to Snowflake

Choose **ONE** of these methods:

<details>
<summary><b>📁 Option A: Snowsight UI (No CLI required)</b></summary>

**Upload CSV files to SEED_STAGE:**
1. In Snowsight, go to **Data** → **Databases** → **FTFP_V1** → **FTFP** → **Stages**
2. Click on **SEED_STAGE**
3. Click **+ Files** button (top right)
4. Select these 4 files from your `seed_data/` folder:
   - `NORMAL_SEED_FULL.csv.gz`
   - `ENGINE_FAILURE_SEED.csv.gz`
   - `TRANSMISSION_FAILURE_SEED.csv.gz`
   - `ELECTRICAL_FAILURE_SEED.csv.gz`
5. Click **Upload**

**Upload ML model files to MODELS:**
1. Go to **Data** → **Databases** → **FTFP_V1** → **ML** → **Stages**
2. Click on **MODELS**
3. Click **+ Files** button
4. Select these 6 files from your `seed_data/` folder:
   - `classifier_v1_0_0.pkl.gz`
   - `regression_v1_0_0.pkl.gz`
   - `regression_temporal_v1_1_0.pkl.gz`
   - `label_mapping_v1_0_0.pkl.gz`
   - `feature_columns_v1_0_0.pkl.gz`
   - `feature_columns_temporal_v1_1_0.pkl.gz`
5. Click **Upload**

</details>

<details>
<summary><b>💻 Option B: Snowflake CLI</b></summary>

Run these commands from inside the `ftfp_v1/` folder:

```bash
# Upload seed data CSV files
snow stage copy seed_data/NORMAL_SEED_FULL.csv.gz @FTFP_V1.FTFP.SEED_STAGE --overwrite --connection YOUR_CONNECTION
snow stage copy seed_data/ENGINE_FAILURE_SEED.csv.gz @FTFP_V1.FTFP.SEED_STAGE --overwrite --connection YOUR_CONNECTION
snow stage copy seed_data/TRANSMISSION_FAILURE_SEED.csv.gz @FTFP_V1.FTFP.SEED_STAGE --overwrite --connection YOUR_CONNECTION
snow stage copy seed_data/ELECTRICAL_FAILURE_SEED.csv.gz @FTFP_V1.FTFP.SEED_STAGE --overwrite --connection YOUR_CONNECTION

# Upload ML model files
snow stage copy seed_data/classifier_v1_0_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONNECTION
snow stage copy seed_data/regression_v1_0_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONNECTION
snow stage copy seed_data/regression_temporal_v1_1_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONNECTION
snow stage copy seed_data/label_mapping_v1_0_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONNECTION
snow stage copy seed_data/feature_columns_v1_0_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONNECTION
snow stage copy seed_data/feature_columns_temporal_v1_1_0.pkl.gz @FTFP_V1.ML.MODELS --overwrite --connection YOUR_CONNECTION
```

> Replace `YOUR_CONNECTION` with your CLI connection name (run `snow connection list` to see options).

</details>

---

#### 2c. Verify uploads

Run in a Snowflake worksheet:
```sql
LIST @FTFP_V1.FTFP.SEED_STAGE;  -- Should show 4 CSV files
LIST @FTFP_V1.ML.MODELS;        -- Should show 6 PKL files
```

---

### Step 3: Load Data & Create ML UDFs

Run the second SQL script:

```
snowflake/02_LOAD_DATA_AND_DEPLOY.sql
```

This:
- ✅ Loads seed data from staged CSV files into tables
- ✅ Creates XGBoost-powered ML UDFs (classifier + regression)
- ✅ Creates feature engineering views
- ✅ Creates stored procedures for service management

---

### Step 4: Deploy Docker Image & Start Service

#### 4a. Pull, Tag, and Push the Docker Image

**First, get your repository URL from Step 1 output.** It looks like:
```
mrpygxt-igb64472.registry.snowflakecomputing.com/ftfp_v1/images/ftfp_repo
```

**Now run these 4 commands** (substituting YOUR repository URL):

```bash
# 1. Pull the pre-built image from GitHub
docker pull ghcr.io/azbarbarian2020/ftfp_v1:v1

# 2. Tag it for YOUR Snowflake registry
#    Add /ftfp_v1:v1 to the END of your repository URL
docker tag ghcr.io/azbarbarian2020/ftfp_v1:v1 \
  mrpygxt-igb64472.registry.snowflakecomputing.com/ftfp_v1/images/ftfp_repo/ftfp_v1:v1

# 3. Login to Snowflake registry (JUST the host part, before the first /)
docker login mrpygxt-igb64472.registry.snowflakecomputing.com
#    Username: Your Snowflake username
#    Password: Your Snowflake password

# 4. Push the image
docker push mrpygxt-igb64472.registry.snowflakecomputing.com/ftfp_v1/images/ftfp_repo/ftfp_v1:v1
```

**Breaking down the image path:**
```
mrpygxt-igb64472.registry.snowflakecomputing.com/ftfp_v1/images/ftfp_repo/ftfp_v1:v1
└─────────────── registry host ───────────────┘└──── repository path ────┘└─ image:tag ─┘
        (use this for docker login)              (from Step 1 output)       (always add this)
```

#### 4b. Deploy the Service

In Snowflake:
```sql
-- Deploy the SPCS service
CALL FTFP_V1.FTFP.DEPLOY_SERVICE();

-- Wait 2-3 minutes, then check status
CALL FTFP_V1.FTFP.CHECK_SERVICE_STATUS();
```

The `CHECK_SERVICE_STATUS` call returns the application URL when the service is `RUNNING`.

---

## ✅ Deployment Complete!

Open the URL from `CHECK_SERVICE_STATUS` to access the Fleet Telemetry dashboard.

---

## 🎮 Using the Application

### Dashboard Controls

| Control | Action |
|---------|--------|
| **Start/Stop** | Begin/pause telemetry streaming |
| **Fast Forward** | Jump ahead 1-8 hours of simulation |
| **Refresh Predictions** | Update ML predictions from latest data |
| **Reset** | Clear all data and start fresh |

### Triggering Failures

1. Select a truck from the dropdown
2. Choose failure type (Engine, Transmission, Electrical)
3. Click "Activate Failure"
4. Watch telemetry change and predictions update in real-time

### Understanding Predictions

| Color | Meaning |
|-------|---------|
| 🟢 Green | Normal operation |
| 🟡 Yellow | Potential issue detected |
| 🔴 Red | Failure predicted - TTF shown in hours |

---

## 📁 Repository Structure

```
ftfp_v1/
├── README.md                          # This file
├── snowflake/
│   ├── 01_INFRASTRUCTURE.sql          # Step 1: Create database, stages, compute pool
│   ├── 02_LOAD_DATA_AND_DEPLOY.sql    # Step 3: Load data, create ML UDFs, views
│   └── service_spec.yaml              # SPCS service specification
├── seed_data/                         # ⬆️ Files to upload to Snowflake stages
│   ├── NORMAL_SEED_FULL.csv.gz        # → @FTFP_V1.FTFP.SEED_STAGE (telemetry data)
│   ├── ENGINE_FAILURE_SEED.csv.gz     # → @FTFP_V1.FTFP.SEED_STAGE
│   ├── TRANSMISSION_FAILURE_SEED.csv.gz
│   ├── ELECTRICAL_FAILURE_SEED.csv.gz
│   ├── classifier_v1_0_0.pkl.gz       # → @FTFP_V1.ML.MODELS (XGBoost models)
│   ├── regression_v1_0_0.pkl.gz       # → @FTFP_V1.ML.MODELS
│   ├── regression_temporal_v1_1_0.pkl.gz
│   ├── label_mapping_v1_0_0.pkl.gz    # → @FTFP_V1.ML.MODELS
│   ├── feature_columns_v1_0_0.pkl.gz  # → @FTFP_V1.ML.MODELS
│   └── feature_columns_temporal_v1_1_0.pkl.gz
├── backend/
│   └── main.py                        # FastAPI application (for Docker image)
├── frontend/
│   └── build/                         # Pre-built React application
└── docker/
    ├── Dockerfile                     # Container build file
    └── requirements.txt               # Python dependencies
```

**Note:** All ML models are in `seed_data/` and get uploaded to the `@FTFP_V1.ML.MODELS` Snowflake stage. The Snowflake Python UDFs load models from this stage. The Docker container does NOT contain the models.

---

## 📊 Database Objects Created

```
FTFP_V1 (Database)
├── FTFP (Schema)
│   ├── TELEMETRY                  # Live telemetry data
│   ├── NORMAL_SEED               # Normal operating patterns
│   ├── ENGINE_FAILURE_SEED       # Engine failure patterns
│   ├── TRANSMISSION_FAILURE_SEED # Transmission failure patterns
│   ├── ELECTRICAL_FAILURE_SEED   # Electrical failure patterns
│   ├── PREDICTION_CACHE          # Cached ML predictions
│   ├── STREAM_STATE              # Streaming state management
│   ├── SEED_DATA (Stage)         # Staged CSV files
│   └── FEATURE_ENGINEERING_VIEW  # ML feature views
├── ML (Schema)
│   ├── MODELS (Stage)            # XGBoost model files
│   ├── CLASSIFY_FAILURE_ML()     # Failure type classifier
│   ├── PREDICT_TTF_ML()          # Time-to-failure regression
│   └── PREDICT_TTF_TEMPORAL()    # Enhanced temporal TTF
├── IMAGES (Schema)
│   └── FTFP_REPO                 # Docker image repository
└── SERVICE (Schema)
    └── FTFP_SERVICE              # Running SPCS service

FTFP_V1_WH (Warehouse - X-Small)
FTFP_V1_POOL (Compute Pool - CPU_X64_XS)
```

---

## 🛠 Troubleshooting

### Service Won't Start

```sql
-- Check compute pool status (must be ACTIVE or IDLE)
SHOW COMPUTE POOLS LIKE 'FTFP%';

-- Check service logs
CALL FTFP_V1.FTFP.GET_SERVICE_LOGS();

-- Verify image was pushed
SHOW IMAGES IN IMAGE REPOSITORY FTFP_V1.IMAGES.FTFP_REPO;
```

### Docker Login Fails

```bash
# Login to JUST the registry host (everything before the first /)
docker login mrpygxt-igb64472.registry.snowflakecomputing.com

# NOT the full repository path:
# WRONG: docker login mrpygxt-igb64472.registry.snowflakecomputing.com/ftfp_v1/images/ftfp_repo
```

### ML Predictions Return NULL

```sql
-- Verify models are uploaded
LIST @FTFP_V1.ML.MODELS;

-- Should show 6 .pkl.gz files
```

### "Repository not found" on Docker Push

Ensure the image repository exists:
```sql
SHOW IMAGE REPOSITORIES IN SCHEMA FTFP_V1.IMAGES;
```

---

## 🧹 Cleanup

To remove all objects:

```sql
-- Stop service first
DROP SERVICE IF EXISTS FTFP_V1.SERVICE.FTFP_SERVICE;

-- Remove compute pool  
DROP COMPUTE POOL IF EXISTS FTFP_V1_POOL;

-- Remove database (cascades all schemas/tables/views)
DROP DATABASE IF EXISTS FTFP_V1;

-- Remove warehouse
DROP WAREHOUSE IF EXISTS FTFP_V1_WH;
```

---

## 📝 License

This project is provided as a demo/sample for Snowflake capabilities.

---

**Built with ❄️ Snowflake + 🐍 Python + ⚛️ React**
