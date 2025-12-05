# FTFP V1 - Fleet Telemetry Failure Prediction

A complete Snowflake demo showcasing **Snowpark Container Services (SPCS)** with a React frontend, FastAPI backend, and real-time ML predictions using XGBoost models.

![FTFP Dashboard](https://img.shields.io/badge/Snowflake-SPCS-blue) ![Python](https://img.shields.io/badge/Python-3.10-green) ![React](https://img.shields.io/badge/React-18-61DAFB)

## 🚀 Quick Start

### Prerequisites

- Snowflake account (**Enterprise Edition** or higher for SPCS)
- `ACCOUNTADMIN` role access
- Docker installed (for image deployment)
- Snowflake CLI (`snow`) installed ([Installation Guide](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/installation/installation))

### One-Command Deployment

```bash
# Clone the repository
git clone https://github.com/azbarbarian2020/ftfp_v1.git
cd ftfp_v1

# Run automated deployment
./deploy.sh --account YOUR_ACCOUNT_LOCATOR --user YOUR_USERNAME
```

The script will:
1. ✅ Create database `FTFP_V1` with all schemas
2. ✅ Create warehouse and compute pool
3. ✅ Pull Docker image from GitHub Container Registry
4. ✅ Push image to your Snowflake registry
5. ✅ Load seed data (100K+ telemetry records)
6. ✅ Deploy ML models and UDFs
7. ✅ Start the SPCS service
8. ✅ Output the application URL

---

## 📋 What's Included

### Application Features

- **Real-time Fleet Monitoring**: Track 10 trucks with live telemetry
- **ML Failure Prediction**: XGBoost models predict failures before they happen
- **Interactive Dashboard**: React-based UI with charts and controls
- **Failure Simulation**: Trigger engine, transmission, or electrical failures
- **Time Travel**: Fast-forward simulation to see predictions evolve

### Technical Components

| Component | Technology | Description |
|-----------|------------|-------------|
| Frontend | React 18 | Interactive dashboard with Recharts |
| Backend | FastAPI | High-performance Python API |
| ML Models | XGBoost | Classifier + regression models |
| Database | Snowflake | Tables, views, UDFs |
| Container | SPCS | Snowpark Container Services |

### ML Models

- **Classifier**: Predicts failure type (ENGINE, TRANSMISSION, ELECTRICAL, NORMAL)
- **TTF Regression**: Predicts hours to failure (basic 11-feature model)
- **TTF Temporal**: Enhanced prediction with 16 temporal features

---

## 🔧 Manual Deployment

If you prefer step-by-step control:

### Step 1: Create Image Repository

```sql
USE ROLE ACCOUNTADMIN;

-- Run the SQL deployment script
-- This creates everything except pushes the Docker image
!source snowflake/DEPLOY_FTFP_V1.sql
```

### Step 2: Push Docker Image

```bash
# Get your Snowflake registry URL (from Step 1 output)
REGISTRY_URL="sfsenorthamerica-YOUR_ACCOUNT.registry.snowflakecomputing.com/ftfp_v1/images/ftfp_repo"

# Pull from GitHub Container Registry
docker pull ghcr.io/azbarbarian2020/ftfp_v1:v1

# Tag for your registry
docker tag ghcr.io/azbarbarian2020/ftfp_v1:v1 $REGISTRY_URL/ftfp_v1:v1

# Login to your Snowflake registry
docker login $REGISTRY_URL

# Push
docker push $REGISTRY_URL/ftfp_v1:v1
```

### Step 3: Deploy Service

```sql
-- After image is pushed, deploy the service
CALL FTFP_V1.FTFP.DEPLOY_SERVICE();

-- Check status (wait 2-3 minutes)
CALL FTFP_V1.FTFP.CHECK_SERVICE_STATUS();
```

---

## 📁 Repository Structure

```
ftfp_v1/
├── deploy.sh                    # Automated deployment script
├── docker/
│   ├── Dockerfile              # Container build file
│   └── requirements.txt        # Python dependencies
├── backend/
│   ├── main.py                 # FastAPI application
│   └── models/                 # XGBoost ML models (.pkl.gz)
├── frontend/
│   └── build/                  # Pre-built React application
├── seed_data/
│   ├── NORMAL_SEED_FULL.csv.gz # Normal telemetry patterns
│   ├── ENGINE_FAILURE_SEED.csv.gz
│   ├── TRANSMISSION_FAILURE_SEED.csv.gz
│   └── ELECTRICAL_FAILURE_SEED.csv.gz
├── snowflake/
│   ├── DEPLOY_FTFP_V1.sql     # Complete SQL deployment
│   └── service_spec.yaml       # SPCS service specification
└── README.md
```

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
4. Watch the truck's telemetry change and predictions update

### Understanding Predictions

| Color | Meaning |
|-------|---------|
| 🟢 Green | Normal operation |
| 🟡 Yellow | Potential issue detected |
| 🔴 Red | Failure predicted - TTF shown in hours |

---

## 🛠 Troubleshooting

### Service Won't Start

```sql
-- Check compute pool status
SHOW COMPUTE POOLS LIKE 'FTFP%';

-- Check service logs
CALL FTFP_V1.FTFP.GET_SERVICE_LOGS();

-- Verify image exists
SHOW IMAGES IN IMAGE REPOSITORY FTFP_V1.IMAGES.FTFP_REPO;
```

### Image Push Fails

```bash
# Ensure you're logged into Snowflake registry
docker logout  # Clear any cached credentials
docker login sfsenorthamerica-YOUR_ACCOUNT.registry.snowflakecomputing.com

# Use your Snowflake username and password (or MFA token)
```

### Predictions Not Updating

```sql
-- Check if ML UDFs exist
SHOW USER FUNCTIONS IN SCHEMA FTFP_V1.ML;

-- Manually refresh predictions
CALL FTFP_V1.FTFP.REFRESH_PREDICTIONS();
```

---

## 📊 Database Objects Created

```
FTFP_V1 (Database)
├── FTFP (Schema)
│   ├── TELEMETRY, NORMAL_SEED, *_FAILURE_SEED (Tables)
│   ├── PREDICTION_CACHE, STREAM_STATE (Tables)
│   └── TELEMETRY_5MIN_AGG, FEATURE_ENGINEERING_VIEW (Views)
├── ML (Schema)
│   ├── MODELS (Stage with .pkl.gz files)
│   └── CLASSIFY_FAILURE_ML, PREDICT_TTF_* (UDFs)
├── IMAGES (Schema)
│   └── FTFP_REPO (Image Repository)
└── SERVICE (Schema)
    └── FTFP_SERVICE (SPCS Service)

FTFP_V1_WH (Warehouse - X-Small)
FTFP_V1_POOL (Compute Pool - CPU_X64_XS)
```

---

## 🧹 Cleanup

To remove all objects created by this demo:

```sql
-- Remove service first
DROP SERVICE IF EXISTS FTFP_V1.SERVICE.FTFP_SERVICE;

-- Remove compute pool
DROP COMPUTE POOL IF EXISTS FTFP_V1_POOL;

-- Remove database (includes all schemas, tables, etc.)
DROP DATABASE IF EXISTS FTFP_V1;

-- Remove warehouse
DROP WAREHOUSE IF EXISTS FTFP_V1_WH;

-- Remove API integration
DROP INTEGRATION IF EXISTS FTFP_GITHUB_INTEGRATION;
```

---

## 📝 License

This project is provided as a demo/sample for Snowflake capabilities. Use at your own discretion.

## 🤝 Contributing

Issues and pull requests welcome! Please ensure any changes maintain compatibility with Snowflake Enterprise Edition.

---

**Built with ❄️ Snowflake + 🐍 Python + ⚛️ React**

