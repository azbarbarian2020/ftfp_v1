# FTFP V1 - Docker Image for Snowpark Container Services (SPCS)
# VERSION: v1
# 
# BUILD INSTRUCTIONS:
#   Build from the repository root directory:
#   docker build --platform linux/amd64 -f docker/Dockerfile -t ftfp_v1:v1 .
#
# This image includes:
#   - FastAPI backend with Snowflake connectivity
#   - Pre-built React frontend
#
# Note: ML models are loaded by Snowflake Python UDFs from @FTFP_V1.ML.MODELS stage,
#       not from this container.

FROM python:3.10-slim

WORKDIR /app

# Install curl for health check
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install backend dependencies
COPY docker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend/*.py ./

# Copy frontend build for static serving
COPY frontend/build/ ./frontend/build/

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/api/health || exit 1

# Start FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
