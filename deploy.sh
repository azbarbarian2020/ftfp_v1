#!/bin/bash
# ============================================================================
# FTFP V1 - Automated Deployment Script
# ============================================================================
# This script deploys a complete FTFP demo environment to any Snowflake account
#
# Usage:
#   ./deploy.sh --account ACCOUNT_LOCATOR --user USERNAME [OPTIONS]
#
# Options:
#   --account     Snowflake account locator (required)
#   --user        Snowflake username (required)
#   --role        Role to use (default: ACCOUNTADMIN)
#   --warehouse   Warehouse name to create (default: FTFP_V1_WH)
#   --database    Database name to create (default: FTFP_V1)
#   --skip-image  Skip Docker image pull/push (if already done)
#   --help        Show this help message
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ROLE="ACCOUNTADMIN"
WAREHOUSE="FTFP_V1_WH"
DATABASE="FTFP_V1"
GHCR_IMAGE="ghcr.io/azbarbarian2020/ftfp_v1:v1"
SKIP_IMAGE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --account)
            ACCOUNT="$2"
            shift 2
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --role)
            ROLE="$2"
            shift 2
            ;;
        --warehouse)
            WAREHOUSE="$2"
            shift 2
            ;;
        --database)
            DATABASE="$2"
            shift 2
            ;;
        --skip-image)
            SKIP_IMAGE=true
            shift
            ;;
        --help)
            echo "FTFP V1 - Automated Deployment Script"
            echo ""
            echo "Usage: ./deploy.sh --account ACCOUNT --user USER [OPTIONS]"
            echo ""
            echo "Required:"
            echo "  --account     Snowflake account locator"
            echo "  --user        Snowflake username"
            echo ""
            echo "Optional:"
            echo "  --role        Role to use (default: ACCOUNTADMIN)"
            echo "  --warehouse   Warehouse name (default: FTFP_V1_WH)"
            echo "  --database    Database name (default: FTFP_V1)"
            echo "  --skip-image  Skip Docker image deployment"
            echo "  --help        Show this help"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$ACCOUNT" ]; then
    echo -e "${RED}Error: --account is required${NC}"
    echo "Usage: ./deploy.sh --account ACCOUNT_LOCATOR --user USERNAME"
    exit 1
fi

if [ -z "$USER" ]; then
    echo -e "${RED}Error: --user is required${NC}"
    echo "Usage: ./deploy.sh --account ACCOUNT_LOCATOR --user USERNAME"
    exit 1
fi

# Derive registry URL from account
# Account format is typically: ORGNAME-ACCOUNTNAME or just ACCOUNTLOCATOR
REGISTRY_HOST="${ACCOUNT}.snowflakecomputing.com"
if [[ "$ACCOUNT" != *"."* ]]; then
    # If no dot, assume it's an org-account format, convert to registry format
    REGISTRY_HOST="${ACCOUNT}.snowflakecomputing.com"
fi

echo ""
echo -e "${BLUE}============================================================================${NC}"
echo -e "${BLUE}           FTFP V1 - Fleet Telemetry Failure Prediction                    ${NC}"
echo -e "${BLUE}                    Automated Deployment Script                             ${NC}"
echo -e "${BLUE}============================================================================${NC}"
echo ""
echo -e "Account:    ${GREEN}$ACCOUNT${NC}"
echo -e "User:       ${GREEN}$USER${NC}"
echo -e "Role:       ${GREEN}$ROLE${NC}"
echo -e "Database:   ${GREEN}$DATABASE${NC}"
echo -e "Warehouse:  ${GREEN}$WAREHOUSE${NC}"
echo ""

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

# Check for Snowflake CLI
if ! command -v snow &> /dev/null; then
    echo -e "${RED}Error: Snowflake CLI (snow) not found${NC}"
    echo "Install from: https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/installation/installation"
    exit 1
fi
echo -e "  ✅ Snowflake CLI found"

# Check for Docker (only if not skipping image)
if [ "$SKIP_IMAGE" = false ]; then
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker not found${NC}"
        echo "Install Docker or use --skip-image if image is already deployed"
        exit 1
    fi
    echo -e "  ✅ Docker found"
fi

echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# ============================================================================
# STEP 1: Run SQL deployment script
# ============================================================================
echo -e "${YELLOW}Step 1: Running SQL deployment script...${NC}"
echo ""

# Create a temporary SQL file with variable substitutions
TEMP_SQL=$(mktemp)
cat > "$TEMP_SQL" << EOF
-- Set deployment variables
SET DB_NAME = '${DATABASE}';
SET WH_NAME = '${WAREHOUSE}';
SET POOL_NAME = '${DATABASE}_POOL';

EOF

# Append the main deployment SQL
cat "$SCRIPT_DIR/snowflake/DEPLOY_FTFP_V1.sql" >> "$TEMP_SQL"

# Run the SQL script
snow sql -f "$TEMP_SQL" \
    --account "$ACCOUNT" \
    --user "$USER" \
    --role "$ROLE" \
    --warehouse "$WAREHOUSE" 2>&1 || {
    echo -e "${RED}SQL deployment failed. Check the error above.${NC}"
    rm -f "$TEMP_SQL"
    exit 1
}

rm -f "$TEMP_SQL"
echo -e "${GREEN}  ✅ SQL deployment complete${NC}"
echo ""

# ============================================================================
# STEP 2: Deploy Docker image (if not skipped)
# ============================================================================
if [ "$SKIP_IMAGE" = false ]; then
    echo -e "${YELLOW}Step 2: Deploying Docker image...${NC}"
    echo ""
    
    # Get the image repository URL from Snowflake
    REPO_URL=$(snow sql -q "SHOW IMAGE REPOSITORIES IN SCHEMA ${DATABASE}.IMAGES; SELECT \"repository_url\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE \"name\" = 'FTFP_REPO';" \
        --account "$ACCOUNT" \
        --user "$USER" \
        --role "$ROLE" \
        --format json 2>/dev/null | jq -r '.[0].repository_url // empty')
    
    if [ -z "$REPO_URL" ]; then
        echo -e "${RED}Could not get image repository URL${NC}"
        echo "You may need to manually push the image."
        echo ""
        echo "Run these commands manually:"
        echo "  docker pull $GHCR_IMAGE"
        echo "  docker tag $GHCR_IMAGE <YOUR_REPO_URL>/ftfp_v1:v1"
        echo "  docker push <YOUR_REPO_URL>/ftfp_v1:v1"
    else
        echo -e "  Repository URL: ${GREEN}$REPO_URL${NC}"
        
        # Pull from GHCR
        echo -e "  Pulling image from GitHub Container Registry..."
        docker pull "$GHCR_IMAGE" || {
            echo -e "${RED}Failed to pull image from GHCR${NC}"
            exit 1
        }
        
        # Tag for Snowflake registry
        TARGET_IMAGE="$REPO_URL/ftfp_v1:v1"
        echo -e "  Tagging image for Snowflake registry..."
        docker tag "$GHCR_IMAGE" "$TARGET_IMAGE"
        
        # Login to Snowflake registry
        echo -e "  Logging into Snowflake registry..."
        echo -e "${YELLOW}  (You may be prompted for your Snowflake password)${NC}"
        docker login "$REPO_URL" -u "$USER" || {
            echo -e "${RED}Failed to login to Snowflake registry${NC}"
            echo "Make sure your password/MFA is correct"
            exit 1
        }
        
        # Push to Snowflake registry
        echo -e "  Pushing image to Snowflake registry (this may take a few minutes)..."
        docker push "$TARGET_IMAGE" || {
            echo -e "${RED}Failed to push image${NC}"
            exit 1
        }
        
        echo -e "${GREEN}  ✅ Docker image deployed${NC}"
    fi
else
    echo -e "${YELLOW}Step 2: Skipping Docker image deployment (--skip-image)${NC}"
fi
echo ""

# ============================================================================
# STEP 3: Deploy SPCS service
# ============================================================================
echo -e "${YELLOW}Step 3: Deploying SPCS service...${NC}"
echo ""

snow sql -q "CALL ${DATABASE}.FTFP.DEPLOY_SERVICE();" \
    --account "$ACCOUNT" \
    --user "$USER" \
    --role "$ROLE" \
    --warehouse "$WAREHOUSE" 2>&1

echo -e "${GREEN}  ✅ Service deployment initiated${NC}"
echo ""

# ============================================================================
# STEP 4: Wait for service and get URL
# ============================================================================
echo -e "${YELLOW}Step 4: Waiting for service to start (this may take 2-3 minutes)...${NC}"
echo ""

for i in {1..20}; do
    sleep 15
    
    # Check service status
    STATUS=$(snow sql -q "SELECT \"status\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) AFTER (STATEMENT => 'SHOW SERVICES IN SCHEMA ${DATABASE}.SERVICE');" \
        --account "$ACCOUNT" \
        --user "$USER" \
        --role "$ROLE" \
        --format json 2>/dev/null | jq -r '.[0].status // "UNKNOWN"')
    
    echo -e "  Status: $STATUS"
    
    if [ "$STATUS" = "READY" ]; then
        break
    fi
done

# Get endpoint URL
echo ""
echo -e "${YELLOW}Getting service endpoint...${NC}"

ENDPOINT=$(snow sql -q "SHOW ENDPOINTS IN SERVICE ${DATABASE}.SERVICE.FTFP_SERVICE; SELECT \"ingress_url\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));" \
    --account "$ACCOUNT" \
    --user "$USER" \
    --role "$ROLE" \
    --format json 2>/dev/null | jq -r '.[0].ingress_url // empty')

echo ""
echo -e "${BLUE}============================================================================${NC}"
echo -e "${GREEN}                    DEPLOYMENT COMPLETE! 🎉                                ${NC}"
echo -e "${BLUE}============================================================================${NC}"
echo ""

if [ -n "$ENDPOINT" ]; then
    echo -e "Application URL: ${GREEN}https://$ENDPOINT${NC}"
else
    echo -e "${YELLOW}Service is still starting. Get the URL with:${NC}"
    echo -e "  SHOW ENDPOINTS IN SERVICE ${DATABASE}.SERVICE.FTFP_SERVICE;"
fi

echo ""
echo -e "Useful commands:"
echo -e "  ${BLUE}Check status:${NC}  CALL ${DATABASE}.FTFP.CHECK_SERVICE_STATUS();"
echo -e "  ${BLUE}View logs:${NC}     CALL ${DATABASE}.FTFP.GET_SERVICE_LOGS();"
echo -e "  ${BLUE}Refresh ML:${NC}    CALL ${DATABASE}.FTFP.REFRESH_PREDICTIONS();"
echo ""
echo -e "${BLUE}============================================================================${NC}"

