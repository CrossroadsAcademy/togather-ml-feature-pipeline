#!/bin/bash

# Feast Setup Script

# Registers feature definitions and optionally materializes features.


set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FEAST_REPO="${SCRIPT_DIR}/../feast_repo"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Feast Setup Script ===${NC}"
echo "Feast repository: ${FEAST_REPO}"

# Check feast is installed
if ! command -v feast &> /dev/null; then
    echo -e "${RED}Error: feast is not installed${NC}"
    echo "Install with: pip install feast[redis]"
    exit 1
fi

# Set default environment variables if not set
export REDIS_HOST="${REDIS_HOST:-localhost}"
export REDIS_PORT="${REDIS_PORT:-6379}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin123}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:9000}"

echo -e "${YELLOW}Environment:${NC}"
echo "  REDIS_HOST: ${REDIS_HOST}"
echo "  REDIS_PORT: ${REDIS_PORT}"
echo "  AWS_ENDPOINT_URL: ${AWS_ENDPOINT_URL}"
echo ""

# Navigate to feast repo
cd "${FEAST_REPO}"

#Show what will be applied
echo -e "${YELLOW}Step 1: Planning changes...${NC}"
feast plan

#Apply feature definitions
echo ""
echo -e "${YELLOW}Step 2: Applying feature definitions...${NC}"
feast apply

echo -e "${GREEN} Feature definitions registered successfully${NC}"

#Optionally materialize
if [[ "$1" == "--materialize" ]]; then
    echo ""
    echo -e "${YELLOW}Step 3: Materializing features to online store...${NC}"

    # Materialize last 7 days
    END_DATE=$(date -u +"%Y-%m-%dT%H:%M:%S")
    START_DATE=$(date -u -v-7d +"%Y-%m-%dT%H:%M:%S" 2>/dev/null || date -u -d "7 days ago" +"%Y-%m-%dT%H:%M:%S")

    echo "  Start: ${START_DATE}"
    echo "  End: ${END_DATE}"

    feast materialize "${START_DATE}" "${END_DATE}"

    echo -e "${GREEN}✓ Features materialized to online store${NC}"
fi

#List registered features
echo ""
echo -e "${YELLOW}Registered Features:${NC}"
feast feature-views list

echo ""
echo -e "${GREEN}=== Feast setup complete ===${NC}"
