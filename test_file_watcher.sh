#!/bin/bash

# File Watcher Demo Script
# This script demonstrates the event-driven validation with HITL contract approval

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║           FILE WATCHER DEMO - Event-Driven Validation           ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}📋 This demo will:${NC}"
echo "  1. Start the file watcher"
echo "  2. Drop a file for an EXISTING dataset (orders) → auto-validates"
echo "  3. Drop a file for a NEW dataset (customers) → requires approval"
echo ""
echo -e "${YELLOW}⚠️  Prerequisites:${NC}"
echo "  - Postgres running (docker-compose up -d)"
echo "  - OPENAI_API_KEY set in .env"
echo "  - orders.yaml contract exists in config/expectations/"
echo ""

read -p "Press ENTER to continue or Ctrl+C to cancel..."

# Create test data if needed
echo ""
echo -e "${BLUE}📝 Creating test data...${NC}"

# Create a simple orders CSV for testing
cat > /tmp/test_orders.csv << 'EOF'
order_id,customer_id,amount,status,created_at
1001,C001,45.99,completed,2026-02-15T10:00:00
1002,C002,120.50,pending,2026-02-15T11:30:00
1003,C001,75.00,completed,2026-02-15T12:15:00
EOF

# Create a customers CSV (new dataset)
cat > /tmp/test_customers.csv << 'EOF'
customer_id,name,email,signup_date,country
C001,John Doe,john@example.com,2025-01-15,USA
C002,Jane Smith,jane@example.com,2025-02-20,UK
C003,Bob Johnson,bob@example.com,2026-01-05,Canada
EOF

echo "  ✅ Test CSVs created in /tmp/"

# Start instructions
echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}STEP 1: Start the File Watcher${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Run this command in a NEW terminal window:"
echo ""
echo -e "${GREEN}    python -m src.runners.file_watcher${NC}"
echo ""
read -p "Press ENTER once the watcher is running..."

# Test 1: Existing dataset
echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}STEP 2: Test with EXISTING dataset (orders)${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Dropping orders file into data/landing/..."
sleep 1

cp /tmp/test_orders.csv data/landing/orders_$(date +%Y%m%d_%H%M%S).csv

echo ""
echo -e "${GREEN}✅ File dropped!${NC}"
echo ""
echo "Watch the file watcher terminal - you should see:"
echo "  - File detected"
echo "  - Contract found ✓"
echo "  - Auto-validation runs"
echo "  - Verdict: PASSED/WARNING/BLOCKED"
echo ""

read -p "Press ENTER to continue to the next test..."

# Test 2: New dataset
echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}STEP 3: Test with NEW dataset (customers)${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Dropping customers file (no contract exists)..."
sleep 1

cp /tmp/test_customers.csv data/landing/customers_$(date +%Y%m%d_%H%M%S).csv

echo ""
echo -e "${GREEN}✅ File dropped!${NC}"
echo ""
echo "Watch the file watcher terminal - you should see:"
echo "  - File detected"
echo "  - No contract found ⚠️"
echo "  - File moved to data/pending_approval/"
echo "  - AI generates proposal"
echo "  - HUMAN ACTION REQUIRED message"
echo ""

read -p "Press ENTER to check pending contracts via API..."

# Check pending contracts
echo ""
echo -e "${BLUE}📋 Fetching pending contracts...${NC}"
echo ""
curl -s http://localhost:8000/contracts/pending | python -m json.tool

echo ""
echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}STEP 4: Review and Approve Contract${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "The proposed contract is in: config/proposals/customers.yaml"
echo ""
echo "To approve, you can:"
echo "  A) Use the API (we'll do this next)"
echo "  B) Use the UI (build this later)"
echo ""

read -p "Press ENTER to auto-approve the contract..."

# Auto-approve (in real scenario, human would review first)
echo ""
echo -e "${BLUE}📝 Approving contract...${NC}"

# Get the proposed YAML
if [ -f "config/proposals/customers.yaml" ]; then
  YAML_CONTENT=$(cat config/proposals/customers.yaml)

  # Approve via API
  curl -X POST http://localhost:8000/contracts/approve \
    -H "Content-Type: application/json" \
    -d "{\"dataset_name\": \"customers\", \"approved_yaml\": $(echo "$YAML_CONTENT" | python -c 'import sys, json; print(json.dumps(sys.stdin.read()))')}" \
    | python -m json.tool

  echo ""
  echo -e "${GREEN}✅ Contract approved!${NC}"
  echo ""
  echo "Check:"
  echo "  - config/expectations/customers.yaml (contract saved)"
  echo "  - data/pending_approval/ (should be empty)"
  echo "  - data/landing/ or data/quarantine/ (file moved based on verdict)"
else
  echo -e "${YELLOW}⚠️  Proposal file not found. AI generation may have failed.${NC}"
fi

echo ""
echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}STEP 5: Test Auto-Validation of Approved Dataset${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Now that customers contract is approved, let's drop another customers file..."
echo "This time it should AUTO-VALIDATE without human intervention!"
echo ""

read -p "Press ENTER to drop another customers file..."

cp /tmp/test_customers.csv data/landing/customers_$(date +%Y%m%d_%H%M%S)_v2.csv

echo ""
echo -e "${GREEN}✅ File dropped!${NC}"
echo ""
echo "Watch the file watcher terminal - you should see:"
echo "  - File detected"
echo "  - Contract found ✓ (customers.yaml now exists)"
echo "  - Auto-validation runs (no human needed!)"
echo "  - Verdict: PASSED/WARNING/BLOCKED"
echo ""

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                    DEMO COMPLETE!                                ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Summary:"
echo "  ✅ Event-driven validation working"
echo "  ✅ Auto-validation for existing datasets"
echo "  ✅ HITL contract approval for new datasets"
echo "  ✅ Auto-quarantine for blocked files"
echo ""
echo "Next steps:"
echo "  1. Build frontend UI for pending contract approvals"
echo "  2. Add Slack notifications for new datasets"
echo "  3. Integrate with Airflow/orchestrator"
echo ""

# Cleanup
echo "Cleaning up test files..."
rm -f /tmp/test_orders.csv /tmp/test_customers.csv
echo "Done!"
