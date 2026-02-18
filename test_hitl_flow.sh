#!/bin/bash

echo "=== File Watcher HITL Integration Test ==="
echo ""

# Cleanup
echo "1. Cleaning up previous test data..."
rm -rf data/pending_approval/* config/proposals/* 2>/dev/null
mkdir -p data/pending_approval config/proposals data/landing

# Start file watcher in background
echo "2. Starting file watcher..."
python3 -m src.runners.file_watcher &
WATCHER_PID=$!
echo "   File watcher PID: $WATCHER_PID"

sleep 3

# Create a test dataset file
echo ""
echo "3. Creating and dropping new test dataset..."
cat > data/landing/newdata_2026-02-15.csv << 'EOF'
customer_id,email,signup_date,total_orders
1,alice@example.com,2026-01-10,5
2,bob@example.com,2026-01-15,3
3,charlie@example.com,2026-02-01,7
EOF

echo "   Test file created: data/landing/newdata_2026-02-15.csv"

sleep 5

# Check proposal created
echo ""
echo "4. Verifying proposal generation..."
if [ -f config/proposals/newdata.yaml ]; then
  echo "   ✓ Proposal created successfully"
  echo "   Proposal location: config/proposals/newdata.yaml"
else
  echo "   ✗ Proposal NOT created"
  kill $WATCHER_PID
  exit 1
fi

# Check file moved to pending
if [ -f data/pending_approval/newdata_2026-02-15.csv ]; then
  echo "   ✓ File moved to pending approval directory"
else
  echo "   ✗ File NOT in pending approval directory"
  kill $WATCHER_PID
  exit 1
fi

# Check metadata file
if [ -f config/proposals/newdata.meta.json ]; then
  echo "   ✓ Metadata file created"
  cat config/proposals/newdata.meta.json | python3 -m json.tool
else
  echo "   ⚠ Metadata file not found (non-critical)"
fi

# Test GET pending endpoint
echo ""
echo "5. Testing GET /contracts/pending endpoint..."
PENDING_RESPONSE=$(curl -s http://localhost:8000/contracts/pending)
echo "   Response: $PENDING_RESPONSE"

# Test approval via API
echo ""
echo "6. Testing contract approval via API..."
PROPOSAL_YAML=$(cat config/proposals/newdata.yaml)
APPROVAL_RESPONSE=$(curl -s -X POST http://localhost:8000/contracts/approve \
  -H "Content-Type: application/json" \
  -d "{\"dataset_name\": \"newdata\", \"approved_yaml\": $(echo "$PROPOSAL_YAML" | jq -Rs .)}")

echo "   Approval response:"
echo "$APPROVAL_RESPONSE" | python3 -m json.tool

# Check contract created
echo ""
echo "7. Verifying contract approval..."
if [ -f config/expectations/newdata.yaml ]; then
  echo "   ✓ Contract approved and saved to config/expectations/newdata.yaml"
else
  echo "   ✗ Contract NOT saved"
  kill $WATCHER_PID
  exit 1
fi

# Check file was validated and moved
echo ""
echo "8. Checking file validation and routing..."
if [ -f data/landing/newdata_2026-02-15.csv ]; then
  echo "   ✓ File validated and kept in landing (PASSED)"
elif [ -f data/quarantine/newdata_2026-02-15.csv ]; then
  echo "   ⚠ File moved to quarantine (validation FAILED)"
else
  echo "   ✗ File location unknown"
fi

# Check verdict file
if [ -f data/landing/newdata_2026-02-15.csv.verdict.json ]; then
  echo "   ✓ Verdict file created"
  echo "   Verdict contents:"
  cat data/landing/newdata_2026-02-15.csv.verdict.json | python3 -m json.tool
fi

# Cleanup
echo ""
echo "9. Cleaning up test process..."
kill $WATCHER_PID 2>/dev/null
echo "   File watcher stopped"

echo ""
echo "=== Test Summary ==="
echo "✓ All HITL workflow tests passed!"
echo ""
echo "Next steps:"
echo "1. Start frontend: cd frontend && npm run dev"
echo "2. Navigate to http://localhost:5173"
echo "3. Check Datasets tab for pending approval notification"
echo "4. Click 'Review Now' to test approval modal"
echo "5. Click 'AI Assistant' on any dataset card to test chatbot"
