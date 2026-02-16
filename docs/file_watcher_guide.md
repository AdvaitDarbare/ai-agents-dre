# File Watcher - Event-Driven Validation Guide

## Overview

The File Watcher provides **event-driven, real-time data quality validation** with **human-in-the-loop contract approval**.

## How It Works

### Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ 1. File lands in data/landing/                             │
└───────────────┬─────────────────────────────────────────────┘
                │
                v
┌─────────────────────────────────────────────────────────────┐
│ 2. File Watcher detects new file                           │
└───────────────┬─────────────────────────────────────────────┘
                │
                v
        ┌───────────────┐
        │ Contract      │
        │ exists?       │
        └───┬───────┬───┘
            │       │
     YES ◄──┘       └──► NO
      │                  │
      v                  v
┌───────────┐      ┌──────────────────┐
│ 3. Auto   │      │ 3. Move to       │
│ Validate  │      │ pending_approval/│
└─────┬─────┘      └────────┬─────────┘
      │                     │
      v                     v
┌───────────┐      ┌──────────────────┐
│ 4. Verdict│      │ 4. AI generates  │
│           │      │ proposed contract│
└─────┬─────┘      └────────┬─────────┘
      │                     │
      │                     v
      │            ┌──────────────────┐
      │            │ 5. Notify human  │
      │            │ (API endpoint)   │
      │            └────────┬─────────┘
      │                     │
      │                     v
      │            ┌──────────────────┐
      │            │ 6. Human reviews │
      │            │ in UI, approves  │
      │            └────────┬─────────┘
      │                     │
      │                     v
      │            ┌──────────────────┐
      │            │ 7. POST /approve │
      │            │ → Auto-validate  │
      │            └────────┬─────────┘
      │                     │
      └─────────┬───────────┘
                │
                v
        ┌───────────────┐
        │ PASSED?       │
        └───┬───────┬───┘
            │       │
     YES ◄──┘       └──► NO (BLOCKED)
      │                  │
      v                  v
┌──────────┐      ┌─────────────┐
│ Keep in  │      │ Move to     │
│ landing/ │      │ quarantine/ │
└──────────┘      └─────────────┘
```

## Usage

### Step 1: Start the File Watcher

```bash
# Terminal 1: Start the watcher
python -m src.runners.file_watcher
```

You'll see:
```
╔══════════════════════════════════════════════════════════════════╗
║                   FILE WATCHER - EVENT DRIVEN DRE                ║
╚══════════════════════════════════════════════════════════════════╝

📁 Watching: data/landing
🎯 Mode: Human-in-the-Loop Contract Approval

Flow:
  1. Drop file → Watcher detects
  2. Contract exists? → Auto-validate
  3. New dataset? → Generate proposal → Wait for approval
  4. BLOCKED files → Move to data/quarantine/
  5. PASSED files → Ready for downstream

Press Ctrl+C to stop
──────────────────────────────────────────────────────────────────
```

### Step 2: Drop a File

#### Scenario A: Existing Dataset (Has Contract)

```bash
# Terminal 2: Drop a file for an existing dataset
cp data/test/orders.csv data/landing/orders_2026-02-15.csv
```

**Watcher Output:**
```
======================================================================
🔔 NEW FILE DETECTED
======================================================================
File: orders_2026-02-15.csv
Dataset: orders
Time: 2026-02-15 14:32:10
======================================================================
✅ Contract found: config/expectations/orders.yaml
   Running validation...
   📊 Running validation pipeline...
   ✅ PASSED - File ready for downstream processing
      Quality Score: 94.5
======================================================================
```

**Result:** File validated automatically, ready for use.

---

#### Scenario B: New Dataset (No Contract)

```bash
# Terminal 2: Drop a file for a NEW dataset
cp data/test/new_customers.csv data/landing/customers.csv
```

**Watcher Output:**
```
======================================================================
🔔 NEW FILE DETECTED
======================================================================
File: customers.csv
Dataset: customers
Time: 2026-02-15 14:35:22
======================================================================
⚠️  No contract found for dataset: customers
   This appears to be a NEW dataset
   📁 Moved to pending approval: data/pending_approval/customers.csv
   🤖 Generating contract proposal using AI...
   ✅ Proposal saved to config/proposals/customers.yaml

   🙋 HUMAN ACTION REQUIRED:
   1. Open UI: http://localhost:5173
   2. Review proposed contract for 'customers'
   3. Edit if needed and approve
   4. File will be validated automatically after approval
======================================================================
```

**What Happened:**
1. File moved to `data/pending_approval/`
2. AI generated contract proposal → `config/proposals/customers.yaml`
3. Metadata saved → `config/proposals/customers.meta.json`
4. Waiting for human approval

### Step 3: Review and Approve Contract (UI or API)

#### Option 1: Via API (for testing)

```bash
# Get pending contracts
curl http://localhost:8000/contracts/pending

# Approve contract
curl -X POST http://localhost:8000/contracts/approve \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_name": "customers",
    "approved_yaml": "<paste the YAML content here>"
  }'
```

**API Response:**
```json
{
  "status": "approved",
  "dataset_name": "customers",
  "contract_path": "config/expectations/customers.yaml",
  "validated_files": [
    {
      "file": "customers.csv",
      "status": "PASSED",
      "quality_score": 92.3
    }
  ],
  "message": "Contract approved. Validated 1 pending file(s)."
}
```

**What Happened:**
1. Contract saved to `config/expectations/customers.yaml`
2. Pending file `customers.csv` validated automatically
3. File moved from `pending_approval/` → `landing/` (if PASSED) or `quarantine/` (if BLOCKED)
4. Proposal files cleaned up

#### Option 2: Via UI (Future)

Frontend will show:
- "Pending Contracts" badge in navbar
- Modal with proposed contract YAML
- Inline editor to adjust contract
- "Approve" and "Reject" buttons

### Step 4: Subsequent Files Auto-Validate

```bash
# Drop another customers file
cp data/test/customers_v2.csv data/landing/customers_2026-02-16.csv
```

**Watcher Output:**
```
======================================================================
🔔 NEW FILE DETECTED
======================================================================
File: customers_2026-02-16.csv
Dataset: customers
Time: 2026-02-15 14:40:05
======================================================================
✅ Contract found: config/expectations/customers.yaml
   Running validation...
   📊 Running validation pipeline...
   ✅ PASSED - File ready for downstream processing
      Quality Score: 91.8
======================================================================
```

**Result:** Now validates automatically because contract exists!

---

## Directory Structure

```
data/
├── landing/              # Drop files here
│   ├── orders_2026-02-15.csv
│   └── orders_2026-02-15.csv.verdict.json  # Auto-generated
│
├── pending_approval/     # Files waiting for contract approval
│   └── customers.csv
│
└── quarantine/          # Blocked/rejected files
    └── bad_data.csv

config/
├── expectations/        # Approved contracts
│   ├── orders.yaml
│   └── customers.yaml   # Created after approval
│
└── proposals/          # AI-generated proposals (pending)
    ├── customers.yaml
    └── customers.meta.json
```

---

## API Endpoints

### `GET /contracts/pending`
List all contracts pending approval.

**Response:**
```json
[
  {
    "dataset_name": "customers",
    "proposed_at": "2026-02-15T14:35:22",
    "source_file": "data/pending_approval/customers.csv",
    "row_count": 1500,
    "column_count": 8,
    "proposed_yaml": "...",
    "pending_files": ["customers.csv"],
    "status": "pending_approval"
  }
]
```

### `POST /contracts/approve`
Approve a contract and validate pending files.

**Request:**
```json
{
  "dataset_name": "customers",
  "approved_yaml": "<YAML content>"
}
```

**Response:**
```json
{
  "status": "approved",
  "dataset_name": "customers",
  "contract_path": "config/expectations/customers.yaml",
  "validated_files": [
    {
      "file": "customers.csv",
      "status": "PASSED",
      "quality_score": 92.3
    }
  ]
}
```

### `DELETE /contracts/pending/{dataset_name}`
Reject a proposal and move files to quarantine.

**Response:**
```json
{
  "status": "rejected",
  "dataset_name": "customers",
  "quarantined_files": ["customers.csv"]
}
```

---

## Integration with Airflow

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

with DAG('orders_pipeline') as dag:

    # Step 1: Extract data to landing zone
    extract = BashOperator(
        task_id='extract',
        bash_command='python extract_orders.py > data/landing/orders_{{ ds }}.csv'
    )

    # Step 2: Wait for verdict file (watcher creates this)
    wait_for_verdict = FileSensor(
        task_id='wait_for_verdict',
        filepath='data/landing/orders_{{ ds }}.csv.verdict.json',
        poke_interval=5,
        timeout=300
    )

    # Step 3: Check verdict status
    check_verdict = BashOperator(
        task_id='check_verdict',
        bash_command='''
        status=$(jq -r '.status' data/landing/orders_{{ ds }}.csv.verdict.json)
        if [ "$status" = "BLOCKED" ]; then
          echo "Data quality check FAILED"
          exit 1
        fi
        echo "Data quality check PASSED"
        '''
    )

    # Step 4: Load to warehouse (only if passed)
    load = BashOperator(
        task_id='load',
        bash_command='python load_to_warehouse.py data/landing/orders_{{ ds }}.csv'
    )

    extract >> wait_for_verdict >> check_verdict >> load
```

---

## Benefits

### For Development
- ✅ Drop files → instant validation
- ✅ No manual CLI commands
- ✅ Real-time feedback

### For Production
- ✅ Event-driven automation
- ✅ Human approval for new datasets (safety)
- ✅ Auto-quarantine bad data
- ✅ Integrates with orchestrators

### For Demos
- ✅ "Watch this happen in real-time..."
- ✅ Shows human-in-the-loop clearly
- ✅ Professional production pattern

---

## Troubleshooting

**Q: File not detected?**
- Check file extension (must be .csv, .parquet, .json)
- Ensure file is in `data/landing/`
- Check watcher is running

**Q: Proposal not generated?**
- Check OPENAI_API_KEY is set
- Check file format is valid
- Review watcher terminal for errors

**Q: How to reset a dataset?**
```bash
# Remove approved contract
rm config/expectations/customers.yaml

# Next file will trigger new proposal
cp data/test/customers.csv data/landing/customers_new.csv
```

---

## Next Steps

1. **Frontend Integration:** Build "Pending Contracts" UI component
2. **Slack Notifications:** Alert team when new dataset detected
3. **Webhook Mode:** Trigger from S3 events, not just file watcher
4. **Approval Workflows:** Multi-level approval for CRITICAL datasets
