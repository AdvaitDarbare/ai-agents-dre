# 🚀 Quick Start Guide

Get up and running with the Autonomous DRE System in 3 minutes!

## Prerequisites

- Python 3.8+
- pip

## Step 1: Install Dependencies (30 seconds)

```bash
pip install -r requirements.txt
```

This installs:
- `pandas` - Data manipulation
- `numpy` - Numerical operations
- `scipy` - Statistical functions
- `pyyaml` - YAML config parsing
- `streamlit` - (Future) Interactive UI

## Step 2: Generate Test Data (10 seconds)

```bash
python3 generate_test_data.py
```

This creates 3 test scenarios:
- ✅ `data/transactions_clean.csv` - Should PASS with warnings
- ❌ `data/transactions_bad.csv` - Should FAIL (missing critical column)
- ⚠️ `data/transactions_warnings.csv` - Should PASS with warnings

## Step 3: Run the Pipeline (5 seconds)

### Test 1: Clean Data (Expected: PASS WITH WARNINGS)

```bash
python3 main.py data/transactions_clean.csv transactions
```

**Expected Output:**
```
🕵️ MONITOR AGENT: Starting Analysis
✓ File is fresh (0.06 MB)
✓ Loaded 1000 rows (6 columns)
✓ Schema validation: PASS_WITH_WARNINGS
✓ Profiled 3 numeric columns
✓ Drift check: PASS

⚠️ STATUS: PASS WITH WARNINGS
Found 1 warnings (non-critical):
  1. Column 'category': constraint (expected: null% <= 5%, actual: null% = 5.5%)

🔒 Quarantined 58 rows (outliers)
```

### Test 2: Bad Data (Expected: FAIL)

```bash
python3 main.py data/transactions_bad.csv transactions
```

**Expected Output:**
```
❌ STATUS: FAIL
Found 1 critical errors:
  1. Column 'transaction_id': missing (expected: column to exist, actual: column not found)

⚠️ Pipeline Halted: Issue requires manual intervention
```

### Test 3: High Null Percentage (Expected: PASS WITH WARNINGS)

```bash
python3 main.py data/transactions_warnings.csv transactions
```

## Step 4: Inspect the Reports

Check the generated JSON reports:

```bash
ls -lh reports/
cat reports/monitor_report_*.json | jq .
```

Each report contains:
- Timestamp
- File path
- Status (PASS/FAIL/PASS_WITH_WARNINGS)
- Critical errors
- Warnings
- Statistical summary
- Quarantine indices (outliers)
- Execution log

## Step 5: Customize the Data Contract

Edit `config/monitor_config.yaml` to define your own rules:

```yaml
columns:
  - name: your_column
    type: int
    severity: CRITICAL  # or WARNING
    constraints:
      unique: true
      null_max: 0
      min: 0
      max: 1000
```

**Severity Levels:**
- `CRITICAL` - Hard stop, pipeline fails immediately
- `WARNING` - Soft fail, pipeline continues with documentation

## Understanding the Output

### Status Types

1. **✅ PASS** - All checks passed, no issues
2. **⚠️ PASS_WITH_WARNINGS** - Non-critical issues detected, data can proceed
3. **❌ FAIL** - Critical issues detected, data blocked

### The 4-Agent Flow

```
Monitor → Diagnoser → Healer → Validator
  ↓          ↓          ↓         ↓
VERDICT   ANALYZE    FIX      VERIFY
```

Currently, only the **Monitor Agent** is fully implemented. The other 3 agents are placeholders.

## Common Use Cases

### Use Case 1: Daily Data Ingestion

```bash
# Add to cron job
0 2 * * * cd /path/to/ai-agents-dre && python3 main.py /data/daily_feed.csv transactions
```

### Use Case 2: Real-time Validation

```python
from agents import MonitorAgent

monitor = MonitorAgent(config_path="config/monitor_config.yaml")
report = monitor.run(file_path="incoming_data.csv", table_name="transactions")

if report['status'] == 'FAIL':
    send_alert(report['critical_errors'])
elif report['status'] == 'PASS_WITH_WARNINGS':
    log_warnings(report['warnings'])
else:
    approve_data()
```

### Use Case 3: Historical Drift Monitoring

After running the pipeline multiple times, the system builds a historical baseline:

```bash
# Run 1: No baseline
python3 main.py data/transactions_clean.csv transactions
# Output: "Drift check: NO_BASELINE"

# Run 2-8: Building baseline
python3 main.py data/transactions_clean.csv transactions
# Output: "Drift check: PASS"

# Run 9: Detect drift
python3 main.py data/transactions_anomaly.csv transactions
# Output: "Drift check: DRIFT_DETECTED"
# Warning: "Row count (500) is 40% lower than 7-day average (850)"
```

## Troubleshooting

### Issue: "File not found"
```bash
# Make sure you're in the project directory
cd /Users/advaitdarbare/Desktop/ai-agents-dre
python3 main.py data/transactions_clean.csv transactions
```

### Issue: "Module not found"
```bash
# Reinstall dependencies
pip install -r requirements.txt
```

### Issue: "Permission denied"
```bash
# Make sure the data directory is writable
chmod 755 data/
```

## Next Steps

1. **Customize the YAML** - Define your own data contract
2. **Add your own data** - Test with real datasets
3. **Implement Diagnoser Agent** - Add root cause analysis
4. **Implement Healer Agent** - Add auto-remediation
5. **Implement Validator Agent** - Add post-fix verification
6. **Build Streamlit UI** - Add interactive chat interface

## Resources

- 📖 **README.md** - Comprehensive guide
- 🏗️ **ARCHITECTURE.md** - System architecture diagrams
- 📊 **SIMPLIFICATION_SUMMARY.md** - What was done
- 🔧 **config/monitor_config.yaml** - Data contract configuration

---

**Need Help?** Check the full documentation in `README.md` or review the architecture in `ARCHITECTURE.md`.
