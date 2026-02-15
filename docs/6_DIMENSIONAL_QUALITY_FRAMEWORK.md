# 6-Dimensional Data Quality Framework

## Overview

This document describes the implementation of the industry-standard 6-dimensional data quality framework in the DRE platform, inspired by Soda.io and Great Expectations.

## The 6 Dimensions

### 1. **Validity** (Weight: 25%)
**Definition**: Does the data follow the correct rules and format?

**What it measures**:
- Schema correctness (columns exist, types match)
- Pattern/regex compliance (email format, phone format, date format)
- Allowed values/enums (Gender in ['Male', 'Female', 'Other'])
- Data type constraints

**Your Implementation**:
- ✅ **SchemaValidator**: Column existence, type validation
- ✅ **DataProfiler**: Pattern regex checks (line 340-352)
- ✅ **DataProfiler**: Allowed values (line 355-367)

**Example Checks**:
```yaml
columns:
  - name: Email
    pattern: '^[\w.-]+@[\w.-]+\.\w+$'
  - name: Gender
    allowed_values: ['Male', 'Female', 'Other']
```

**Score Calculation**:
```
validity_score = (passed_checks / total_checks) * 100
```

---

### 2. **Completeness** (Weight: 25%)
**Definition**: Is any data missing?

**What it measures**:
- Null/missing value detection
- Row count expectations (min/max)
- Required field enforcement

**Your Implementation**:
- ✅ **DataProfiler**: Nullable checks (line 288-293)
- ✅ **DataProfiler**: Null rate tracking (line 279)
- ✅ **DataProfiler**: Row count validation (line 154-173)
- ✅ **AnomalyDetector**: Row count anomalies

**Example Checks**:
```yaml
columns:
  - name: Patient Name
    nullable: false  # Must be present
quality:
  min_rows: 100
  max_rows: 10000
```

**Score Calculation**:
```
completeness_score = 100 - (null_rate * 100)
```

---

### 3. **Uniqueness** (Weight: 15%)
**Definition**: Are there duplicates where there shouldn't be?

**What it measures**:
- Primary key violations
- Duplicate detection
- Uniqueness rate

**Your Implementation**:
- ✅ **DataProfiler**: Primary key checks (line 296-303)
- ✅ **DataProfiler**: Uniqueness rate (line 281)
- ✅ **SchemaValidator**: Primary key duplicate check (line 425-444)

**Example Checks**:
```yaml
columns:
  - name: order_id
    isPrimaryKey: true  # Must be unique
```

**Score Calculation**:
```
uniqueness_score = (1 - duplicate_rate) * 100
```

---

### 4. **Accuracy** (Weight: 15%)
**Definition**: Does the data match reality and expectations?

**What it measures**:
- Range validation (age between 0-120)
- Cross-table reconciliation (orders.total = sum(line_items.amount))
- Reference data validation (country codes must exist in country table)
- Custom business rules

**Your Implementation**:
- ✅ **DataProfiler**: Min/max range checks (line 318-337)
- ✅ **DataProfiler**: Custom SQL checks (line 380-458)
- ❌ **Cross-table reconciliation**: Not implemented (Gap)
- ❌ **Reference data validation**: Not implemented (Gap)

**Example Checks**:
```yaml
columns:
  - name: Age
    min_value: 0
    max_value: 120
quality:
  custom_checks:
    - name: positive_total
      sql_condition: "order_total >= 0"
      severity: error
```

**Score Calculation**:
```
accuracy_score = ((total_rows - range_violations - custom_check_failures) / total_rows) * 100
```

---

### 5. **Timeliness** (Weight: 10%)
**Definition**: Is the data fresh and up-to-date?

**What it measures**:
- Freshness (last update timestamp)
- SLA compliance (data must refresh within 6 hours)
- Lag detection (time between source update and warehouse load)

**Your Implementation**:
- ⚠️ **MonitorAgent.check_timeliness**: Basic file mtime (line 114-131)
- ❌ **SLA-based freshness**: Not implemented (Gap)
- ❌ **Scheduled monitoring**: Not implemented (Gap)

**Example Checks**:
```yaml
quality:
  freshness_sla: "6h"  # Data must be < 6 hours old
  warning_threshold: "5h"
```

**Score Calculation** (Planned):
```
if data_age > sla_threshold:
    timeliness_score = 0
elif data_age > warning_threshold:
    timeliness_score = 50
else:
    timeliness_score = 100
```

---

### 6. **Consistency** (Weight: 10%)
**Definition**: Is the data consistent across systems and over time?

**What it measures**:
- Distribution drift (values shifting over time)
- Cross-system reconciliation (same order in ERP and warehouse)
- Source-to-target validation (ETL correctness)
- Historical baseline comparison

**Your Implementation**:
- ✅ **AnomalyDetector**: Distribution drift (Z-scores)
- ✅ **AnomalyDetector**: Historical baseline comparison
- ❌ **Cross-system reconciliation**: Not implemented (Gap)
- ❌ **Source-to-target diff**: Not implemented (Gap)

**Example Checks**:
```yaml
quality:
  anomaly_thresholds:
    z_score_warning: 2.5
    z_score_critical: 3.0
```

**Score Calculation**:
```
consistency_score = (metrics_without_anomalies / total_metrics) * 100
```

---

## Weighted Overall Score

The overall quality score is calculated as a **weighted average** of all 6 dimensions:

```
Overall Score = Σ (Dimension Score × Weight)
```

**Example Calculation**:
```
Validity:      98.0 × 0.25 = 24.50
Completeness:  75.2 × 0.25 = 18.80
Uniqueness:   100.0 × 0.15 = 15.00
Accuracy:      85.0 × 0.15 = 12.75
Timeliness:    92.0 × 0.10 =  9.20
Consistency:   80.0 × 0.10 =  8.00
                            -------
Overall Score:              88.25%
```

---

## Customizable Weights

Weights can be customized per dataset in the YAML contract:

```yaml
# config/expectations/financial_transactions.yaml
quality:
  dimension_weights:
    Validity: 0.30      # Critical for financial data
    Completeness: 0.20
    Uniqueness: 0.20
    Accuracy: 0.25      # Very important for $ amounts
    Timeliness: 0.05    # Less critical (batch processing)
    Consistency: 0.00   # Not applicable
```

**Use Cases**:
- **Financial Data**: High weight on Accuracy and Validity
- **Log Data**: Low weight on Completeness (missing logs OK)
- **Real-time Data**: High weight on Timeliness
- **Reference Data**: High weight on Uniqueness

---

## JSON API Response Format

The `/quality-dimensions/{dataset_name}` endpoint returns:

```json
{
  "dataset_name": "orders",
  "timestamp": "2026-02-15T14:00:00Z",
  "overall_score": 88.25,
  "dimensions": [
    {
      "name": "Validity",
      "score": 98.0,
      "weight": 0.25,
      "status": "PASS",
      "check_count": {
        "total": 12,
        "passed": 11,
        "failed": 1
      },
      "violations": [
        "Email: PATTERN violation: 5 values don't match regex"
      ]
    },
    {
      "name": "Completeness",
      "score": 75.2,
      "weight": 0.25,
      "status": "FAIL",
      "check_count": {
        "total": 5,
        "passed": 3,
        "failed": 2
      },
      "violations": [
        "Age: NOT NULL violation: 159 nulls (15.9%)",
        "Email: NOT NULL violation: 384 nulls (38.4%)"
      ]
    },
    {
      "name": "Uniqueness",
      "score": 100.0,
      "weight": 0.15,
      "status": "PASS",
      "check_count": {
        "total": 2,
        "passed": 2,
        "failed": 0
      },
      "violations": []
    },
    {
      "name": "Timeliness",
      "score": 92.0,
      "weight": 0.10,
      "status": "PASS",
      "check_count": {
        "total": 1,
        "passed": 1,
        "failed": 0
      },
      "violations": []
    },
    {
      "name": "Accuracy",
      "score": 85.0,
      "weight": 0.15,
      "status": "PASS",
      "check_count": {
        "total": 4,
        "passed": 4,
        "failed": 0
      },
      "violations": []
    },
    {
      "name": "Consistency",
      "score": 80.0,
      "weight": 0.10,
      "status": "WARN",
      "check_count": {
        "total": 3,
        "passed": 2,
        "failed": 1
      },
      "violations": [
        "row_count: Z-score 2.8 - Volume dropped 15%"
      ]
    }
  ],
  "remediation_status": "OPEN_INCIDENT"
}
```

---

## Visualization

### 1. Radar (Spider) Chart
Best for showing balanced view across all 6 dimensions.

**Implementation**: `QualityRadarChart.jsx`

**Benefits**:
- Instantly see which dimension is failing
- Visualize score distribution
- Compare datasets side-by-side

### 2. Progress Rings (Donuts)
A row of 6 circular progress bars, one per dimension.

**Implementation**: Coming soon

**Benefits**:
- Space-efficient
- Easy to scan
- Works well in dashboards

### 3. Weighted Score Card
Simple text-based breakdown with visual bars.

**Implementation**: Coming soon

**Benefits**:
- Accessible
- Easy to understand
- Works in all contexts

---

## Implementation Status

### ✅ Fully Implemented (3/6)
1. **Validity**: SchemaValidator + DataProfiler (patterns, allowed_values)
2. **Completeness**: DataProfiler (null checks) + AnomalyDetector (row count)
3. **Uniqueness**: DataProfiler (primary key) + SchemaValidator (duplicates)

### ⚠️ Partially Implemented (3/6)
4. **Accuracy**: Range checks ✅, Custom SQL ✅, Cross-table ❌, Reference data ❌
5. **Timeliness**: File mtime ⚠️, SLA ❌, Scheduled scans ❌
6. **Consistency**: Drift detection ✅, Cross-system ❌, Source-to-target ❌

---

## Roadmap: Closing the Gaps

### Phase 1: Accuracy Enhancements
- [ ] Cross-table reconciliation checks
- [ ] Reference data validation (lookup tables)
- [ ] Inter-dataset consistency rules

### Phase 2: Timeliness Implementation
- [ ] SLA-based freshness monitoring
- [ ] Scheduled scan orchestration
- [ ] Lag detection (source → warehouse)

### Phase 3: Consistency Expansion
- [ ] Cross-system reconciliation
- [ ] Source-to-target diff validation
- [ ] Multi-environment consistency checks

---

## Usage Example

### 1. Add dimension weights to contract

```yaml
# config/expectations/healthcare_messy_data.yaml
quality:
  dimension_weights:
    Validity: 0.30
    Completeness: 0.25
    Uniqueness: 0.15
    Accuracy: 0.15
    Timeliness: 0.10
    Consistency: 0.05
```

### 2. Run pipeline

```bash
python src/main.py run healthcare_messy_data
```

### 3. View dimension scores

```bash
curl http://localhost:8000/quality-dimensions/healthcare_messy_data | jq
```

### 4. Visualize in UI

Navigate to dataset → Quality tab → Radar Chart

---

## Comparison with Soda.io

| Feature | Soda.io | Your DRE Platform |
|---------|---------|-------------------|
| 6-Dimensional Framework | ✅ Implied | ✅ Explicit |
| Weighted Scoring | ❌ Fixed | ✅ Configurable |
| Radar Chart | ⚠️ Via integrations | ✅ Built-in |
| Per-dimension drill-down | ✅ Yes | ✅ Yes |
| Custom weights | ❌ No | ✅ Per-dataset YAML |
| Transparent calculation | ❌ Opaque | ✅ Open formula |

**Your Advantage**: Customizable weights + transparent scoring + native visualization

---

## Next Steps

1. ✅ Implement DimensionScorer tool
2. ✅ Integrate into MonitorAgent pipeline
3. ✅ Create API endpoint
4. ✅ Build Radar Chart component
5. ⬜ Add dimension weights to YAML schema
6. ⬜ Store dimension history in PostgreSQL
7. ⬜ Build dimension trend charts
8. ⬜ Add dimension-based alerting
