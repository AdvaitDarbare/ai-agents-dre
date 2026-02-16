# 6-Dimensional Data Quality Framework

This platform computes a weighted 6D quality score per run and stores the full dimension payload in `run_history.dimension_scores`.

Implementation: `src/tools/dimension_scorer.py`

## Dimensions

### 1) Validity
Checks structural and rule-level correctness:
- schema presence/type checks
- pattern and allowed-values violations

Primary sources:
- `SchemaValidator`
- `DataProfiler` violations

### 2) Completeness
Checks missingness and volume expectations:
- null rates
- row-count constraints

Primary sources:
- `DataProfiler`
- anomaly metrics such as `row_count`

### 3) Uniqueness
Checks duplication/PK quality:
- duplicate detection
- PK-related violations

Primary sources:
- `DataProfiler`
- schema/profile uniqueness indicators

### 4) Accuracy
Checks business-level correctness:
- numeric range violations
- custom check failures

Primary source:
- `DataProfiler.custom_check_results`

### 5) Timeliness
Checks freshness and SLA adherence.

Current behavior:
- consumes `freshness_age_minutes` from anomaly metrics when present
- if SLO target is available (`slo_target_minutes`), compares freshness directly
- otherwise uses a conservative fallback threshold for scoring

Primary sources:
- `MonitorAgent` freshness metric generation
- anomaly metric tags

### 6) Consistency
Checks stability over time using anomaly outputs.

Current behavior:
- penalizes anomalous metrics in consistency score
- uses anomaly reasons and z-score context

Primary source:
- `AnomalyDetector` anomaly report

## Weighted Overall Score

`overall_score = Σ(dimension_score × dimension_weight)`

Default weights are defined in `DimensionScorer.DEFAULT_WEIGHTS`, and can be overridden from contract YAML.

## Contract Weight Overrides

Contract-level weights can be supplied under:

```yaml
quality:
  quality_weights:
    validity: 25
    completeness: 25
    uniqueness: 15
    accuracy: 15
    timeliness: 10
    consistency: 10
```

Values are normalized automatically if they do not sum to 1.0.

## API Shape

`GET /quality-dimensions/{dataset_name}` returns the latest stored dimension payload from `run_history`.

If no run exists yet for the dataset, the API responds with a not-found status for dimension scores.

## Relationship to SLOs

- Dimension score: composite quality signal for reliability dashboards
- SLO checks: explicit objective pass/fail rules (availability, min quality, max anomalies, freshness SLA)

Both are persisted and surfaced to UI, but serve different operator decisions.
