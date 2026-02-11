# Databricks Agentic Data Quality Monitoring — Gap Analysis & Improvement Plan

> **Source:** [Databricks Blog — Data Quality Monitoring at Scale with Agentic AI](https://www.databricks.com/blog/data-quality-monitoring-scale-agentic-ai) (Published Feb 4, 2026)
> **Analysis Date:** February 11, 2026

---

## 1. Executive Summary

Databricks' Agentic Data Quality Monitoring (DQM) represents the state of the art in enterprise data observability. It's built natively on **Unity Catalog** and focuses on three principles:

1. **Learned behavior, not static rules** — AI agents learn historical patterns and seasonal behavior
2. **Prioritization by lineage** — Issues are ranked by downstream impact using Unity Catalog lineage
3. **Root cause tracing** — Connects quality issues directly back to upstream Lakeflow jobs/pipelines

Our Agentic DRE platform already implements many of these concepts but at a smaller scale. This document identifies gaps and proposes concrete improvements to bring our platform closer to production-grade enterprise quality.

---

## 2. Feature-by-Feature Comparison

### ✅ What We Already Do Well (Parity or Close)

| Capability | Databricks | Our Platform | Status |
|---|---|---|---|
| **Z-Score Anomaly Detection** | Agents learn historical patterns | `AnomalyDetector` with seasonal baselines in DuckDB | ✅ Parity |
| **Seasonal Awareness** | Learns weekend dips, tax season, etc. | `get_seasonal_baseline()` uses day-of-week bucketing | ✅ Parity |
| **Lineage-Based Prioritization** | Unity Catalog lineage + certified tags | `ImpactAnalyzer` reads `lineage.yaml` for downstream criticality | ✅ Parity |
| **Data Profiling** | Table-level summary stats tracked over time | `DataProfiler` with null rates, uniqueness, ranges, pattern/regex, allowed values | ✅ **Ahead** |
| **Schema Validation** | Part of anomaly detection | `SchemaValidator` with type checks, column presence, evolution detection | ✅ Parity |
| **Self-Healing Remediation** | Not mentioned (roadmap: "filter and quarantine bad data") | `SchemaRemediator` with LLM-powered YAML fixes, safety gates, backups | ✅ **Ahead** |
| **Configurable Thresholds** | Agents learn thresholds automatically | Externalized to YAML `anomaly_thresholds` block | ✅ Parity |
| **Trust Score / Health Indicator** | "Health Indicator" across platform | Weighted composite Trust Score (history + verdict + quality) | ✅ Parity |

### ❌ Gaps Where Databricks Is Ahead

| # | Databricks Capability | Our Gap | Severity |
|---|---|---|---|
| **G1** | **Schema-Level Auto-Discovery** — Enable monitoring on an entire *schema* (all tables), not one table at a time | We require explicit YAML contracts per table. No auto-discovery. | 🔴 HIGH |
| **G2** | **Intelligent Scan Scheduling** — Tables are scanned based on importance + update frequency. Static/deprecated tables are skipped automatically. | We scan every dataset every time. No frequency awareness. | 🔴 HIGH |
| **G3** | **System Tables for Observability** — Health, thresholds, and patterns stored in queryable system tables | We log to DuckDB `metric_history` but lack structured system tables for health status, learned thresholds, and run outcomes | 🟡 MEDIUM |
| **G4** | **Root Cause Tracing to Upstream Jobs** — Click from quality issue → exact upstream pipeline/job that caused it | We have lineage for *downstream* impact but no *upstream* root cause tracing | 🟡 MEDIUM |
| **G5** | **Multi-Table Holistic View** — Consolidated dashboard showing health of ALL tables in a schema | Dashboard shows one dataset at a time | 🟡 MEDIUM |
| **G6** | **Automated Alerting** — Built-in alert routing based on issue severity and lineage | No alerting system (Slack, email, PagerDuty) | 🟡 MEDIUM |
| **G7** | **Freshness & Completeness as First-Class Signals** — Monitored automatically without configuration | Freshness is mocked in the timeline; completeness (row count) is checked but not as a dedicated first-class signal | 🟡 MEDIUM |
| **G8** | **Certification/Deprecation Tags** — Tables can be "certified" or "deprecated" to influence scan priority | No concept of table lifecycle status | 🟢 LOW |

---

## 3. Improvement Plan — Prioritized Phases

### Phase 1: Schema-Level Auto-Discovery (Addresses G1, G5)
**Goal:** Monitor all YAML contracts in `config/expectations/` automatically instead of requiring manual dataset selection.

**Implementation:**
```
File: src/agents/monitor_agent.py
New Method: discover_datasets()
```

1. **Auto-scan** `config/expectations/*.yaml` to find all dataset contracts
2. **Register** each dataset in a new DuckDB table `dataset_registry`
3. **Dashboard change:** Add a "Schema Health Overview" tab showing all datasets in a grid with status badges (✅ PASSED / ⚠️ WARNING / 🚫 BLOCKED)
4. **Batch evaluation:** `evaluate_all()` method that runs all datasets sequentially

```python
# Proposed API
agent = MonitorAgent()
# Instead of:
result = agent.evaluate_data_file("data/transactions.csv", "transactions")
# Also support:
results = agent.evaluate_all("data/")  # Auto-discovers all datasets
```

**Effort:** ~1 day | **Impact:** 🔴 HIGH

---

### Phase 2: Intelligent Scan Scheduling (Addresses G2, G8)
**Goal:** Don't re-scan datasets that haven't changed. Scan critical datasets more frequently.

**Implementation:**
```
New File: src/tools/scan_scheduler.py
```

1. **Track file modification times** — Store `file_mtime` in DuckDB. Skip files that haven't changed since last scan.
2. **Criticality-based frequency:**
   - `HIGH` lineage criticality → scan every run
   - `MEDIUM` → scan every 2nd run
   - `LOW` → scan every 5th run
3. **Lifecycle tags** in YAML contract:
   ```yaml
   info:
     lifecycle: certified  # or: deprecated, draft, active
   ```
   - `deprecated` → skip automatically
   - `draft` → scan but don't alert

**Effort:** ~0.5 day | **Impact:** 🔴 HIGH

---

### Phase 3: System Tables & Run History (Addresses G3)
**Goal:** Structured, queryable tables for all observability data — not just raw metrics.

**New DuckDB Tables:**
```sql
-- Table 1: Run outcomes (the missing piece)
CREATE TABLE run_history (
    run_id VARCHAR,
    timestamp TIMESTAMP,
    dataset_name VARCHAR,
    status VARCHAR,        -- PASSED, WARNING, BLOCKED
    quality_score DOUBLE,
    anomaly_count INTEGER,
    z_score_max DOUBLE,
    reason VARCHAR,
    duration_ms INTEGER
);

-- Table 2: Learned thresholds (so agents don't re-learn)
CREATE TABLE learned_thresholds (
    dataset_name VARCHAR,
    metric_name VARCHAR,
    baseline_mean DOUBLE,
    baseline_std DOUBLE,
    baseline_type VARCHAR,  -- 'seasonal' or 'global'
    last_updated TIMESTAMP,
    sample_count INTEGER
);

-- Table 3: Dataset registry
CREATE TABLE dataset_registry (
    dataset_name VARCHAR,
    contract_path VARCHAR,
    lifecycle VARCHAR,       -- certified, active, deprecated
    criticality VARCHAR,
    last_scanned TIMESTAMP,
    last_status VARCHAR,
    scan_count INTEGER
);
```

**Effort:** ~1 day | **Impact:** 🟡 MEDIUM

---

### Phase 4: Upstream Root Cause Tracing (Addresses G4)
**Goal:** When a quality issue is detected, trace it back to the *cause* not just the *effect*.

**Implementation:**
1. **Extend `lineage.yaml`** with upstream sources:
   ```yaml
   datasets:
     transactions:
       upstream:
         - name: payment_gateway_api
           type: api
           refresh: "every 15 min"
         - name: user_service
           type: microservice
       consumers:
         - name: CEO_Revenue_Dashboard
           criticality: HIGH
   ```

2. **Root cause suggestions** in the LLM enrichment prompt:
   ```
   Given these upstream data sources: {upstream_sources}
   And this anomaly: {anomaly_details}
   Suggest the most likely root cause and which upstream source to investigate.
   ```

3. **Dashboard:** Add "Probable Root Cause" card to the Overview tab.

**Effort:** ~0.5 day | **Impact:** 🟡 MEDIUM

---

### Phase 5: Automated Alerting (Addresses G6)
**Goal:** Route alerts to the right people based on severity and lineage.

**Implementation:**
```
New File: src/tools/alert_router.py
```

1. **Alert channels** defined in a new `config/alerts.yaml`:
   ```yaml
   channels:
     slack:
       webhook_url: ${SLACK_WEBHOOK_URL}
       severity: [WARNING, BLOCKED]
     email:
       recipients: ["data-team@company.com"]
       severity: [BLOCKED]
     pagerduty:
       api_key: ${PAGERDUTY_KEY}
       severity: [BLOCKED]
       only_if_criticality: [HIGH, CRITICAL]
   ```

2. **Trigger** at the end of `evaluate_data_file()` based on verdict
3. **De-duplication:** Don't alert for the same issue within a configurable cooldown window

**Effort:** ~1 day | **Impact:** 🟡 MEDIUM

---

### Phase 6: Real Freshness Monitoring (Addresses G7)
**Goal:** Track actual data freshness rather than mocking it.

**Implementation:**
1. **Store file stat `mtime`** and the max timestamp from the data itself
2. **Define SLA** in the YAML contract:
   ```yaml
   quality:
     freshness_sla_hours: 24  # Data should be < 24 hours old
   ```
3. **Calculate freshness:** `data_age = now() - max(timestamp_column)`
4. **Alert** if `data_age > freshness_sla_hours`
5. **Dashboard:** Replace the mock freshness timeline with real values from `run_history`

**Effort:** ~0.5 day | **Impact:** 🟡 MEDIUM

---

## 4. Architecture Comparison

```
┌──────────────────────────────────────────────────────────────────┐
│                    DATABRICKS ARCHITECTURE                       │
├──────────────────────────────────────────────────────────────────┤
│  Unity Catalog ──→ Schema-Level Auto-Discovery                  │
│       │                                                          │
│       ├── Anomaly Detection (learned thresholds)                 │
│       ├── Data Profiling (summary stats over time)               │
│       ├── Lineage-Based Prioritization (upstream + downstream)   │
│       └── System Tables (queryable health/thresholds/patterns)   │
│       │                                                          │
│       ▼                                                          │
│  Lakeflow Observability ──→ Root Cause Tracing to Jobs           │
│  Health Indicator ──→ Consistent signal across all surfaces      │
│  Alerts ──→ Automated routing (roadmap)                          │
│  Quarantine ──→ Filter bad data (roadmap)                        │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                 OUR CURRENT ARCHITECTURE                         │
├──────────────────────────────────────────────────────────────────┤
│  YAML Contracts ──→ Per-Dataset Manual Selection                 │
│       │                                                          │
│       ├── SchemaValidator  (column/type/evolution)                │
│       ├── DataProfiler     (null, range, regex, allowed_values)  │
│       ├── AnomalyDetector  (Z-score + seasonal baselines)        │
│       ├── ImpactAnalyzer   (downstream lineage only)             │
│       └── DuckDB metric_history (raw metrics, no run outcomes)   │
│       │                                                          │
│       ▼                                                          │
│  SchemaRemediator ──→ LLM Self-Healing (AHEAD of Databricks)    │
│  Trust Score ──→ Weighted composite (parity)                     │
│  Dashboard ──→ Single-dataset view (gap)                         │
│  Alerting ──→ Not implemented (gap)                              │
└──────────────────────────────────────────────────────────────────┘
```

---

## 5. Recommended Execution Order

| Priority | Phase | Effort | Impact | Why This Order |
|---|---|---|---|---|
| 🥇 **P0** | Phase 1: Auto-Discovery | 1 day | 🔴 | Foundation for everything else — can't scale without it |
| 🥈 **P1** | Phase 3: System Tables | 1 day | 🟡 | Enables Phase 2 and Phase 5 — need `run_history` and `dataset_registry` |
| 🥉 **P2** | Phase 2: Scan Scheduling | 0.5 day | 🔴 | Biggest scaling win — skip unchanged/deprecated tables |
| 4️⃣ **P3** | Phase 6: Real Freshness | 0.5 day | 🟡 | Quick win — replaces mock data with real signal |
| 5️⃣ **P4** | Phase 4: Root Cause Tracing | 0.5 day | 🟡 | High value for debugging but requires upstream lineage data |
| 6️⃣ **P5** | Phase 5: Alerting | 1 day | 🟡 | Important for production but not blocking other features |

**Total estimated effort: ~4.5 days**

---

## 6. Our Competitive Advantages (Where We're Ahead)

While Databricks' platform is broader, our project has unique strengths:

1. **🤖 LLM Self-Healing** — Databricks detects problems. We *fix* them. Our `SchemaRemediator` generates corrected YAML via LLM and applies it with safety gates (YAML validation, column removal prevention, timestamped backups). Databricks lists "filter and quarantine bad data" as *roadmap*.

2. **📊 Richer Profiling** — Our `DataProfiler` already checks regex patterns, allowed values, range constraints, custom SQL, and per-column quality scores. Databricks lists "percent null, uniqueness, and validity" as *roadmap*.

3. **⚖️ Trust Score** — Our weighted composite (history 40% + verdict 35% + quality 25%) gives a single number users can reason about. Databricks calls this "Health Indicator" and shows it's still being built out.

4. **🔍 Schema Changelog** — We have a diff-based audit trail of every schema change via backups. Databricks doesn't mention this.

---

## 7. Key Takeaway

> **Databricks' key insight: The hard problem isn't detection — it's scale and prioritization.**

They don't try to build the best anomaly detector. They focus on:
- Monitoring *everything* automatically (schema-level discovery)
- Scanning *smartly* (importance-weighted scheduling)
- Tracing *root cause* (upstream lineage to jobs)
- Making health *visible everywhere* (system tables)

Our detection and remediation capabilities are strong. **What we need is the orchestration layer** — auto-discovery, scheduling, structured run history, and alerting — to turn a powerful tool into a scalable platform.
