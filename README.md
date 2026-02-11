# Agentic Data Reliability Engineering (DRE) Platform

Welcome to the **Agentic DRE** platform—a next-generation data observability and reliability system driven by autonomous AI agents. This platform acts as an intelligent "gatekeeper" for your data lake and warehouse, ensuring that only high-quality, trusted data makes it into production.

## 🚀 Progress So Far

We have built a robust foundation for automated data reliability:

### 1. Agentic Orchestration
- **Monitor Agent**: A production-grade orchestrator that coordinates multiple detection tools. It uses a "Sequential Logic Pipeline" to evaluate data health.
- **Agentic Reasoning**: Integrated with **Agno (GPT-4o)** to provide human-readable verdicts, deep analysis of anomalies, and actionable technical advice.
- **Triage Logic**: Implemented sophisticated status handling (PASSED, WARNING, BLOCKED) based on criticality and impact.

### 2. Multi-Layer Detection
- **Schema Validation (Hard Gate)**: Automated checks for missing columns and type mismatches that block broken data before it lands.
- **Data Profiling**: Value-level quality checks including null rates, distribution statistics, and custom SQL-based business logic validation.
- **Anomaly Detection**: Z-score based drift detection for volume and metric distributions, backed by a persistent **DuckDB** history.
- **Impact Analysis**: Lineage-aware criticality assessment to determine the "blast radius" of data issues.

### 3. Automated Remediation
- **Schema Remediator**: An actuator agent that proposes YAML-based schema fixes when evolution is detected.
- **Safety First**: Automatic backups of configuration files before any automated updates.

### 4. Control Center (Dashboard)
- **Schema Health Pulse**: A global view monitoring all dataset contracts at once.
- **Deep-Dive Analytics**: Heatmaps for null rates, volume anomaly charts with confidence bands, and data freshness timelines.
- **Interactive Agent**: A copilot interface for querying technical details about pipeline health.

---

## 🔮 What's Next?

Our roadmap focuses on deepening the autonomy and integration of the platform:

1.  **Advanced Actuator Agents**: Extending remediation beyond schema fixes to include automated data cleansing, quarantining, and source-system notifications.
2.  **Expanded Connectivity**: Adding native connectors for major cloud warehouses and data lake formats (e.g., Iceberg) to support enterprise-wide observability.
3.  **Statistical Sophistication**: Implementing more advanced drift detection methods like Kolmogorov-Smirnov (K-S) tests and CUSUM for subtle trend shifts.
4.  **Agentic PII Detection**: Automatic identification and masking of sensitive information to ensure compliance and privacy.
5.  **Streaming Support**: Moving from batch file evaluation to real-time stream monitoring (e.g., Kafka, Pulsar).

---

## 🛠️ Getting Started

1.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Run the demo**:
    ```bash
    python src/main.py
    ```
3.  **Launch the Dashboard**:
    ```bash
    streamlit run src/dashboard/app.py
    ```

---
*Built for modern data teams who value reliability, automation, and speed.*
