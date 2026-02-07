# Agentic Data Reliability Engineering (DRE) Platform

An autonomous, AI-driven system designed to guard your data warehouse by monitoring data quality, enforcing strict contracts, and enabling seamless human-in-the-loop governance.

## 🚀 Project Overview

This platform implements a **Monitor Agent** that orchestrates a suite of specialized tools to validate data, detect drift, and manage the lifecycle of data contracts. It features a modern **React-based Data Contract IDE** for real-time visibility and editing of governance rules.

## ✨ Key Features Implemented

### 1. 🧠 The Monitor Agent
- **File Metadata Tool**: "The Scout" - inspecting file size, format, and modification times before loading.
- **Data Loader Tool**: "The Gateway" - standardizes column names (normalization) and detects PII.
- **Schema Validator Tool**: "The Enforcer" - dynamically validates data against **Open Data Contract Standard (ODCS)** YAML files.
- **Drift Check Tool**: "The Memory" - detects distribution shifts compared to historical baselines.

### 2. 🛡️ Advanced Data Quality
- **Stats Analysis Tool**: "The Mathematician" - adaptive outlier detection using Z-Score (normal data) and IQR (skewed data).
- **Seasonal Detector**: "The Context Aware" - learns day-of-week and monthly patterns to prevent false alarms on cyclical data.
- **Consistency Check Tool**: "The Relationship Manager" - validates referential integrity (Foreign Keys) between datasets.
- **Table Prioritizer**: "The Strategist" - intelligent scanning prioritization based on downstream table impact.

### 3. 🛡️ Data Contract & Governance
- **Strict Mode Enforcement**: Configurable `strictMode` to either CRITICAL STOP on violation or PASS WITH WARNINGS.
- **AI-Driven Draft Generation**: Automatically profiles new data and generates draft ODCS contracts for missing tables.
- **Human-in-the-Loop (HITL)**: Interactive approval workflow for new contracts.

### 4. 🖥️ Data Contract IDE (Frontend)
- **Interactive Report Viewer**: Visual dashboard with status badges (PASS, WARN, FAIL, MISSING).
- **Advanced Insights Panel**: 
    - **Seasonal Context**: Visualizes whether current data volume is "Normal" for today (e.g., Monday).
    - **Relationships**: Visualizes Foreign Key validation status and orphans.
- **Live Contract Editor**: Edit YAML contracts directly in the browser with syntax highlighting.
- **Version Lineage**: Full history tracking with one-click restore.

## 🛠️ Tech Stack
- **Backend**: Python, FastAPI, Great Expectations (GX), Pandas, SQLite
- **Frontend**: React, TypeScript, Tremor UI, Tailwind CSS
- **Data Standards**: ODCS (Open Data Contract Standard) v3.1.0

## 🚦 Current Status & Scenarios
- ✅ **Products**: Valid data with approved contract -> **PASS**
- ❌ **Users (Strict)**: Invalid data + strict mode -> **CRITICAL FAIL**
- ❌ **Transactions (Orphans)**: Data with missing Foreign Keys -> **FAIL (Consistency Check)**
- ✅ **Transactions (Seasonal)**: Valid data matches learned day-of-week pattern -> **PASS (Seasonal Context)**
- 📝 **Orders**: New data + no contract -> **DRAFT GENERATED (HITL)**

## 🔮 Roadmap
- [x] Basic Monitor Agent
- [x] Frontend Dashboard
- [x] Live Contract Editing
- [x] Contract Versioning
- [x] Advanced Insights (Seasonality, Consistency)
- [ ] Integration with Slack/Teams for alerts
- [ ] Automated remediation suggestions
