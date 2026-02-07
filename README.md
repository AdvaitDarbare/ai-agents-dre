# Agentic Data Reliability Engineering (DRE) Platform

An autonomous, AI-driven system designed to guard your data warehouse by monitoring data quality, enforcing strict contracts, and enabling seamless human-in-the-loop governance.

## 🚀 Project Overview

This platform implements a **Monitor Agent** that orchestrates a suite of specialized tools to validate data, detect drift, and manage the lifecycle of data contracts. It features a modern **React-based Data Contract IDE** for real-time visibility and editing of governance rules.

## ✨ Key Features Implemented

### 1. 🧠 The Monitor Agent
- **File Metadata Tool**: "The Scout" - inspecting file size, format, and modification times before loading.
- **Data Loader Tool**: "The Gateway" - standardizes column names (normalization) and detects PII.
- **Schema Validator Tool**: "The Enforcer" - dynamically validates data against **Open Data Contract Standard (ODCS)** YAML files.
- **Drift Check Tool**: "The Statistician" - detects distribution shifts using PSI and KL Divergence.

### 2. 🛡️ Data Contract & Governance
- **Strict Mode Enforcment**: Configurable `strictMode` to either CRITICAL STOP on violation or PASS WITH WARNINGS.
- **AI-Driven Draft Generation**: Automatically profiles new data and generates draft ODCS contracts for missing tables.
- **Human-in-the-Loop (HITL)**: Interactive approval workflow for new contracts.

### 3. 🖥️ Data Contract IDE (Frontend)
- **Interactive Report Viewer**: Visual dashboard with status badges (PASS, WARN, FAIL, MISSING), KPI grids, and drift analysis.
- **Live Contract Editor**: Edit YAML contracts directly in the browser with syntax highlighting.
- **"Update & Re-Validate"**: Save changes and immediately trigger a pipeline re-run to test new rules.
- **Version Lineage**: 
    - Full history tracking of contract changes.
    - One-click restore to previous versions.
    - Clean sequential version numbering (e.g., "Version 1", "Version 2").
    - Visual "Active" indicators for the currently loaded version.

## 🛠️ Tech Stack
- **Backend**: Python, FastAPI, Great Expectations (GX)
- **Frontend**: React, TypeScript, Tremor UI, Tailwind CSS
- **Data Standards**: ODCS (Open Data Contract Standard) v3.1.0

## 🚦 Current Status & Scenarios
- ✅ **Products**: Valid data with approved contract -> **PASS**
- ❌ **Users (Strict)**: Invalid data + strict mode -> **CRITICAL FAIL**
- ⚠️ **Users (Lenient)**: Invalid data + relaxed mode -> **WARN**
- 📝 **Orders**: New data + no contract -> **DRAFT GENERATED (HITL)**

## 🔮 Roadmap
- [x] Basic Monitor Agent
- [x] Frontend Dashboard
- [x] Live Contract Editing
- [x] Contract Versioning & Archiving
- [ ] Integration with Slack/Teams for alerts
- [ ] Automated remediation suggestions
