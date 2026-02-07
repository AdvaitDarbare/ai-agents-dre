---
description: Simplify project to 4-agent autonomous DRE system
---

# Project Simplification Plan

## Phase 1: Clean Up Repository
1. Remove all complex documentation files (readme_docs/, HOW_IT_WORKS.md, etc.)
2. Remove example scripts and test files
3. Keep only essential config and schema files

## Phase 2: Restructure Agent Architecture
1. Create 4 agent modules:
   - `agents/monitor_agent.py` - The Gatekeeper (already exists, needs update)
   - `agents/diagnoser_agent.py` - Root Cause Analyzer
   - `agents/healer_agent.py` - Auto-Remediation
   - `agents/validator_agent.py` - Post-Fix Verification

## Phase 3: Implement Monitor Agent Tools
1. `tools/monitor/file_metadata_tool.py` - File sanity checks
2. `tools/monitor/data_loader_tool.py` - Smart sampling
3. `tools/monitor/schema_validator_tool.py` - YAML contract validation
4. `tools/monitor/stats_analysis_tool.py` - Adaptive outlier detection
5. `tools/monitor/drift_check_tool.py` - Historical comparison

## Phase 4: Create Simple Demo
1. Streamlit UI for interactive chat
2. SQLite for historical metrics
3. Sample data and YAML config

## Phase 5: Clean Documentation
1. Single README.md with architecture overview
2. YAML configuration guide
3. Quick start guide
