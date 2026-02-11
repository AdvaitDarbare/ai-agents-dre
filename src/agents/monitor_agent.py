"""
Monitor Agent - Sentinel of Data Quality

This orchestrator ties together all data observability tools.
It acts as the "Gatekeeper" before data is loaded into the Data Warehouse.

Key Responsibilities:
1. Schema Validation (Hard Gate): Blocks missing columns/type mismatches.
2. Anomaly Detection (Soft Gate): Checks for drift and volume anomalies.
3. Impact Analysis (Context): Decides if an anomaly is critical based on lineage.
4. LLM Reasoning: Analyzing the combined report to produce actionable advice.

Outputs:
A structured JSON verdict + Human-readable summary.
"""

import os
import pandas as pd
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

# Tool Imports
from src.tools.schema_validator import validate_schema, ValidationResult, ValidationStatus
from src.tools.anomaly_detector import AnomalyDetector
from src.tools.impact_analyzer import ImpactAnalyzer
from src.tools.doris_loader import DorisLoader
from src.tools.schema_remediator import SchemaRemediator

# Agno Agent Imports
from agno.agent import Agent
from agno.models.openai import OpenAIChat

class MonitorAgent:
    """
    The Agentic Orchestrator - Coordinates detection, impact analysis, and decision making.
    """
    
    def __init__(self, contracts_path: str = "config/expectations", 
                 lineage_path: str = "config/lineage.yaml"):
        """
        Initialize the Monitor Agent with all sub-tools.
        """
        self.contracts_path = Path(contracts_path)
        
        # Initialize Detectors
        # SchemaValidator is functional, so we initiate it per run usually, 
        # but here we keep paths ready.
        
        self.anomaly_detector = AnomalyDetector()
        self.impact_analyzer = ImpactAnalyzer(lineage_path)
        self.loader = DorisLoader()
        self.remediator = SchemaRemediator()
        
        # Initialize the Reasoning Engine (LLM)
        # Using Agno's Agent with OpenAI
        self.reasoning_agent = Agent(
            model=OpenAIChat(id=os.getenv("OPENAI_MODEL_NAME", "gpt-4o")),
            description="You are a Senior Data Reliability Engineer. You analyze data quality reports and recommend actions.",
            instructions=[
                "Analyze the provided JSON verdict from the data pipeline.",
                "If status is BLOCKED, explain exactly why (e.g. schema violation).",
                "If status is WARNING, explain the anomaly and why we are allowing it (e.g. low impact).",
                "If status is PASSED, confirm data is clean.",
                "Provide specific, technical advice on next steps (e.g. 'Update schema.yaml', 'Quarantine file').",
                "Do not be generic. Use the specific metric names and values provided."
            ],
            markdown=True
        )

    def evaluate_data_file(self, file_path: str, dataset_name: str) -> Dict[str, Any]:
        """
        Execute the Sequential Logic Pipeline to evaluate a data file.
        
        Args:
            file_path: Path to the CSV/Parquet file
            dataset_name: Name of the dataset (e.g. 'transactions')
            
        Returns:
            Structured dictionary containing the final verdict.
        """
        verdict = {
            "status": "PASSED",
            "reason": "All checks passed.",
            "anomalies": [],
            "schema_evolution": {"new_columns": [], "missing_columns": []},
            "actions": ["Proceed to Load"],
            "dataset": dataset_name,
            "timestamp": datetime.now().isoformat()
        }
        
        # 0. Load Data
        try:
            if file_path.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            else:
                df = pd.read_csv(file_path)
        except FileNotFoundError:
            return {
                "status": "BLOCKED", 
                "reason": f"File not found: {file_path}",
                "actions": ["Abort"]
            }
        except Exception as e:
            return {
                "status": "BLOCKED",
                "reason": f"Failed to load file: {str(e)}",
                "actions": ["Abort"]
            }

        # ---------------------------------------------------------
        # Stage A: Schema Validation (The "Hard" Gate)
        # ---------------------------------------------------------
        print(f"\n🔍 [Stage A] Validating Schema for '{dataset_name}'...")
        contract_file = self.contracts_path / f"{dataset_name}.yaml"
        
        # We use the functional wrapper from schema_validator
        # but we need to pass the file_path, not the dataframe directly to the existing tool
        # The existing tool reads the file itself. 
        # Ideally, we'd refactor to accept DF, but for now let's pass file path.
        schema_result = validate_schema(contract_file, file_path, source_type="csv")
        schema_diff = schema_result.get_schema_diff()
        
        # Store diff in verdict
        verdict["schema_evolution"] = schema_diff
        
        # Logic: Breaking vs Non-Breaking
        if schema_diff["missing_columns"] or schema_diff["type_mismatches"]:
            verdict["status"] = "BLOCKED"
            verdict["reason"] = f"Schema Violation: Missing {len(schema_diff['missing_columns'])} cols, {len(schema_diff['type_mismatches'])} mismatches."
            verdict["actions"] = ["Quarantine", "Fix Schema/Data"]
            verdict["load_status"] = "SKIPPED (Blocked by Agent)"
            return self._enrich_with_llm(verdict)
            
        if schema_diff["new_columns"]:
            print(f"⚠️  Schema Evolution Detected: {len(schema_diff['new_columns'])} new columns.")
            # We don't block, but we note it.
        
        # ---------------------------------------------------------
        # Stage B: Anomaly Detection (The "Soft" Gate)
        # ---------------------------------------------------------
        print(f"\n📉 [Stage B] Checking Network Anomalies for '{dataset_name}'...")
        
        # Run Detection (Pass DF for distribution checks)
        anomaly_report = self.anomaly_detector.evaluate_run(dataset_name, 
                                                            {"row_count": len(df)}, 
                                                            dataframe=df)
        
        if anomaly_report["status"] == "ANOMALY_DETECTED":
            # Check Impact Analysis
            print(f"\n🎯 [Impact Analysis] Assessing Criticality...")
            impact = self.impact_analyzer.get_downstream_impact(dataset_name)
            criticality = impact.get("overall_criticality", "LOW")
            
            verdict["anomalies"] = anomaly_report["anomalies"]
            
            # Decision Matrix
            # We look at the max Z-Score to determine severity
            max_z = 0.0
            for anomaly in anomaly_report["anomalies"]:
                z = abs(anomaly.get("z_score", 0))
                if z > max_z: max_z = z
            
            if criticality in ["HIGH", "CRITICAL"] and max_z > 3.0:
                verdict["status"] = "BLOCKED"
                verdict["reason"] = f"CRITICAL ANOMALY (Z={max_z:.1f}) on HIGH IMPACT dataset."
                verdict["actions"] = ["Quarantine", "Alert Execs"]
            
            elif criticality == "LOW" and max_z > 3.0:
                verdict["status"] = "WARNING"
                verdict["reason"] = f"Anomaly detected (Z={max_z:.1f}), but impact is LOW."
                verdict["actions"] = ["Proceed to Load", "Log Warning"]
            
            else:
                # Fallback for minor anomalies
                 verdict["status"] = "WARNING"
                 verdict["reason"] = "Minor anomalies detected."
        
        # ---------------------------------------------------------
        # Stage C: The Action (Load or Skip)
        # ---------------------------------------------------------
        if verdict["status"] in ["PASSED", "WARNING"]:
            try:
                print(f"🚀 [Stage C] Loading Data into Doris...")
                load_result = self.loader.load_data(df, dataset_name)
                verdict["load_status"] = load_result
            except Exception as e:
                verdict["status"] = "BLOCKED"  # Downgrade to blocked on load failure
                verdict["load_status"] = {"error": str(e)}
                verdict["reason"] += f" (Load Failed: {str(e)})"
        else:
            verdict["load_status"] = "SKIPPED (Blocked by Agent)"

        # ---------------------------------------------------------
        # Stage D: The Verdict Summary
        # ---------------------------------------------------------
        return self._enrich_with_llm(verdict)

    def _enrich_with_llm(self, verdict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use Agno LLM Agent to generate a human-readable summary/advice.
        """
        print(f"\n🤖 [Stage C] Generating Agentic Advice...")
        
        # Convert verdict to string for LLM
        verdict_str = json.dumps(verdict, indent=2)
        
        try:
            # Ask the LLM
            response = self.reasoning_agent.run(f"Current Verdict:\n{verdict_str}")
            
            # Extract the content
            advice = response.content
            verdict["llm_advice"] = advice
        except Exception as e:
            print(f"❌ LLM Error: {e}")
            verdict["llm_advice"] = "Could not generate advice due to LLM error."
            
        return verdict

    def get_schema_content(self, dataset_name: str) -> str:
        """Read the raw content of a schema file."""
        path = self.contracts_path / f"{dataset_name}.yaml"
        if not path.exists():
            return ""
        with open(path, "r") as f:
            return f.read()

    def remediate_schema(self, dataset_name: str, new_yaml_content: str) -> bool:
        """Overwrite the existing schema with the new agreed-upon contract."""
        path = self.contracts_path / f"{dataset_name}.yaml"
        try:
            with open(path, "w") as f:
                f.write(new_yaml_content)
            print(f"✅ Schema remediated for {dataset_name}")
            return True
        except Exception as e:
            print(f"❌ Failed to remediate schema: {e}")
            return False
