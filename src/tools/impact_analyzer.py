"""
Impact Analyzer Tool - Downstream Dependency & Criticality Analysis

This tool prevents "Alert Fatigue" by checking if a data failure actually matters.
It reads a lineage graph (YAML) to determine what downstream systems (Dashboards, ML Models)
depend on the failing dataset.

Key Features:
1. Loads lineage graph from config/lineage.yaml
2. Identifies all downstream consumers (Consumer + Criticality)
3. Provides context for the Agent to escalate or suppress alerts.

Example Output:
{
    "dataset": "transactions",
    "criticality": "HIGH",
    "impacted_systems": [
        {"name": "Executive_Dashboard", "type": "dashboard", "owner": "CEO", "criticality": "HIGH"},
        {"name": "Fraud_Detection_Model", "type": "ml_model", "owner": "Data Science", "criticality": "MEDIUM"}
    ]
}
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional

class ImpactAnalyzer:
    """
    The Business Context Engine - Determines the "Blast Radius" of a data failure.
    """
    
    def __init__(self, lineage_path: str = "config/lineage.yaml"):
        """
        Initialize the Impact Analyzer.
        
        Args:
            lineage_path: Path to the lineage configuration file.
        """
        self.lineage_path = Path(lineage_path)
        self.lineage_graph = self._load_lineage()
        
    def _load_lineage(self) -> Dict[str, Any]:
        """Load and parse the lineage YAML file."""
        if not self.lineage_path.exists():
            print(f"⚠️ WARNING: Lineage file not found at {self.lineage_path}. Assuming no downstream dependencies.")
            return {}
            
        try:
            with open(self.lineage_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"❌ ERROR: Failed to parse lineage file: {e}")
            return {}

    def get_downstream_impact(self, dataset_name: str) -> Dict[str, Any]:
        """
        Identify all downstream consumers for a given dataset.
        
        Args:
            dataset_name: Name of the dataset (e.g., 'transactions')
            
        Returns:
            Dictionary containing overall criticality and list of impacted systems.
        """
        impact_report = {
            "dataset": dataset_name,
            "overall_criticality": "LOW",
            "impacted_consumers": []
        }
        
        # 1. Find the dataset in the lineage graph
        # Structure assumption: 
        # datasets:
        #   transactions:
        #     consumers:
        #       - name: safe_executive_dashboard
        #         type: dashboard
        #         criticality: HIGH
        
        dataset_info = self.lineage_graph.get("datasets", {}).get(dataset_name)
        
        if not dataset_info:
            return impact_report
            
        consumers = dataset_info.get("consumers", [])
        impact_report["impacted_consumers"] = consumers
        
        # 2. Determine Overall Criticality (Max of all consumers)
        criticality_levels = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
        max_level = 0
        
        for consumer in consumers:
            level_str = consumer.get("criticality", "LOW").upper()
            level_val = criticality_levels.get(level_str, 1)
            if level_val > max_level:
                max_level = level_val
                # Update the string representation
                impact_report["overall_criticality"] = level_str
                
        return impact_report

if __name__ == "__main__":
    # Create a dummy lineage file for testing if it doesn't exist
    dummy_lineage = {
        "datasets": {
            "transactions": {
                "consumers": [
                    {"name": "CEO_Revenue_Dashboard", "type": "dashboard", "owner": "Executive Team", "criticality": "HIGH"},
                    {"name": "Churn_Prediction_Model", "type": "ml_model", "owner": "Data Science", "criticality": "MEDIUM"}
                ]
            },
            "logs": {
                "consumers": [
                    {"name": "Dev_Debug_Tool", "type": "app", "owner": "Engineering", "criticality": "LOW"}
                ]
            }
        }
    }
    
    Path("config").mkdir(exist_ok=True)
    with open("config/lineage.yaml", "w") as f:
        yaml.dump(dummy_lineage, f)
        
    # Test the Analyzer
    analyzer = ImpactAnalyzer()
    
    print("\n🔍 Analyzing Impact for 'transactions':")
    report = analyzer.get_downstream_impact("transactions")
    print(yaml.dump(report, sort_keys=False))
    
    print("\n🔍 Analyzing Impact for 'logs':")
    report = analyzer.get_downstream_impact("logs")
    print(yaml.dump(report, sort_keys=False))
