"""
Tool F: ContractGeneratorTool
Generates an ODCS v3.1.0 Draft Contract from data profiling stats.
"""
import pandas as pd
import yaml
from typing import Dict, Any

class ContractGeneratorTool:
    """Auto-generates data contracts based on observed data."""
    
    def run(self, df: pd.DataFrame, stats_profile: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """
        Generate a draft ODCS v3.1.0 contract.
        
        Args:
            df: The dataframe (for types)
            stats_profile: Output from StatsAnalysisTool (for heuristics)
            table_name: Name of the table
            
        Returns:
            {
                "yaml_content": str,
                "schema_dict": dict
            }
        """
        columns = []
        
        for col in df.columns:
            # 1. Infer Type
            dtype = str(df[col].dtype)
            physical_type = "string"
            if "int" in dtype: physical_type = "int64"
            elif "float" in dtype: physical_type = "float64"
            elif "bool" in dtype: physical_type = "bool"
            elif "datetime" in dtype: physical_type = "timestamp"
            
            col_def = {
                "name": col,
                "physicalType": physical_type,
                "quality": []
            }
            
            # 2. Logic Mapping (Heuristics)
            stats = stats_profile.get(col, {})
            
            # Nulls -> Required
            null_pct = stats.get('null_pct', 100)
            if null_pct == 0:
                col_def['required'] = True
            else:
                col_def['required'] = False
                # Add quality rule for nulls?
                # Maybe loose constraint: nullValues <= observed + buffer?
                # For basic draft, let's keep it simple.
            
            # Unique -> Key candidate
            unique_pct = stats.get('unique_pct', 0)
            if unique_pct >= 99.9: # Allow small margin or strict 100
                col_def['quality'].append({
                    "metric": "unique",
                    "mustBe": True,
                    "severity": "critical" if unique_pct == 100 else "warning"
                })
            
            # Numeric ranges (Optional, maybe too strict for draft?)
            # min_val = stats.get('min')
            # if min_val is not None and isinstance(min_val, (int, float)) and min_val >= 0:
             #    col_def['quality'].append({"metric": "minValue", "mustBe": 0})
            
            columns.append(col_def)
            
        # Construct ODCS Structure
        contract = {
            "dataContractSpecification": "3.1.0",
            "id": f"urn:datacontract:{table_name}",
            "info": {
                "title": f"Draft Contract for {table_name}",
                "version": "1.0.0",
                "status": "draft",
                "owner": "data-team"
            },
            # Strict Mode Logic
            "strictMode": False, 
            "schema": [
                {
                    "name": table_name,
                    "physicalType": "table",
                    "properties": columns
                }
            ],
            "quality": [
                 # Default freshness rule
                {"metric": "freshness", "threshold": "24h"}
            ]
        }
        
        # Sort columns to standard order? (Already valid)
        
        return {
            "yaml_content": yaml.dump(contract, sort_keys=False),
            "schema_dict": contract
        }
