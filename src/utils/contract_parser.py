import yaml
from typing import Dict, List, Any

class ContractParser:
    """
    Parses your custom ODCS YAML to extract rules for the Agent tools.
    Acts as an adapter between your YAML format and the Tool interfaces.
    """
    def __init__(self, contracts_path="config/expectations"):
        self.contracts_path = contracts_path
        # We don't load in __init__ anymore to allow dynamic loading per table

    def _load_yaml(self, table_name: str) -> Dict[str, Any]:
        """Internal helper to load the raw YAML."""
        try:
            with open(f"{self.contracts_path}/{table_name}.yaml", "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            return {}

    def get_schema_columns(self, table_name: str = "transactions") -> Dict[str, str]:
        """
        REQUIRED BY: SchemaValidator
        Returns: { 'col_name': 'duckdb_type' }
        """
        contract = self._load_yaml(table_name)
        type_map = {
            "varchar": "VARCHAR",
            "string": "VARCHAR",
            "double": "DOUBLE", 
            "integer": "BIGINT",
            "timestamp": "TIMESTAMP",
            "boolean": "BOOLEAN"
        }
        
        schema = {}
        # Your YAML has 'columns' at the root
        for col in contract.get("columns", []):
            dtype = col.get("data_type", "varchar").lower()
            schema[col["name"]] = type_map.get(dtype, "VARCHAR")
            
        return schema

    def get_primary_key(self, table_name: str = "transactions") -> str:
        """
        REQUIRED BY: SchemaValidator (for uniqueness checks)
        Returns: The Primary Key column name, or None if not defined
        
        Looks for 'isPrimaryKey: true' in the YAML columns section.
        """
        contract = self._load_yaml(table_name)
        
        for col in contract.get("columns", []):
            if col.get("isPrimaryKey") is True:
                return col["name"]
        
        return None
    
    def get_quality_rules(self, table_name: str = "transactions") -> List[Dict[str, Any]]:
        """
        REQUIRED BY: DataProfiler
        Returns: A list of rules like [{'type': 'min_rows', ...}, {'type': 'notNull', ...}]
        """
        contract = self._load_yaml(table_name)
        rules = []
        
        # 1. Dataset-Level Rules (Volume)
        # Your YAML puts min_rows inside a 'quality' dict
        quality_section = contract.get("quality", {})
        if "min_rows" in quality_section:
            rules.append({
                "scope": "dataset",
                "type": "rowCount",
                "min": quality_section["min_rows"],
                "max": quality_section.get("max_rows", float("inf"))
            })

        # 2. Column-Level Rules
        for col in contract.get("columns", []):
            col_name = col["name"]
            
            # Rule: Not Null
            # Your YAML uses 'nullable: false'
            if col.get("nullable") is False:
                rules.append({
                    "scope": "column",
                    "column": col_name,
                    "type": "notNull"
                })

            # Rule: Range (min_value / max_value)
            # Your YAML uses 'min_value' keys directly
            if "min_value" in col or "max_value" in col:
                rules.append({
                    "scope": "column",
                    "column": col_name,
                    "type": "range",
                    "min": col.get("min_value", float("-inf")),
                    "max": col.get("max_value", float("inf"))
                })

            # Rule: Freshness (Implicit for timestamps)
            if col.get("data_type") == "timestamp":
                rules.append({
                    "scope": "column",
                    "column": col_name,
                    "type": "freshness",
                    "maxAge": "24h"
                })
        
        # 3. Custom SQL Rules (Consistency)
        # Look for 'custom_checks' in the quality section
        for check in quality_section.get("custom_checks", []):
            rules.append({
                "scope": "dataset",
                "type": "sql",
                "name": check.get("name", "Custom Check"),
                "query": check["sql_condition"],  # e.g. "amount < 100000"
                "severity": check.get("severity", "error")
            })
                
        return rules

# --- Quick Test ---
if __name__ == "__main__":
    parser = ContractParser()
    print("Schema Columns:", parser.get_schema_columns("transactions"))
    print("Primary Key:", parser.get_primary_key("transactions"))
    print("Quality Rules:", parser.get_quality_rules("transactions"))
