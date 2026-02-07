"""
Tool 6: ConsistencyCheckTool (Cross-Table Validator)
Validates foreign key relationships (Referential Integrity) between datasets.
"""
import pandas as pd
from typing import Dict, Any, List

class ConsistencyCheckTool:
    """Checks for orphan records by validating foreign keys defined in the data contract."""
    
    def __init__(self, ref_path: str = "data"):
        self.ref_path = ref_path
    
    def run(self, df: pd.DataFrame, table_name: str, contract: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Validate referential integrity using contract definitions.
        
        Args:
            df: The dataframe to validate (child table)
            table_name: The name of the table (e.g., 'transactions')
            contract: The full contract dictionary containing the schema and foreignKeys.
            
        Returns:
            Dict with status, orphan_count, and sample invalid IDs.
        """
        if not contract:
            return {
                "status": "SKIPPED",
                "message": "No contract provided to check FKs",
                "orphans": []
            }
            
        # 1. Find the specific table schema in the contract
        schemas = contract.get('schema', [])
        target_schema = None
        
        # If contract has multiple schemas, find the ranking one
        for s in schemas:
            if s.get('name') == table_name:
                target_schema = s
                break
        
        # Fallback: if only one schema exists, assume it's the one (common single-file pattern)
        if not target_schema and len(schemas) == 1:
            target_schema = schemas[0]
            
        if not target_schema:
            return {
                "status": "SKIPPED",
                "message": f"Schema for '{table_name}' not found in contract",
                "orphans": []
            }

        # 2. Check for Foreign Keys
        fks = target_schema.get('foreignKeys', [])
        if not fks:
            return {
                "status": "SKIPPED", 
                "message": "No foreign keys defined in contract",
                "orphans": []
            }
            
        # 3. Validate each FK (Currently supports 1st FK for demo simplicity, can loop)
        # TODO: Loop through all FKs. For now, we take the first one.
        fk_def = fks[0]
        
        # Parse definition
        try:
            fk_col = fk_def['columns'][0] # Source column
            ref_table = fk_def['referenceTable'] # Target table name (e.g. 'users')
            ref_pk = fk_def['referenceColumns'][0] # Target column
        except (KeyError, IndexError):
             return {
                "status": "ERROR", 
                "message": "Invalid foreignKey definition in YAML",
                "orphans": []
            }
        
        # 4. Perform the check
        ref_file = f"{self.ref_path}/{ref_table}.csv"
        
        # Check if FK column exists in source
        if fk_col not in df.columns:
            return {
                "status": "FAIL", 
                "message": f"FK Column '{fk_col}' missing in source",
                "orphans": []
            }
            
        try:
            # Load reference table
            ref_df = pd.read_csv(ref_file)
            
            # Check if PK exists in ref
            if ref_pk not in ref_df.columns:
                return {
                    "status": "ERROR",
                    "message": f"Reference column '{ref_pk}' missing in {ref_table}.csv",
                    "orphans": []
                }
            
            # Get valid IDs
            # Convert to string to ensure type matching (common issue: int vs str)
            valid_ids = set(ref_df[ref_pk].astype(str).unique())
            
            # Find orphans
            non_null_fk = df[df[fk_col].notna()].copy()
            non_null_fk['fk_str'] = non_null_fk[fk_col].astype(str)
            
            orphans = non_null_fk[~non_null_fk['fk_str'].isin(valid_ids)]
            orphan_count = len(orphans)
            orphan_ids = orphans[fk_col].unique().tolist()[:5] # Sample of first 5
            
            status = "PASS"
            if orphan_count > 0:
                status = "FAIL"
                
            return {
                "status": status,
                "relationship": f"{table_name}.{fk_col} -> {ref_table}.{ref_pk}",
                "orphan_count": orphan_count,
                "orphan_pct": (orphan_count / len(df)) * 100,
                "sample_orphans": orphan_ids,
                "decision": "CRITICAL_STOP" if orphan_count > 0 else "CONTINUE"
            }
            
        except FileNotFoundError:
             return {
                "status": "ERROR",
                "message": f"Reference dataset '{ref_table}.csv' not found",
                "orphans": []
            }
        except Exception as e:
            return {
                "status": "ERROR", 
                "message": f"Consistency check failed: {str(e)}",
                "orphans": []
            }
