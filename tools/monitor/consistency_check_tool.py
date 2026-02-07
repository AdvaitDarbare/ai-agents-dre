
"""
Tool 6: ConsistencyCheckTool (Cross-Table Validator)
Validates foreign key relationships (Referential Integrity) between datasets.
"""
import pandas as pd
from typing import Dict, Any, List

class ConsistencyCheckTool:
    """Checks for orphan records by validating foreign keys against reference tables."""
    
    def __init__(self, ref_path: str = "data"):
        self.ref_path = ref_path
    
    def run(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Validate referential integrity.
        
        Args:
            df: The dataframe to validate (child table)
            table_name: The name of the table (e.g., 'transactions')
            
        Returns:
            Dict with status, orphan_count, and sample invalid IDs.
        """
        # Define relationships (In a real app, this would be in config.yaml)
        relationships = {
            'transactions': {
                'fk_column': 'user_id',
                'ref_table': 'users.csv',
                'ref_pk': 'user_id'
            }
        }
        
        if table_name not in relationships:
            return {
                "status": "SKIPPED",
                "message": f"No relationships defined for table '{table_name}'",
                "orphans": []
            }
            
        rel = relationships[table_name]
        fk_col = rel['fk_column']
        ref_file = f"{self.ref_path}/{rel['ref_table']}"
        ref_pk = rel['ref_pk']
        
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
            
            # Get valid IDs
            valid_ids = set(ref_df[ref_pk].unique())
            
            # Find orphans (records in df where fk is NOT in valid_ids)
            # Handle potential type mismatch (e.g. int vs string) by converting to string for comparison
            # But usually pandas handles int/int well. Let's try strictly.
            
            # Filter out nulls first (null FK might be allowed depending on schema, but usually checked elsewhere)
            non_null_fk = df[df[fk_col].notna()]
            
            orphans = non_null_fk[~non_null_fk[fk_col].isin(valid_ids)]
            orphan_count = len(orphans)
            orphan_ids = orphans[fk_col].unique().tolist()[:5] # Sample of first 5
            
            status = "PASS"
            if orphan_count > 0:
                status = "FAIL"
                
            return {
                "status": status,
                "relationship": f"{table_name}.{fk_col} -> {rel['ref_table']}.{ref_pk}",
                "orphan_count": orphan_count,
                "orphan_pct": (orphan_count / len(df)) * 100,
                "sample_orphans": orphan_ids,
                "decision": "CRITICAL_STOP" if orphan_count > 0 else "CONTINUE"
            }
            
        except FileNotFoundError:
             return {
                "status": "ERROR",
                "message": f"Reference file '{ref_file}' not found",
                "orphans": []
            }
        except Exception as e:
            return {
                "status": "ERROR", 
                "message": str(e),
                "orphans": []
            }
