"""
Data Profiler Tool - Single-Pass SQL Aggregation for Data Quality

This tool performs single-pass SQL aggregation to calculate:
- Volume: Total row count
- Completeness: NULL counts per column
- Range: MIN/MAX values per column

It validates these metrics against ODCS contract rules.
"""

import duckdb
from pathlib import Path
from typing import List, Dict, Any
from src.utils.contract_parser import ContractParser


class DataProfiler:
    """
    The Mathematician - Analyzes data values using single-pass SQL aggregation.
    
    This tool doesn't care about column names; it cares about the VALUES inside them.
    It constructs one giant SQL query to calculate all metrics in milliseconds.
    """
    
    def __init__(self, contracts_path: str = "config/expectations"):
        """
        Initialize the DataProfiler.
        
        Args:
            contracts_path: Directory containing ODCS contract YAML files
        """
        self.contracts_path = contracts_path
        self.contract_parser = ContractParser(contracts_path)
        self.conn = duckdb.connect(":memory:")
    
    def analyze(self, file_path: str, table_name: str) -> List[str]:
        """
        Analyze a CSV file against its ODCS contract using single-pass SQL.
        
        Args:
            file_path: Path to the CSV file to analyze
            table_name: Name of the table/dataset (used to find contract)
            
        Returns:
            List of error strings. Empty list if all checks pass.
            
        Example output:
            [
                "❌ VOLUME: Found 5 rows, expected at least 100 rows",
                "❌ COMPLETENESS: Column 'user_id' has 3 NULL values (required field)",
                "❌ RANGE: Column 'amount' min value -50.0 is below allowed minimum 0.0",
                "❌ CONSISTENCY: Rule 'No Future Transactions' failed on 1 rows"
            ]
        """
        errors = []
        
        try:
            # Get quality rules from contract
            rules = self.contract_parser.get_quality_rules(table_name)
            schema_columns = self.contract_parser.get_schema_columns(table_name)
            
            # Build and execute single-pass SQL query
            profile_data = self._execute_profiling_query(file_path, rules, schema_columns)
            
            # Validate against rules (volume, completeness, range)
            errors.extend(self._validate_rules(profile_data, rules))
            
            # Execute custom SQL consistency checks
            errors.extend(self._validate_consistency_rules(file_path, rules))
            
        except FileNotFoundError as e:
            errors.append(f"⚠️ NO CONTRACT: {str(e)}")
        except Exception as e:
            errors.append(f"❌ ERROR: Failed to analyze data: {str(e)}")
        
        return errors
    
    def _execute_profiling_query(self, file_path: str, rules: List[Dict], schema_columns: Dict[str, str]) -> Dict[str, Any]:
        """
        Construct and execute a single-pass SQL query to calculate all metrics.
        
        This is the "magic" - one query to rule them all!
        
        Args:
            file_path: Path to CSV file
            rules: List of quality rules from contract
            schema_columns: Expected columns from contract
            
        Returns:
            Dictionary with profiling results
        """
        # First, get the actual columns in the CSV
        actual_columns_query = f"SELECT * FROM read_csv_auto('{file_path}') LIMIT 0"
        actual_columns_raw = [desc[0] for desc in self.conn.execute(actual_columns_query).description]
        actual_columns = [col.lower() for col in actual_columns_raw]
        
        # Build SELECT clauses for each metric
        select_clauses = ["COUNT(*) as total_rows"]
        columns_to_profile = []
        
        # Determine which columns need profiling based on rules
        required_columns = set()
        range_columns = {}
        
        for rule in rules:
            if rule['scope'] == 'column':
                col_name = rule['column']
                required_columns.add(col_name)
                
                if rule['type'] == 'range':
                    range_columns[col_name] = rule
        
        # Profile each column
        for col_name in schema_columns.keys():
            if col_name.lower() not in actual_columns:
                # Column doesn't exist - we'll handle this in validation
                continue
            
            columns_to_profile.append(col_name)
            
            # Count non-null values
            select_clauses.append(f"COUNT({col_name}) as {col_name}_count")
            
            # Calculate MIN/MAX for columns with range rules
            if col_name in range_columns:
                select_clauses.append(f"MIN({col_name}) as {col_name}_min")
                select_clauses.append(f"MAX({col_name}) as {col_name}_max")
        
        # Construct the full query
        query = f"""
            SELECT {', '.join(select_clauses)}
            FROM read_csv_auto('{file_path}')
        """
        
        # Execute query
        result = self.conn.execute(query).fetchone()
        
        # Parse results into dictionary
        profile_data = {
            'total_rows': result[0],
            'columns': {},
            'missing_columns': [col for col in schema_columns.keys() if col.lower() not in actual_columns]
        }
        
        idx = 1
        for col_name in columns_to_profile:
            profile_data['columns'][col_name] = {
                'count': result[idx],
                'null_count': result[0] - result[idx]  # total_rows - non_null_count
            }
            idx += 1
            
            # Add min/max if they were calculated
            if col_name in range_columns:
                profile_data['columns'][col_name]['min'] = result[idx]
                profile_data['columns'][col_name]['max'] = result[idx + 1]
                idx += 2
        
        return profile_data
    
    def _validate_rules(self, profile_data: Dict[str, Any], rules: List[Dict]) -> List[str]:
        """
        Validate profile data against all rules from the contract.
        
        Args:
            profile_data: Results from profiling query
            rules: List of quality rules
            
        Returns:
            List of error messages
        """
        errors = []
        
        for rule in rules:
            if rule['scope'] == 'dataset':
                # Dataset-level rules (volume)
                if rule['type'] == 'rowCount':
                    total_rows = profile_data['total_rows']
                    min_rows = rule.get('min', 0)
                    max_rows = rule.get('max', float('inf'))
                    
                    if total_rows < min_rows:
                        errors.append(
                            f"❌ VOLUME: Found {total_rows} rows, expected at least {min_rows} rows"
                        )
                    elif max_rows != float('inf') and total_rows > max_rows:
                        errors.append(
                            f"❌ VOLUME: Found {total_rows} rows, expected at most {max_rows} rows"
                        )
            
            elif rule['scope'] == 'column':
                col_name = rule['column']
                
                # Check if column is missing
                if col_name in profile_data['missing_columns']:
                    if rule['type'] == 'notNull':
                        errors.append(
                            f"❌ COMPLETENESS: Column '{col_name}' is missing from dataset (required field)"
                        )
                    continue
                
                col_profile = profile_data['columns'].get(col_name, {})
                
                # Completeness rules
                if rule['type'] == 'notNull':
                    null_count = col_profile.get('null_count', 0)
                    if null_count > 0:
                        errors.append(
                            f"❌ COMPLETENESS: Column '{col_name}' has {null_count} NULL values (required field)"
                        )
                
                # Range rules
                elif rule['type'] == 'range':
                    actual_min = col_profile.get('min')
                    actual_max = col_profile.get('max')
                    
                    allowed_min = rule.get('min', float('-inf'))
                    allowed_max = rule.get('max', float('inf'))
                    
                    if actual_min is not None and actual_min < allowed_min:
                        errors.append(
                            f"❌ RANGE: Column '{col_name}' min value {actual_min} is below allowed minimum {allowed_min}"
                        )
                    
                    if actual_max is not None and actual_max > allowed_max:
                        errors.append(
                            f"❌ RANGE: Column '{col_name}' max value {actual_max} exceeds allowed maximum {allowed_max}"
                        )
        
        return errors
    
    def _validate_consistency_rules(self, file_path: str, rules: List[Dict]) -> List[str]:
        """
        Execute custom SQL consistency checks from the contract.
        
        These are business logic rules like:
        - start_date < end_date
        - timestamp <= now()
        - amount < 5000 OR (amount >= 5000 AND status = 'COMPLETED')
        
        Args:
            file_path: Path to the CSV file
            rules: List of quality rules from contract
            
        Returns:
            List of error messages for failing consistency checks
        """
        errors = []
        
        for rule in rules:
            # Only process SQL-type rules
            if rule.get('type') == 'sql' and rule.get('scope') == 'dataset':
                try:
                    rule_name = rule.get('name', 'Custom Check')
                    sql_condition = rule['query']
                    
                    # Count rows that VIOLATE the rule (i.e., NOT meeting the condition)
                    query = f"""
                        SELECT COUNT(*) 
                        FROM read_csv_auto('{file_path}')
                        WHERE NOT ({sql_condition})
                    """
                    
                    failing_rows = self.conn.execute(query).fetchone()[0]
                    
                    if failing_rows > 0:
                        errors.append(
                            f"❌ CONSISTENCY: Rule '{rule_name}' failed on {failing_rows} rows"
                        )
                        
                except Exception as e:
                    errors.append(
                        f"❌ CONSISTENCY: Failed to execute rule '{rule_name}': {str(e)}"
                    )
        
        return errors


if __name__ == '__main__':
    # Example usage
    profiler = DataProfiler()
    
    # Analyze a file
    errors = profiler.analyze('data/landing/transactions_perfect.csv', 'transactions')
    
    if not errors:
        print("✅ All data quality checks passed!")
    else:
        print("❌ Data quality issues found:")
        for error in errors:
            print(f"   {error}")
