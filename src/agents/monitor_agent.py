"""
Monitor Agent - Deterministic Data Quality Orchestrator

This agent orchestrates all data quality checks before data is loaded into Apache Doris.
It acts as a preloader validation layer ensuring data meets all quality standards.

The Monitor Agent performs:
1. Timeliness Check - Ensures files are fresh and not stale
2. Schema Validation - Validates structure and uniqueness
3. Data Profiling - Checks volume, completeness, range, and consistency

This is a deterministic agent - all checks are rule-based, not statistical.
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from src.tools.schema_validator import SchemaValidator
from src.tools.data_profiler import DataProfiler
from src.agents.file_actuator import FileActuator


class MonitorAgent:
    """
    Deterministic Monitor Agent for Data Quality Checks.
    
    This agent coordinates all quality checks and provides a unified interface
    for validating data before it enters Apache Doris.
    """
    
    def __init__(self, contracts_path: str = "config/expectations"):
        """
        Initialize the Monitor Agent.
        
        Args:
            contracts_path: Directory containing ODCS contract YAML files
        """
        self.contracts_path = contracts_path
        self.schema_validator = SchemaValidator
        self.data_profiler = DataProfiler(contracts_path=contracts_path)
        self.actuator = FileActuator()  # Physical file movement system
    
    def check_timeliness(self, file_path: str, max_age_hours: int = 24) -> Tuple[bool, Optional[str]]:
        """
        Check if a file is fresh enough to process.
        
        This is a straightforward check: if the file is older than the threshold,
        it's considered stale and should not be processed.
        
        Args:
            file_path: Path to the file to check
            max_age_hours: Maximum allowed age in hours (default: 24)
            
        Returns:
            Tuple of (is_fresh, error_message)
            - (True, None) if file is fresh
            - (False, error_message) if file is stale
            
        Example:
            >>> agent = MonitorAgent()
            >>> is_fresh, error = agent.check_timeliness('data/transactions.csv')
            >>> if not is_fresh:
            ...     print(error)
            ❌ TIMELINESS: File is 48.5 hours old, exceeds maximum age of 24.0 hours
        """
        try:
            # Get file modification time
            file_stats = os.stat(file_path)
            file_mtime = datetime.fromtimestamp(file_stats.st_mtime)
            
            # Calculate age
            current_time = datetime.now()
            file_age = current_time - file_mtime
            file_age_hours = file_age.total_seconds() / 3600
            
            # Check if file is too old
            if file_age_hours >= max_age_hours:
                error_msg = (
                    f"❌ TIMELINESS: File is {file_age_hours:.1f} hours old, "
                    f"exceeds or equals maximum age of {max_age_hours:.1f} hours"
                )
                return False, error_msg
            
            # File is fresh
            return True, None
            
        except FileNotFoundError:
            return False, f"❌ TIMELINESS: File not found: {file_path}"
        except Exception as e:
            return False, f"❌ TIMELINESS: Error checking file age: {str(e)}"
    
    def validate_schema(self, file_path: str, table_name: str, 
                       contract_path: Optional[str] = None) -> List[str]:
        """
        Validate file schema against ODCS contract.
        
        Checks:
        - Column existence (schema mismatch)
        - Schema drift (unexpected columns)
        - Primary key uniqueness
        
        Args:
            file_path: Path to the file to validate
            table_name: Name of the table/dataset
            contract_path: Optional specific contract path (uses table_name if not provided)
            
        Returns:
            List of error/warning messages. Empty list if validation passes.
        """
        if contract_path is None:
            contract_path = Path(self.contracts_path) / f"{table_name}.yaml"
        
        validator = self.schema_validator(contract_path)
        result = validator.validate_file(file_path, file_format="csv")
        
        # Format issues into error messages
        errors = []
        for issue in result.issues:
            if issue.issue_type == "missing_column":
                errors.append(f"❌ SCHEMA MISMATCH: Missing required column '{issue.column}'")
            elif issue.issue_type == "unexpected_column":
                errors.append(f"⚠️ SCHEMA DRIFT: New column detected '{issue.column}' (Not in contract)")
            elif issue.issue_type == "uniqueness_violation":
                errors.append(f"❌ UNIQUENESS: Found {issue.actual.split(' ')[0]} duplicate IDs in Primary Key '{issue.column}'")
            elif issue.issue_type == "type_mismatch":
                errors.append(f"❌ TYPE MISMATCH: Column '{issue.column}' expected {issue.expected}, got {issue.actual}")
        
        return errors
    
    def profile_data(self, file_path: str, table_name: str) -> List[str]:
        """
        Profile data and validate against quality rules.
        
        Checks:
        - Volume (row count)
        - Completeness (NULL values)
        - Range (min/max values)
        - Consistency (business logic rules)
        
        Args:
            file_path: Path to the file to profile
            table_name: Name of the table/dataset
            
        Returns:
            List of error messages. Empty list if all checks pass.
        """
        return self.data_profiler.analyze(file_path, table_name)
    
    def run_all_checks(self, file_path: str, table_name: str, 
                      max_age_hours: int = 24) -> Dict[str, any]:
        """
        Run all data quality checks on a file.
        
        This is the main entry point for the Monitor Agent. It orchestrates
        all checks in the correct order:
        1. Timeliness (fail fast if file is stale)
        2. Schema Validation
        3. Data Profiling
        
        Args:
            file_path: Path to the file to check
            table_name: Name of the table/dataset
            max_age_hours: Maximum allowed file age in hours
            
        Returns:
            Dictionary with check results:
            {
                'file': str,
                'table': str,
                'timestamp': str,
                'timeliness': {'passed': bool, 'error': Optional[str]},
                'schema': {'passed': bool, 'errors': List[str]},
                'profiling': {'passed': bool, 'errors': List[str]},
                'overall_status': 'PASS' | 'FAIL',
                'can_load': bool
            }
        """
        results = {
            'file': file_path,
            'table': table_name,
            'timestamp': datetime.now().isoformat(),
            'timeliness': {},
            'schema': {},
            'profiling': {},
            'overall_status': 'FAIL',
            'can_load': False
        }
        
        print("\n" + "=" * 80)
        print(f"🔍 MONITOR AGENT: Data Quality Check for '{table_name}'")
        print("=" * 80)
        print(f"📁 File: {file_path}")
        print(f"⏰ Timestamp: {results['timestamp']}")
        print()
        
        # 1. Timeliness Check (fail fast)
        print("1️⃣  TIMELINESS CHECK")
        print("-" * 80)
        is_fresh, timeliness_error = self.check_timeliness(file_path, max_age_hours)
        results['timeliness'] = {'passed': is_fresh, 'error': timeliness_error}
        
        if is_fresh:
            print(f"✅ File is fresh (< {max_age_hours} hours old)")
        else:
            print(timeliness_error)
            print("\n" + "=" * 80)
            print("❌ OVERALL STATUS: FAIL - File too old, cannot proceed")
            print("=" * 80)
            return results
        
        # 2. Schema Validation
        print("\n2️⃣  SCHEMA VALIDATION")
        print("-" * 80)
        schema_errors = self.validate_schema(file_path, table_name)
        results['schema'] = {'passed': len(schema_errors) == 0, 'errors': schema_errors}
        
        if schema_errors:
            for error in schema_errors:
                print(error)
        else:
            print("✅ Schema validation passed")
        
        # 3. Data Profiling
        print("\n3️⃣  DATA PROFILING")
        print("-" * 80)
        profiling_errors = self.profile_data(file_path, table_name)
        results['profiling'] = {'passed': len(profiling_errors) == 0, 'errors': profiling_errors}
        
        if profiling_errors:
            for error in profiling_errors:
                print(error)
        else:
            print("✅ Data profiling passed")
        
        # Overall Status
        print("\n" + "=" * 80)
        all_checks_passed = (
            results['timeliness']['passed'] and
            results['schema']['passed'] and
            results['profiling']['passed']
        )
        
        if all_checks_passed:
            results['overall_status'] = 'PASS'
            results['can_load'] = True
            print("✅ OVERALL STATUS: PASS - Data ready for Apache Doris")
        else:
            results['overall_status'] = 'FAIL'
            results['can_load'] = False
            
            # Count total errors
            total_errors = len(schema_errors) + len(profiling_errors)
            if not is_fresh:
                total_errors += 1
            
            print(f"❌ OVERALL STATUS: FAIL - Found {total_errors} issue(s)")
            print("   Data cannot be loaded into Apache Doris until issues are resolved")
        
        print("=" * 80)
        
        return results
    
    def process_file(self, file_path: str, table_name: str, 
                    max_age_hours: int = 24, take_action: bool = True) -> Dict[str, any]:
        """
        Process a file: Run all checks AND take action (move file).
        
        This is the complete workflow:
        1. Run all data quality checks
        2. Based on results, PHYSICALLY MOVE the file:
           - ✅ PASS → data/staging/ (ready for Doris)
           - ❌ FAIL → data/quarantine/ (requires human review)
        
        This prevents:
        - Re-scanning the same bad files repeatedly
        - Polluting the Agentic Brain's memory
        - Bad data from entering Apache Doris
        
        Args:
            file_path: Path to the file to process
            table_name: Name of the table/dataset
            max_age_hours: Maximum allowed file age in hours
            take_action: If True, physically move files (default: True)
            
        Returns:
            Dictionary with check results and action taken
        """
        # Run all quality checks (Detector)
        results = self.run_all_checks(file_path, table_name, max_age_hours)
        
        # Take action based on results (Actuator)
        if take_action:
            print("\n" + "🔧" * 40)
            print("ACTUATOR: Taking Physical Action...")
            print("🔧" * 40)
            
            if results['can_load']:
                # Move to staging
                new_path = self.actuator.move_to_staging(file_path, results)
                results['action_taken'] = 'MOVED_TO_STAGING'
                results['new_location'] = str(new_path)
                print(f"\n🎯 RESULT: File approved and moved to STAGING")
                print(f"   Apache Doris can now safely load this file from: {new_path}")
            else:
                # Move to quarantine
                new_path = self.actuator.move_to_quarantine(file_path, results)
                results['action_taken'] = 'MOVED_TO_QUARANTINE'
                results['new_location'] = str(new_path)
                print(f"\n🚫 RESULT: File quarantined for human review")
                print(f"   File will NOT enter Apache Doris")
                print(f"   Review error report at: {new_path}.error.json")
            
            print("🔧" * 40)
        else:
            results['action_taken'] = 'NO_ACTION'
            results['new_location'] = file_path
        
        return results


if __name__ == '__main__':
    # Example usage
    agent = MonitorAgent()
    
    # Run all checks on a perfect file
    print("\n📊 Example 1: Perfect Transaction Data")
    results = agent.run_all_checks(
        file_path='data/landing/transactions_perfect.csv',
        table_name='transactions',
        max_age_hours=24
    )
    
    print(f"\n🎯 Can Load to Doris: {results['can_load']}")
