"""
Tool C: SchemaValidatorTool (ODCS v3.1.0 Compliant)
Validates DataFrame against Open Data Contract Standard v3.1.0 YAML.
"""
import pandas as pd
import yaml
from typing import Dict, Any, List, Optional

class SchemaValidatorTool:
    """Validates data against ODCS v3.1.0 contract."""
    
    def run(self, df: pd.DataFrame, yaml_config_path: str, table_name: str = None) -> Dict[str, Any]:
        """
        Validate DataFrame against ODCS v3.1.0 schema.
        
        Args:
            df: DataFrame to validate
            yaml_config_path: Path to ODCS-compliant YAML file
            table_name: Optional name of table to validate against (if contract has multiple)
            
        Returns:
            Validation report.
        """
        # Load YAML config
        with open(yaml_config_path, 'r') as f:
            contract = yaml.safe_load(f)
        
        violations = []
        has_critical = False
        
        # Parse ODCS v3.1.0 Structure
        # Root -> schema (list) -> [table_obj]
        schema_list = contract.get('schema', [])
        if not schema_list:
             return {
                "status": "FAIL",
                "violations": [],
                "decision": "CRITICAL_STOP",
                "summary": "No schema definition found in contract"
            }
            
        # Find matching table definition
        table_def = None
        if table_name:
            for schema_obj in schema_list:
                if schema_obj.get('name') == table_name or schema_obj.get('physicalName') == table_name:
                    table_def = schema_obj
                    break
        
        # Fallback: Use first if not found or not specified (backward compatibility)
        if not table_def:
            if table_name:
                print(f"⚠️  Warning: Schema for table '{table_name}' not found in contract. Using first available schema.")
            table_def = schema_list[0]

        properties = table_def.get('properties', [])
        
        # Build expectation map
        # key: col_name, value: property_dict
        expected_cols = {prop['name']: prop for prop in properties}
        actual_cols = set(df.columns)
        
        # Check Strict Mode (Custom Property)
        strict_mode = contract.get('strictMode', False) or contract.get('strict', False)
        
        # 1. Missing Columns
        for col_name, prop in expected_cols.items():
            is_required = prop.get('required', False)
            severity = "CRITICAL" if is_required else "WARNING"
            
            # Strict Mode Escalation
            if strict_mode and severity == 'WARNING':
                severity = 'CRITICAL'
            
            if col_name not in actual_cols:
                violations.append({
                    "column": col_name,
                    "issue": "missing",
                    "severity": severity,
                    "expected": "column to exist",
                    "actual": "column not found"
                })
                if severity == "CRITICAL":
                    has_critical = True

        # 2. Extra Columns & Evolution Suggestion
        suggested_updates = []
        for col_name in actual_cols:
            if col_name not in expected_cols:
                severity = "WARNING"
                if strict_mode:
                    severity = "CRITICAL"
                    
                violations.append({
                    "column": col_name,
                    "issue": "extra",
                    "severity": severity,
                    "expected": "not defined in schema",
                    "actual": "column exists"
                })
                if severity == "CRITICAL": 
                    has_critical = True
                
                # Generate Suggestion for Evolution
                # Infer type
                curr_dtype = str(df[col_name].dtype)
                inferred_type = "string" # Default
                if "int" in curr_dtype: inferred_type = "int64"
                elif "float" in curr_dtype: inferred_type = "float64"
                elif "bool" in curr_dtype: inferred_type = "bool"
                elif "date" in curr_dtype: inferred_type = "timestamp"
                
                suggested_updates.append({
                    "name": col_name,
                    "physicalType": inferred_type,
                    "quality": [],
                    "description": "Automatically detected column"
                })
                
        # 3. Type & Quality Checks
        for col_name, prop in expected_cols.items():
            if col_name not in actual_cols:
                continue
                
            # Type Check
            physical_type = prop.get('physicalType')
            if physical_type:
                severity = "CRITICAL" # Types usually critical
                # But if some logic allowed warning types? 
                # Strict mode ensures it stays critical.
                
                actual_type = str(df[col_name].dtype)
                if not self._types_match(physical_type, actual_type):
                    violations.append({
                        "column": col_name,
                        "issue": "type_mismatch",
                        "severity": severity,
                        "expected": physical_type,
                        "actual": actual_type
                    })
                    has_critical = True
            
            # Quality Rules
            quality_rules = prop.get('quality', [])
            for rule in quality_rules:
                metric = rule.get('metric')
                must_be = rule.get('mustBe')
                rule_severity = rule.get('severity', 'WARNING').upper()
                
                if strict_mode and rule_severity == 'WARNING':
                    rule_severity = 'CRITICAL'
                
                check_result = self._check_rule(df, col_name, metric, must_be)
                if check_result:
                    check_result['severity'] = rule_severity
                    violations.append(check_result)
                    if rule_severity == "CRITICAL":
                        has_critical = True

        # Determine final decision
        if has_critical:
            status = "FAIL"
            decision = "CRITICAL_STOP"
        elif violations:
            status = "PASS_WITH_WARNINGS"
            decision = "CONTINUE"
        else:
            status = "PASS"
            decision = "CONTINUE"
        
        return {
            "status": status,
            "violations": violations,
            "decision": decision,
            "summary": f"{len(violations)} issues found ({sum(1 for v in violations if v['severity'] == 'CRITICAL')} critical)",
            "suggested_updates": suggested_updates # For Evolution UI
        }

    def _types_match(self, expected: str, actual: str) -> bool:
        """Map ODCS physical types to Pandas dtypes."""
        type_map = {
            'int': ['int64', 'int32', 'int16', 'int8'],
            'int64': ['int64'],
            'float': ['float64', 'float32'],
            'float64': ['float64'],
            'string': ['object', 'string'],
            'varchar': ['object'],
            'bool': ['bool'],
            'timestamp': ['datetime64[ns]', 'object'],
            'date': ['datetime64[ns]', 'object']
        }
        
        expected_lower = expected.lower()
        if '(' in expected_lower:
            expected_lower = expected_lower.split('(')[0]

        for key, values in type_map.items():
            if expected_lower == key:
                return any(v in actual for v in values)
        
        return expected.lower() in actual.lower()

    def _check_rule(self, df: pd.DataFrame, col_name: str, metric: str, must_be: Any) -> Dict:
        """Evaluate a single ODCS quality rule."""
        
        # Null Values
        if metric == 'nullValues':
            null_count = df[col_name].isna().sum()
            total_count = len(df)
            null_pct = (null_count / total_count) * 100
            
            threshold_pct = 0.0
            
            if isinstance(must_be, str) and '%' in must_be:
                threshold_pct = float(must_be.strip('%'))
            elif isinstance(must_be, (int, float)):
                if must_be == 0:
                     # Exact 0 count required
                     if null_count > 0:
                        return {
                            "column": col_name,
                            "issue": "quality_rule",
                            "expected": "0 nulls",
                            "actual": f"{null_count} nulls"
                        }
                     return None
                else: 
                     # If non-zero number provided, assume it is Percentage (convention)
                     threshold_pct = float(must_be)
            
            if null_pct > threshold_pct:
                 return {
                    "column": col_name,
                    "issue": "quality_rule",
                    "expected": f"nulls <= {must_be}",
                    "actual": f"{null_pct:.1f}%"
                }

        # Unique Values
        elif metric == 'unique':
            if must_be is True:
                duplicates = df[col_name].duplicated().sum()
                if duplicates > 0:
                    return {
                        "column": col_name,
                        "issue": "quality_rule",
                        "expected": "unique values",
                        "actual": f"{duplicates} duplicates"
                    }

        # Min Value
        elif metric == 'minValue':
            min_val = df[col_name].min()
            if min_val < must_be:
                return {
                    "column": col_name,
                    "issue": "quality_rule",
                    "expected": f"min >= {must_be}",
                    "actual": f"min = {min_val}"
                }
                
        return None
