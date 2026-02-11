"""
Schema Validator Tool for Data Quality Monitoring

This tool validates data schema validity by checking:
1. Column presence (expected columns exist)
2. Data types (columns have correct data types)
3. Uniqueness (Primary Key duplicates)

Uses DuckDB DESCRIBE to introspect data and compares against YAML schema definitions.
"""

import duckdb
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
from enum import Enum


class ValidationStatus(Enum):
    """Validation result status."""
    PASS = "pass"
    FAIL = "fail"
    WARNING = "warning"


class DataType(Enum):
    """Supported data types for validation."""
    # Numeric types
    INTEGER = "integer"
    BIGINT = "bigint"
    SMALLINT = "smallint"
    TINYINT = "tinyint"
    HUGEINT = "hugeint"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    
    # String types
    VARCHAR = "varchar"
    STRING = "string"
    TEXT = "text"
    
    # Boolean
    BOOLEAN = "boolean"
    
    # Date/Time types
    DATE = "date"
    TIMESTAMP = "timestamp"
    TIME = "time"
    INTERVAL = "interval"
    
    # Complex types
    BLOB = "blob"
    JSON = "json"
    LIST = "list"
    STRUCT = "struct"


@dataclass
class ColumnSchema:
    """Schema definition for a single column."""
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None


@dataclass
class TableSchema:
    """Schema definition for a table."""
    table_name: str
    columns: List[ColumnSchema]
    description: Optional[str] = None


@dataclass
class ValidationIssue:
    """Represents a schema validation issue."""
    severity: ValidationStatus
    column: Optional[str]
    issue_type: str
    message: str
    expected: Optional[str] = None
    actual: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of schema validation."""
    status: ValidationStatus
    table_name: str
    issues: List[ValidationIssue] = field(default_factory=list)
    passed_checks: int = 0
    failed_checks: int = 0
    
    @property
    def is_valid(self) -> bool:
        """Returns True if validation passed."""
        return self.status == ValidationStatus.PASS
    
    def add_issue(self, issue: ValidationIssue) -> None:
        """Add a validation issue."""
        self.issues.append(issue)
        if issue.severity == ValidationStatus.FAIL:
            self.failed_checks += 1
            self.status = ValidationStatus.FAIL
        elif issue.severity == ValidationStatus.WARNING and self.status != ValidationStatus.FAIL:
            self.status = ValidationStatus.WARNING
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert validation result to dictionary."""
        return {
            "status": self.status.value,
            "table_name": self.table_name,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "total_issues": len(self.issues),
            "schema_diff": self.get_schema_diff(),
            "issues": [
                {
                    "severity": issue.severity.value,
                    "column": issue.column,
                    "issue_type": issue.issue_type,
                    "message": issue.message,
                    "expected": issue.expected,
                    "actual": issue.actual
                }
                for issue in self.issues
            ]
        }

    def get_schema_diff(self) -> Dict[str, List[str]]:
        """
        Return a structured diff of the schema validation.
        
        Returns:
            Dict with keys: missing_columns, new_columns, type_mismatches
        """
        diff = {
            "missing_columns": [],
            "new_columns": [],
            "type_mismatches": []
        }
        
        for issue in self.issues:
            if issue.issue_type == "missing_column":
                diff["missing_columns"].append(issue.column)
            elif issue.issue_type == "unexpected_column":
                diff["new_columns"].append(issue.column)
            elif issue.issue_type == "type_mismatch":
                diff["type_mismatches"].append({
                    "column": issue.column,
                    "expected": issue.expected,
                    "actual": issue.actual
                })
                
        return diff


class SchemaValidator:
    """
    Validates data schema against YAML schema definitions using DuckDB.
    
    This is a deterministic validation tool that checks:
    - Column existence
    - Data type correctness
    """
    
    # Map DuckDB types to our normalized types
    TYPE_MAPPINGS = {
        "INTEGER": ["integer", "int", "int4", "bigint", "int8"],  # Allow bigint as compatible with integer
        "BIGINT": ["bigint", "int8", "long", "integer", "int"],  # Allow integer as compatible with bigint
        "SMALLINT": ["smallint", "int2", "short"],
        "TINYINT": ["tinyint", "int1"],
        "HUGEINT": ["hugeint"],
        "FLOAT": ["float", "float4", "real"],
        "DOUBLE": ["double", "float8", "numeric"],
        "DECIMAL": ["decimal", "numeric"],
        "VARCHAR": ["varchar", "string", "text", "char"],
        "BOOLEAN": ["boolean", "bool"],
        "DATE": ["date"],
        "TIMESTAMP": ["timestamp", "datetime"],
        "TIME": ["time"],
        "INTERVAL": ["interval"],
        "BLOB": ["blob", "binary", "varbinary"],
        "JSON": ["json"],
    }
    
    def __init__(self, schema_path: Union[str, Path], conn: Optional[duckdb.DuckDBPyConnection] = None):
        """
        Initialize the schema validator.
        
        Args:
            schema_path: Path to YAML schema definition file
            conn: Optional DuckDB connection (creates new one if not provided)
        """
        self.schema_path = Path(schema_path)
        self.conn = conn if conn is not None else duckdb.connect(":memory:")
        self.schema = self._load_schema()
        
        # Import ContractParser for PK lookup
        try:
            from src.utils.contract_parser import ContractParser
            # Extract table name from schema path (e.g., transactions.yaml -> transactions)
            table_name = self.schema_path.stem
            contracts_dir = self.schema_path.parent
            self.parser = ContractParser(str(contracts_dir))
            self.primary_key = self.parser.get_primary_key(table_name)
        except Exception:
            # If ContractParser not available, set primary_key to None
            self.primary_key = None
    
    def _load_schema(self) -> TableSchema:
        """Load and parse YAML schema definition."""
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")
        
        with open(self.schema_path, 'r') as f:
            schema_data = yaml.safe_load(f)
        
        # Parse columns
        columns = []
        for col_def in schema_data.get('columns', []):
            columns.append(ColumnSchema(
                name=col_def['name'],
                data_type=col_def['data_type'].lower(),
                nullable=col_def.get('nullable', True),
                description=col_def.get('description')
            ))
        
        return TableSchema(
            table_name=schema_data.get('table_name', 'unknown'),
            columns=columns,
            description=schema_data.get('description')
        )
    
    def _normalize_type(self, duckdb_type: str) -> str:
        """
        Normalize DuckDB type to a standard type for comparison.
        
        Args:
            duckdb_type: DuckDB type string (e.g., "INTEGER", "VARCHAR")
            
        Returns:
            Normalized type string
        """
        duckdb_type_upper = duckdb_type.upper().split('(')[0]  # Remove precision/scale
        
        for normalized, variants in self.TYPE_MAPPINGS.items():
            if duckdb_type_upper == normalized:
                return normalized.lower()
            if duckdb_type.lower() in variants:
                return normalized.lower()
        
        # Return as-is if no mapping found
        return duckdb_type.lower()
    
    def _types_compatible(self, expected_type: str, actual_type: str) -> bool:
        """
        Check if two types are compatible.
        
        Args:
            expected_type: Expected type from schema
            actual_type: Actual type from DuckDB DESCRIBE
            
        Returns:
            True if types are compatible
        """
        expected_normalized = expected_type.lower().split('(')[0]
        actual_normalized = self._normalize_type(actual_type)
        
        # Check if they match in any mapping group
        for normalized, variants in self.TYPE_MAPPINGS.items():
            if expected_normalized in variants and actual_normalized == normalized.lower():
                return True
        
        # Direct match
        return expected_normalized == actual_normalized
    
    def validate_file(self, file_path: Union[str, Path], file_format: str = "csv") -> ValidationResult:
        """
        Validate a data file against the schema.
        
        Args:
            file_path: Path to data file
            file_format: Format of the file (csv, parquet, json)
            
        Returns:
            ValidationResult object
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            result = ValidationResult(
                status=ValidationStatus.FAIL,
                table_name=self.schema.table_name
            )
            result.add_issue(ValidationIssue(
                severity=ValidationStatus.FAIL,
                column=None,
                issue_type="file_not_found",
                message=f"Data file not found: {file_path}"
            ))
            return result
        
        # Load data into temporary table
        table_name = "temp_validation_table"
        try:
            if file_format.lower() == "csv":
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
            elif file_format.lower() == "parquet":
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
            elif file_format.lower() == "json":
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{file_path}')")
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
        except Exception as e:
            result = ValidationResult(
                status=ValidationStatus.FAIL,
                table_name=self.schema.table_name
            )
            result.add_issue(ValidationIssue(
                severity=ValidationStatus.FAIL,
                column=None,
                issue_type="load_error",
                message=f"Failed to load data file: {str(e)}"
            ))
            return result
        
        return self.validate_table(table_name)
    
    def validate_table(self, table_name: str) -> ValidationResult:
        """
        Validate a DuckDB table against the schema.
        
        Args:
            table_name: Name of the table in DuckDB
            
        Returns:
            ValidationResult object
        """
        result = ValidationResult(
            status=ValidationStatus.PASS,
            table_name=self.schema.table_name
        )
        
        # Use DESCRIBE to get actual schema
        try:
            describe_result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
        except Exception as e:
            result.add_issue(ValidationIssue(
                severity=ValidationStatus.FAIL,
                column=None,
                issue_type="describe_error",
                message=f"Failed to describe table: {str(e)}"
            ))
            return result
        
        # Parse DESCRIBE output
        actual_columns = {}
        for row in describe_result:
            col_name = row[0]
            col_type = row[1]
            col_nullable = row[2] == "YES" if len(row) > 2 else True
            
            actual_columns[col_name.lower()] = {
                "name": col_name,
                "type": col_type,
                "nullable": col_nullable
            }
        
        # Check each expected column
        for expected_col in self.schema.columns:
            col_name_lower = expected_col.name.lower()
            
            # Check if column exists
            if col_name_lower not in actual_columns:
                result.add_issue(ValidationIssue(
                    severity=ValidationStatus.FAIL,
                    column=expected_col.name,
                    issue_type="missing_column",
                    message=f"Column '{expected_col.name}' is missing from the data",
                    expected=expected_col.name,
                    actual="<not found>"
                ))
                continue
            
            actual_col = actual_columns[col_name_lower]
            
            # Check data type
            if not self._types_compatible(expected_col.data_type, actual_col["type"]):
                result.add_issue(ValidationIssue(
                    severity=ValidationStatus.FAIL,
                    column=expected_col.name,
                    issue_type="type_mismatch",
                    message=f"Column '{expected_col.name}' has incorrect type",
                    expected=expected_col.data_type,
                    actual=actual_col["type"]
                ))
            else:
                result.passed_checks += 1
        
        # Check for unexpected columns (warning only)
        expected_col_names = {col.name.lower() for col in self.schema.columns}
        for actual_col_name in actual_columns.keys():
            if actual_col_name not in expected_col_names:
                result.add_issue(ValidationIssue(
                    severity=ValidationStatus.WARNING,
                    column=actual_columns[actual_col_name]["name"],
                    issue_type="unexpected_column",
                    message=f"Unexpected column '{actual_columns[actual_col_name]['name']}' found in data",
                    expected="<not in schema>",
                    actual=actual_columns[actual_col_name]["name"]
                ))
        
        # Check for duplicate Primary Keys (Uniqueness Check)
        if self.primary_key:
            # Verify the PK column exists in the data
            if self.primary_key.lower() in actual_columns:
                try:
                    # SQL query to count duplicates: total rows - distinct values
                    dupes_query = f"SELECT COUNT(*) - COUNT(DISTINCT {self.primary_key}) FROM {table_name}"
                    duplicate_count = self.conn.execute(dupes_query).fetchone()[0]
                    
                    if duplicate_count > 0:
                        result.add_issue(ValidationIssue(
                            severity=ValidationStatus.FAIL,
                            column=self.primary_key,
                            issue_type="duplicate_primary_key",
                            message=f"❌ UNIQUENESS: Found {duplicate_count} duplicate IDs in Primary Key '{self.primary_key}'",
                            expected="unique values",
                            actual=f"{duplicate_count} duplicates"
                        ))
                    else:
                        result.passed_checks += 1
                except Exception as e:
                    # If the query fails, add a warning but don't fail validation
                    result.add_issue(ValidationIssue(
                        severity=ValidationStatus.WARNING,
                        column=self.primary_key,
                        issue_type="uniqueness_check_error",
                        message=f"Could not check uniqueness for '{self.primary_key}': {str(e)}",
                        expected="unique check",
                        actual="error"
                    ))
        
        return result
    
    def validate_query(self, query: str, alias: str = "validation_view") -> ValidationResult:
        """
        Validate the result of a SQL query against the schema.
        
        Args:
            query: SQL query to validate
            alias: Alias for the query result
            
        Returns:
            ValidationResult object
        """
        try:
            # Create a view from the query
            self.conn.execute(f"CREATE OR REPLACE VIEW {alias} AS {query}")
            return self.validate_table(alias)
        except Exception as e:
            result = ValidationResult(
                status=ValidationStatus.FAIL,
                table_name=self.schema.table_name
            )
            result.add_issue(ValidationIssue(
                severity=ValidationStatus.FAIL,
                column=None,
                issue_type="query_error",
                message=f"Failed to execute query: {str(e)}"
            ))
            return result


def validate_schema(
    schema_path: Union[str, Path],
    data_source: Union[str, Path],
    source_type: str = "csv",
    conn: Optional[duckdb.DuckDBPyConnection] = None
) -> ValidationResult:
    """
    Convenience function to validate a data source against a schema.
    
    Args:
        schema_path: Path to YAML schema file
        data_source: Path to data file or SQL query
        source_type: Type of data source ("csv", "parquet", "json", "table", "query")
        conn: Optional DuckDB connection
        
    Returns:
        ValidationResult object
    """
    validator = SchemaValidator(schema_path, conn)
    
    if source_type in ["csv", "parquet", "json"]:
        return validator.validate_file(data_source, source_type)
    elif source_type == "table":
        return validator.validate_table(str(data_source))
    elif source_type == "query":
        return validator.validate_query(str(data_source))
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
