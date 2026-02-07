"""
🕵️ Monitor Agent: The "Gatekeeper"

Core Mission:
- Role: Autonomous Data Reliability Engineer (Level 1)
- Goal: Prevent bad data from entering the warehouse by validating files 
        against a Data Contract (YAML) and Historical Norms
- Key Directive: "Triage intelligently. Block critical failures immediately, 
                  but allow non-critical warnings to pass with documentation."

This agent follows a Short-Circuit Decision Tree:
1. Sanity Check → STOP if stale/duplicate
2. Load & Sampling → CRITICAL STOP if load fails
3. Schema Validation → CRITICAL STOP if missing critical columns
4. Statistical Profiling → Adaptive outlier detection
5. Drift Detection → WARN if deviation > 30%
6. Quality Metrics → Comprehensive health assessment
7. Final Verdict → Structured JSON report with health indicator

Enhanced with Advanced features:
- Learned behavior (seasonal patterns) vs static rules
- Intelligent scanning prioritization by table importance
- Comprehensive quality metrics (freshness, completeness, validity, uniqueness)
- Unified health indicator across the platform
"""

import json
import yaml
import glob
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path

# Core tools
from tools.monitor import (
    FileMetadataTool,
    DataLoaderTool,
    SchemaValidatorTool,
    StatsAnalysisTool,
    DriftCheckTool
)

# Enhanced tools
from tools.monitor import (
    SeasonalDetector,
    TablePrioritizer,
    QualityMetricsTool,
    TablePrioritizer,
    QualityMetricsTool,
    HealthIndicator,
    ConsistencyCheckTool
)
from tools.monitor.contract_generator_tool import ContractGeneratorTool


class MonitorAgent:
    """
    The Gatekeeper - Autonomous Data Reliability Engineer.
    
    This agent is Safe, Smart, Aware, and Tolerant:
    - Safe: Short-circuiting prevents crashes
    - Smart: Adapts stats methods based on skewness + seasonal patterns
    - Aware: Checks historical drift AND learned seasonal behaviors
    - Tolerant: Distinguishes between "Stop the pipeline" and "Send an alert"
    
    Enhanced with Advanced capabilities:
    - Table prioritization by downstream impact
    - Seasonal pattern learning (day-of-week, monthly)
    - Comprehensive quality metrics
    - Unified health indicator
    """
    
    def __init__(self, contracts_dir: str = "contracts"):
        self.contracts_dir = contracts_dir
        
        # Core tools
        self.file_metadata_tool = FileMetadataTool(freshness_hours=24) # Default, overridden per contract
        self.data_loader_tool = DataLoaderTool(sampling_threshold_mb=500)
        self.schema_validator_tool = SchemaValidatorTool()
        self.stats_analysis_tool = StatsAnalysisTool()
        self.drift_check_tool = DriftCheckTool()
        
        # Enhanced tools
        self.seasonal_detector = SeasonalDetector()
        self.table_prioritizer = TablePrioritizer()
        self.quality_metrics_tool = QualityMetricsTool()
        self.health_indicator = HealthIndicator()
        self.consistency_check_tool = ConsistencyCheckTool()
        self.contract_generator_tool = ContractGeneratorTool()
        
        # Execution state
        self.execution_log = []
        self.start_time = None

    def _find_contract(self, table_name: str):
        """Find the contract YAML and schema object for a table."""
        # 1. Check direct filename match (optimization)
        direct_path = Path(self.contracts_dir) / f"{table_name}.yaml"
        if direct_path.exists():
            try:
                with open(direct_path, 'r') as f:
                    contract = yaml.safe_load(f)
                    schemas = contract.get('schema', [])
                    for s in schemas:
                        if s.get('name') == table_name or s.get('physicalName') == table_name:
                            return str(direct_path), s
                    # If filename matches but table name inside doesn't? 
                    # Still fallback to first schema or continue search.
                    if schemas: return str(direct_path), schemas[0]
            except Exception as e:
                print(f"⚠️ Error reading {direct_path}: {e}")

        # 2. Scan all files in registry
        search_pattern = str(Path(self.contracts_dir) / "*.yaml")
        for file_path in glob.glob(search_pattern):
            if file_path == str(direct_path): continue
            try:
                with open(file_path, 'r') as f:
                    contract = yaml.safe_load(f)
                    schemas = contract.get('schema', [])
                    for s in schemas:
                        if s.get('name') == table_name or s.get('physicalName') == table_name:
                            return file_path, s
            except:
                continue
        
        return None, None
    
    def run(self, file_path: str, table_name: str = "default") -> Dict[str, Any]:
        """
        Execute the Monitor Agent's decision tree.
        
        Returns a structured JSON report with the final verdict.
        """
        self.start_time = datetime.now()
        self.execution_log = []
        
        print(f"\n{'='*60}")
        print(f"🕵️  MONITOR AGENT: Starting Analysis for table '{table_name}'")
        print(f"{'='*60}")
        
        # 0. Find Contract in Registry
        print(f"\n[Step 0] Locating Contract in Registry ({self.contracts_dir})...")
        contract_path, schema_obj = self._find_contract(table_name)
        
        freshness_limit = 24 # Default
        if contract_path:
            print(f"  ✓ Found contract: {contract_path}")
            # Extract freshness from table schema
            if schema_obj:
                quality_rules = schema_obj.get('quality', [])
                for rule in quality_rules:
                    if rule.get('metric') == 'freshness':
                         threshold = rule.get('threshold', '24h')
                         if isinstance(threshold, str) and threshold.endswith('h'):
                            freshness_limit = int(threshold[:-1])
                         elif isinstance(threshold, int):
                            freshness_limit = threshold
        else:
            print(f"  ⚠️  No specific contract found for '{table_name}'. Using defaults.")

        # 1. Metadata check
        print(f"\n[Step 1/5] Checking File Metadata...")
        metadata_result = self.file_metadata_tool.run(file_path, override_freshness_hours=freshness_limit)
        self._log_step("FileMetadataTool", metadata_result)
        
        if metadata_result['decision'] == 'STOP':
            return self._generate_final_report(
                file_path=file_path,
                status="FAIL",
                critical_errors=[metadata_result.get('reason', 'File sanity check failed')],
                warnings=[],
                stats_summary={},
                quarantine_indices=[]
            )
        
        print(f"  ✓ File is {metadata_result['status']} ({metadata_result['size_mb']} MB)")
        
        # 2. Load Data
        print(f"\n[Step 2/5] Loading Data...")
        load_result = self.data_loader_tool.run(
            file_path=file_path,
            size_mb=metadata_result['size_mb']
        )
        
        # Store dataframe separately
        df = load_result.pop('dataframe', None)
        self._log_step("DataLoaderTool", load_result)
        
        if load_result['decision'] == 'CRITICAL_STOP' or df is None:
            return self._generate_final_report(
                file_path=file_path,
                status="FAIL",
                critical_errors=[load_result.get('reason', 'Failed to load data')],
                warnings=[],
                stats_summary={},
                quarantine_indices=[]
            )
            
        sampled = load_result.get('sampled', False)
        print(f"  ✓ Loaded {len(df)} rows")

        # 3. Schema Validation (Dynamic)
        print(f"\n[Step 3/5] Schema Validation (ODCS v3.1)...")
        suggested_updates = []
        active_contract_yaml = None
        
        if contract_path:
            # Capture raw contract content for Frontend Editor
            try:
                with open(contract_path, 'r') as f:
                    active_contract_yaml = f.read()
            except Exception as e:
                print(f"  ⚠️  Could not read contract file: {e}")

            schema_path_to_use = contract_path
            schema_result = self.schema_validator_tool.run(df, schema_path_to_use, table_name)
            self._log_step("SchemaValidatorTool", schema_result)
            suggested_updates = schema_result.get('suggested_updates', [])
            
            if schema_result['decision'] == 'CRITICAL_STOP':
                criticals = [v for v in schema_result.get('violations', []) if v.get('severity') == 'CRITICAL']
                reason = f"Critical Schema Violations: {len(criticals)}"
                return self._generate_final_report(
                    file_path=file_path,
                    status="FAIL",
                    critical_errors=[reason],
                    warnings=[],
                    stats_summary={},
                    quarantine_indices=[],
                    suggested_updates=suggested_updates,
                    active_contract=active_contract_yaml, # Pass content
                    table_name=table_name
                )
            print(f"  ✓ Schema validation: {schema_result['status']}")
            if schema_result.get('violations'):
                print(f"  ⚠️  {len(schema_result['violations'])} warnings (non-critical)")
        else:
            print("  ⚠️  No contract found. Initiating Contract Profiler...")
            
            # Run Stats Profiling Early (Phase 1)
            stats_result = self.stats_analysis_tool.run(df)
            
            # Run Contract Generator (Phase 2)
            draft_contract = self.contract_generator_tool.run(df, stats_result['profiles'], table_name)
            
            # Embed drafted contract
            inferred_contract_yaml = draft_contract['yaml_content']
            print(f"  📝 Draft contract generated for '{table_name}'")
            
            # Return special status for Frontend Proposer
            return self._generate_final_report(
                 file_path=file_path,
                 status="CONTRACT_MISSING",
                 critical_errors=[],
                 warnings=["No contract found. Draft generated."],
                 stats_summary=stats_result['profiles'],
                 quarantine_indices=[],
                 inferred_contract=inferred_contract_yaml,
                 table_name=table_name
            )
        
        # STEP 3.5: Multi-Table Consistency Check (Referential Integrity)
        print("\nStep 3.5/5: Consistency Check (Referential Integrity)...")
        
        # Load contract object for FK validation
        contract_dict = None
        if active_contract_yaml:
            try:
                contract_dict = yaml.safe_load(active_contract_yaml)
            except:
                pass

        consistency_result = self.consistency_check_tool.run(df, table_name, contract=contract_dict)
        self._log_step("ConsistencyCheckTool", consistency_result)
        
        if consistency_result.get('status') == 'FAIL':
            msg = f"Found {consistency_result['orphan_count']} orphan records ({consistency_result['orphan_pct']:.1f}%) in {consistency_result['relationship']}. Sample IDs: {consistency_result['sample_orphans']}"
            return self._generate_final_report(
                file_path=file_path,
                status="FAIL",
                critical_errors=[msg],
                warnings=[],
                stats_summary={},
                quarantine_indices=[]
            )
        elif consistency_result.get('status') == 'SKIPPED':
            print("  ℹ️  No relationships defined (Skipped)")
        else:
            print(f"  ✓ Consistency passed: {consistency_result['relationship']}")

        # STEP 4: Statistical Profiling
        print("\nStep 4/5: Statistical Profiling...")
        stats_result = self.stats_analysis_tool.run(df)
        self._log_step("StatsAnalysisTool", stats_result)
        
        print(f"  ✓ Profiled {len(stats_result['profiles'])} numeric columns")
        for col, profile in stats_result['profiles'].items():
            if 'outlier_method' in profile:
                print(f"    - {col}: {profile['outlier_method']} method, {profile['outlier_count']} outliers")
        
        # STEP 5: Drift Detection (The Memory)
        print("\nStep 5/5: Drift Detection (Historical Comparison)...")
        drift_result = self.drift_check_tool.run(
            current_stats={
                'row_count': len(df),
                'profiles': stats_result['profiles']
            },
            table_name=table_name
        )
        self._log_step("DriftCheckTool", drift_result)
        
        print(f"  ✓ Drift check: {drift_result['status']}")
        if drift_result['drift_warnings']:
            print(f"  ⚠️  {len(drift_result['drift_warnings'])} drift warnings detected")
        
        # Save current run to history
        self.drift_check_tool.save_current_run(
            table_name=table_name,
            file_hash=metadata_result['hash'],
            row_count=len(df),
            column_stats=stats_result['profiles']
        )
        
        # STEP 5.5: Seasonal Anomaly Detection
        print("\nStep 5.5/5: Seasonal Anomaly Detection...")
        seasonal_analysis = {}
        # Check row count seasonality
        row_count_anomaly = self.seasonal_detector.check_anomaly(
            table_name, "row_count", len(df)
        )
        seasonal_analysis["row_count"] = row_count_anomaly
        
        if row_count_anomaly['is_anomaly']:
            print(f"  ⚠️  Seasonal Anomaly: {row_count_anomaly['context']}")
            
        # Online Learning: Update patterns if data is fundamentally valid
        # We only learn if schema validation passed to avoid poisoning the model with garbage
        if schema_result['decision'] != 'CRITICAL_STOP': 
             self.seasonal_detector.learn_patterns(table_name, "row_count", [{
                 "timestamp": datetime.now().isoformat(),
                 "value": len(df)
             }])
        
        # STEP 6: Final Verdict
        print(f"\n{'='*60}")
        print("📊 FINAL VERDICT")
        print(f"{'='*60}")
        
        # Collect all warnings
        warnings = []
        
        # Seasonal warnings
        if seasonal_analysis.get('row_count', {}).get('is_anomaly'):
             warnings.append(f"Seasonal Anomaly: {seasonal_analysis['row_count']['context']}")
        
        # Schema warnings
        for violation in schema_result.get('violations', []):
            if violation['severity'] == 'WARNING':
                warnings.append(
                    f"Column '{violation['column']}': {violation['issue']} "
                    f"(expected: {violation['expected']}, actual: {violation['actual']})"
                )
        
        # Drift warnings
        for drift_warn in drift_result.get('drift_warnings', []):
            warnings.append(
                f"Drift: {drift_warn['metric']} = {drift_warn['current']} "
                f"(baseline: {drift_warn['baseline']}, deviation: {drift_warn['deviation_pct']}%)"
            )
        
        # Collect quarantine indices (outliers)
        quarantine_indices = []
        for col, profile in stats_result['profiles'].items():
            if 'outlier_indices' in profile:
                quarantine_indices.extend(profile['outlier_indices'])
        quarantine_indices = list(set(quarantine_indices))  # Remove duplicates
        
        # Determine final status
        if warnings:
            status = "PASS_WITH_WARNINGS"
        else:
            status = "PASS"
        
        return self._generate_final_report(
            file_path=file_path,
            status=status,
            critical_errors=[],
            warnings=warnings,
            stats_summary=stats_result['profiles'],
            quarantine_indices=quarantine_indices,
            df=df,
            table_name=table_name,
            suggested_updates=suggested_updates,
            active_contract=active_contract_yaml,
            seasonal_analysis=seasonal_analysis,
            consistency_result=consistency_result
        )
    
    def _log_step(self, tool_name: str, result: Dict[str, Any]):
        """Log a tool execution step."""
        self.execution_log.append({
            "tool": tool_name,
            "timestamp": datetime.now().isoformat(),
            "result": result
        })
    
    def _generate_final_report(self, file_path: str, status: str, 
                               critical_errors: List[str], warnings: List[str],
                               stats_summary: Dict[str, Any], 
                               quarantine_indices: List[int],
                               df=None, table_name: str = "default",
                               suggested_updates: List[Dict] = None,
                               inferred_contract: str = None,
                               active_contract: str = None,
                               seasonal_analysis: Dict = None,
                               consistency_result: Dict = None) -> Dict[str, Any]:
        """
        Generate the final structured JSON report.
        
        This is the "Handover" format for the Diagnoser Agent.
        Enhanced with Advanced metrics.
        """
        execution_time = (datetime.now() - self.start_time).total_seconds()
        
        # Calculate quality metrics if DataFrame is available
        quality_metrics = None
        health_result = None
        if df is not None:
            try:
                quality_metrics = self.quality_metrics_tool.run(df)
                
                # Calculate unified health indicator
                monitor_report_preview = {
                    "status": status,
                    "warnings": warnings,
                    "critical_errors": critical_errors
                }
                health_result = self.health_indicator.calculate_health(
                    quality_metrics, monitor_report_preview
                )
            except Exception as e:
                print(f"  ⚠️  Quality metrics calculation failed: {e}")
        
        # Fallback for Critical Failures (Ensure Health Score)
        if health_result is None and status == "FAIL":
            health_result = {
                "status": "CRITICAL",
                "score": 0.0,
                "badge": "🛑",
                "safe_to_use": False,
                "issue_count": len(critical_errors),
                "issues": critical_errors,
                "summary": "Pipeline halted due to critical errors.",
                "recommendations": ["Fix critical schema violations immediately.", "Review the contract or data source."],
                "risk_assessment": "High",
                "timestamp": datetime.now().isoformat()
            }
        
        # Get table priority information
        table_priority = self.table_prioritizer.get_priority(table_name)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "file": file_path,
            "table_name": table_name,
            "status": status,
            "execution_time": f"{execution_time:.2f}s",
            "critical_errors": critical_errors,
            "warnings": warnings,
            "stats_summary": stats_summary,
            "quarantine_indices": quarantine_indices[:100],  # Limit to first 100
            "execution_log": self.execution_log,
            
            # Advanced additions
            "quality_metrics": quality_metrics,
            "health_indicator": health_result,
            "table_priority": table_priority,
            "seasonal_analysis": seasonal_analysis,
            "consistency_result": consistency_result,
            
            # Evolution Suggestion
            "schema_evolution": {
                "suggested_updates": suggested_updates or []
            },
            
            # Draft Proposal
            "inferred_contract": inferred_contract,
            
            # Active Contract Preview
            "active_contract": active_contract
        }
        
        # Print summary
        if status == "PASS":
            print("✅ STATUS: PASS")
            print("All checks passed successfully!")
        elif status == "PASS_WITH_WARNINGS":
            print("⚠️  STATUS: PASS WITH WARNINGS")
            print(f"Found {len(warnings)} warnings (non-critical):")
            for i, warning in enumerate(warnings[:5], 1):
                print(f"  {i}. {warning}")
            if len(warnings) > 5:
                print(f"  ... and {len(warnings) - 5} more")
        elif status == "CONTRACT_MISSING":
             print("📝 STATUS: CONTRACT DRAFTED")
             print("No active contract found. A draft has been generated for review.")
        else:
            print("❌ STATUS: FAIL")
            print(f"Found {len(critical_errors)} critical errors:")
            for i, error in enumerate(critical_errors, 1):
                print(f"  {i}. {error}")
        
        if quarantine_indices:
            print(f"\n🔒 Quarantined {len(quarantine_indices)} rows (outliers)")
        
        # Print health indicator
        if health_result:
            print(f"\n{health_result['badge']} HEALTH INDICATOR: {health_result['status']} ({health_result['score']}/100)")
            print(f"   Safe to use: {'Yes' if health_result['safe_to_use'] else 'No'}")
        
        # Print quality metrics summary
        if quality_metrics:
            print(f"\n📊 QUALITY METRICS:")
            print(f"   Freshness:    {quality_metrics['metrics']['freshness']['status']}")
            print(f"   Completeness: {quality_metrics['metrics']['completeness']['score']:.1f}%")
            print(f"   Validity:     {quality_metrics['metrics']['validity']['score']:.1f}%")
            print(f"   Uniqueness:   {quality_metrics['metrics']['uniqueness']['score']:.1f}%")
        
        # Print table priority
        if table_priority.get('priority_tier') != 'UNKNOWN':
            print(f"\n🎯 TABLE PRIORITY: {table_priority['priority_tier']} (score: {table_priority['priority_score']})")
        
        print(f"\n⏱️  Execution time: {execution_time:.2f}s")
        print(f"{'='*60}\n")
        
        return report
    
    def save_report(self, report: Dict[str, Any], output_dir: str = "reports"):
        """Save the report to a JSON file."""
        import numpy as np
        import math
        
        # Convert numpy types and NaN to native Python types for JSON serialization
        def convert_to_native(obj):
            # Handle NaN values (from numpy or math)
            if isinstance(obj, float) and (math.isnan(obj) or np.isnan(obj)):
                return None
            # Handle numpy integers
            elif isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            # Handle numpy floats
            elif isinstance(obj, (np.floating, np.float64, np.float32)):
                # Check for NaN again after type check
                if np.isnan(obj):
                    return None
                return float(obj)
            # Handle numpy arrays
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            # Handle dictionaries recursively
            elif isinstance(obj, dict):
                return {k: convert_to_native(v) for k, v in obj.items()}
            # Handle lists recursively
            elif isinstance(obj, list):
                return [convert_to_native(item) for item in obj]
            return obj
        
        report_clean = convert_to_native(report)
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/monitor_report_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(report_clean, f, indent=2)
        
        print(f"📄 Report saved to: {filename}")
        return filename

