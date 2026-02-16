"""
Dimension Scorer - 6-Dimensional Data Quality Framework

Maps tool outputs to the 6 standard data quality dimensions:
1. Validity - Does it follow rules/format?
2. Completeness - Is any data missing?
3. Uniqueness - Are there duplicates?
4. Accuracy - Does it match reality?
5. Timeliness - Is it fresh?
6. Consistency - Is it same across systems/time?

Returns weighted aggregate score + per-dimension breakdown for visualization.
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import json


@dataclass
class DimensionScore:
    """Score for a single quality dimension."""
    name: str
    score: float  # 0-100
    weight: float  # 0-1 (must sum to 1.0 across all dimensions)
    status: str  # PASS, WARN, FAIL
    check_count: Dict[str, int] = field(default_factory=lambda: {"total": 0, "passed": 0, "failed": 0})
    violations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "score": round(self.score, 2),
            "weight": self.weight,
            "status": self.status,
            "check_count": self.check_count,
            "violations": self.violations[:5]  # Limit to top 5 for API
        }


@dataclass
class QualityDimensionReport:
    """Complete 6-dimensional quality report."""
    dataset_name: str
    timestamp: str
    overall_score: float
    dimensions: List[DimensionScore]
    remediation_status: str = "NO_ACTION_NEEDED"  # NO_ACTION_NEEDED, OPEN_INCIDENT, REMEDIATED

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "timestamp": self.timestamp,
            "overall_score": round(self.overall_score, 2),
            "dimensions": [d.to_dict() for d in self.dimensions],
            "remediation_status": self.remediation_status
        }


class DimensionScorer:
    """
    Calculates 6-dimensional quality scores from tool outputs.

    Aggregates results from SchemaValidator, DataProfiler, and AnomalyDetector
    into the standard 6 dimensions for visualization (Radar chart, Donut rings).
    """

    # Default weights (can be overridden per dataset in YAML)
    DEFAULT_WEIGHTS = {
        "Validity": 0.25,
        "Completeness": 0.25,
        "Uniqueness": 0.15,
        "Accuracy": 0.15,
        "Timeliness": 0.10,
        "Consistency": 0.10
    }

    def __init__(self, weights: Optional[Dict[str, float]] = None):
        """
        Initialize with custom weights or use defaults.

        Args:
            weights: Dict mapping dimension name → weight (0-1, must sum to 1.0)
        """
        self.weights = weights or self.DEFAULT_WEIGHTS
        self._validate_weights()

    def _validate_weights(self):
        """Ensure weights sum to 1.0, auto-normalize if needed."""
        if set(self.weights.keys()) != set(self.DEFAULT_WEIGHTS.keys()):
            raise ValueError(f"Weights must include all 6 dimensions: {list(self.DEFAULT_WEIGHTS.keys())}")

        total = sum(self.weights.values())

        if total == 0:
            raise ValueError("Weights must not all be zero")

        # Auto-normalize if sum != 1.0 (allows user-friendly integers like 30, 25, 20)
        if abs(total - 1.0) > 0.01:
            print(f"⚙️  Normalizing weights (sum={total:.2f} → 1.0)")
            self.weights = {k: v/total for k, v in self.weights.items()}

    @staticmethod
    def load_weights_from_contract(contract_path: Union[str, Path]) -> Optional[Dict[str, float]]:
        """
        Load quality_weights from YAML contract.

        Args:
            contract_path: Path to YAML contract file

        Returns:
            Dict mapping dimension name → weight, or None if not specified

        Example YAML:
            quality:
              quality_weights:
                completeness: 30
                validity: 25
                accuracy: 20
        """
        import yaml

        path = Path(contract_path)
        if not path.exists():
            return None

        try:
            with open(path, 'r') as f:
                contract = yaml.safe_load(f) or {}

            weights_raw = contract.get("quality", {}).get("quality_weights")
            if not weights_raw:
                return None

            # Map YAML keys (lowercase) to DimensionScorer keys (titlecase)
            dimension_map = {
                "validity": "Validity",
                "completeness": "Completeness",
                "uniqueness": "Uniqueness",
                "accuracy": "Accuracy",
                "timeliness": "Timeliness",
                "consistency": "Consistency"
            }

            weights = {}
            for yaml_key, dim_name in dimension_map.items():
                if yaml_key in weights_raw:
                    weights[dim_name] = float(weights_raw[yaml_key])

            if len(weights) != 6:
                print(f"⚠️  Incomplete weights in {path.name} (need all 6 dimensions), using defaults")
                return None

            return weights

        except Exception as e:
            print(f"⚠️  Failed to load weights from {path.name}: {e}")
            return None

    def calculate_dimension_scores(
        self,
        dataset_name: str,
        schema_result: Dict[str, Any],
        profile_report: Dict[str, Any],
        anomaly_report: Dict[str, Any]
    ) -> QualityDimensionReport:
        """
        Calculate 6-dimensional quality scores from tool outputs.

        Args:
            dataset_name: Name of the dataset
            schema_result: Output from SchemaValidator.to_dict()
            profile_report: Output from DataProfiler.to_dict()
            anomaly_report: Output from AnomalyDetector.evaluate_run()

        Returns:
            QualityDimensionReport with weighted overall score and per-dimension breakdown
        """
        dimensions = []

        # 1. Validity (Schema + Format checks)
        validity = self._calculate_validity(schema_result, profile_report)
        dimensions.append(validity)

        # 2. Completeness (Null checks + Row count)
        completeness = self._calculate_completeness(profile_report, anomaly_report)
        dimensions.append(completeness)

        # 3. Uniqueness (Duplicate detection)
        uniqueness = self._calculate_uniqueness(profile_report)
        dimensions.append(uniqueness)

        # 4. Accuracy (Range checks + Custom SQL)
        accuracy = self._calculate_accuracy(profile_report)
        dimensions.append(accuracy)

        # 5. Timeliness (Freshness checks)
        timeliness = self._calculate_timeliness(anomaly_report)
        dimensions.append(timeliness)

        # 6. Consistency (Drift detection)
        consistency = self._calculate_consistency(anomaly_report)
        dimensions.append(consistency)

        # Calculate weighted overall score
        overall_score = sum(d.score * d.weight for d in dimensions)

        # Determine remediation status
        remediation_status = self._determine_remediation_status(dimensions, overall_score)

        return QualityDimensionReport(
            dataset_name=dataset_name,
            timestamp=datetime.utcnow().isoformat() + "Z",
            overall_score=overall_score,
            dimensions=dimensions,
            remediation_status=remediation_status
        )

    def _calculate_validity(self, schema_result: Dict, profile_report: Dict) -> DimensionScore:
        """
        Validity = Schema correctness + Pattern/Allowed values compliance

        Sources:
        - SchemaValidator: Column presence, type correctness
        - DataProfiler: Pattern violations, allowed_values violations
        """
        total_checks = 0
        passed_checks = 0
        violations = []

        # Schema validation checks
        if schema_result.get("status") == "pass":
            passed_checks += schema_result.get("passed_checks", 0)
            total_checks += schema_result.get("passed_checks", 0)
        else:
            total_checks += len(schema_result.get("issues", []))
            for issue in schema_result.get("issues", []):
                if issue.get("issue_type") in ["missing_column", "type_mismatch"]:
                    violations.append(f"{issue['column']}: {issue['message']}")

        # Pattern and allowed_values violations from profile
        for col_name, col_profile in profile_report.get("column_profiles", {}).items():
            col_violations = col_profile.get("violations", [])

            # Check for pattern violations
            pattern_violation = next((v for v in col_violations if "PATTERN" in v), None)
            if pattern_violation:
                total_checks += 1
                violations.append(f"{col_name}: {pattern_violation}")

            # Check for allowed_values violations
            allowed_violation = next((v for v in col_violations if "ALLOWED VALUES" in v), None)
            if allowed_violation:
                total_checks += 1
                violations.append(f"{col_name}: {allowed_violation}")

        score = (passed_checks / total_checks * 100) if total_checks > 0 else 100.0
        status = "PASS" if score >= 95 else ("WARN" if score >= 80 else "FAIL")

        return DimensionScore(
            name="Validity",
            score=score,
            weight=self.weights["Validity"],
            status=status,
            check_count={"total": total_checks, "passed": passed_checks, "failed": total_checks - passed_checks},
            violations=violations
        )

    def _calculate_completeness(self, profile_report: Dict, anomaly_report: Dict) -> DimensionScore:
        """
        Completeness = Null rate compliance + Row count expectations

        Sources:
        - DataProfiler: Nullable violations, row count min/max
        - AnomalyDetector: Row count anomalies
        """
        total_checks = 0
        passed_checks = 0
        violations = []

        # Null violations
        for col_name, col_profile in profile_report.get("column_profiles", {}).items():
            for violation in col_profile.get("violations", []):
                if "NOT NULL" in violation:
                    total_checks += 1
                    violations.append(f"{col_name}: {violation}")
                else:
                    total_checks += 1
                    passed_checks += 1

        # Row count violations
        for constraint in profile_report.get("constraint_violations", []):
            if "ROW_COUNT" in constraint.get("type", ""):
                total_checks += 1
                violations.append(constraint["message"])

        # Row count anomalies
        if anomaly_report.get("status") == "ANOMALY_DETECTED":
            for anomaly in anomaly_report.get("anomalies", []):
                if anomaly.get("metric_name") == "row_count":
                    total_checks += 1
                    violations.append(f"Row count anomaly: {anomaly.get('reason', '')}")
        else:
            total_checks += 1
            passed_checks += 1

        score = (passed_checks / total_checks * 100) if total_checks > 0 else 100.0
        status = "PASS" if score >= 95 else ("WARN" if score >= 80 else "FAIL")

        return DimensionScore(
            name="Completeness",
            score=score,
            weight=self.weights["Completeness"],
            status=status,
            check_count={"total": total_checks, "passed": passed_checks, "failed": total_checks - passed_checks},
            violations=violations
        )

    def _calculate_uniqueness(self, profile_report: Dict) -> DimensionScore:
        """
        Uniqueness = Primary key violations + Duplicate detection

        Sources:
        - DataProfiler: isPrimaryKey violations
        """
        total_checks = 0
        passed_checks = 0
        violations = []

        for col_name, col_profile in profile_report.get("column_profiles", {}).items():
            for violation in col_profile.get("violations", []):
                if "PRIMARY KEY" in violation or "duplicate" in violation.lower():
                    total_checks += 1
                    violations.append(f"{col_name}: {violation}")
                else:
                    total_checks += 1
                    passed_checks += 1

        score = (passed_checks / total_checks * 100) if total_checks > 0 else 100.0
        status = "PASS" if score >= 95 else ("WARN" if score >= 80 else "FAIL")

        return DimensionScore(
            name="Uniqueness",
            score=score,
            weight=self.weights["Uniqueness"],
            status=status,
            check_count={"total": total_checks, "passed": passed_checks, "failed": total_checks - passed_checks},
            violations=violations
        )

    def _calculate_accuracy(self, profile_report: Dict) -> DimensionScore:
        """
        Accuracy = Range violations + Custom SQL check failures

        Sources:
        - DataProfiler: min_value/max_value violations, custom_checks
        """
        total_checks = 0
        passed_checks = 0
        violations = []

        # Range violations
        for col_name, col_profile in profile_report.get("column_profiles", {}).items():
            for violation in col_profile.get("violations", []):
                if "RANGE" in violation:
                    total_checks += 1
                    violations.append(f"{col_name}: {violation}")
                else:
                    total_checks += 1
                    passed_checks += 1

        # Custom SQL checks
        for check_result in profile_report.get("custom_check_results", []):
            total_checks += 1
            if check_result.get("passed", True):
                passed_checks += 1
            else:
                violations.append(f"{check_result['name']}: {check_result.get('error', 'Failed')}")

        score = (passed_checks / total_checks * 100) if total_checks > 0 else 100.0
        status = "PASS" if score >= 95 else ("WARN" if score >= 80 else "FAIL")

        return DimensionScore(
            name="Accuracy",
            score=score,
            weight=self.weights["Accuracy"],
            status=status,
            check_count={"total": total_checks, "passed": passed_checks, "failed": total_checks - passed_checks},
            violations=violations
        )

    def _calculate_timeliness(self, anomaly_report: Dict) -> DimensionScore:
        """
        Timeliness = Freshness checks + SLA compliance

        Sources:
        - AnomalyDetector: (Currently basic, will be enhanced)

        TODO: Add SLA-based freshness monitoring
        """
        total_checks = 0
        passed_checks = 0
        violations = []

        metrics = anomaly_report.get("metrics", {})
        freshness = metrics.get("freshness_age_minutes")
        if isinstance(freshness, dict):
            total_checks += 1
            freshness_value = freshness.get("value")
            tags = freshness.get("tags", {}) if isinstance(freshness.get("tags"), dict) else {}
            slo_target = tags.get("slo_target_minutes")

            if slo_target is not None and freshness_value is not None:
                if float(freshness_value) <= float(slo_target):
                    passed_checks += 1
                else:
                    violations.append(
                        f"Freshness breach: age {freshness_value:.2f} min > target {float(slo_target):.2f} min"
                    )
            else:
                # Fallback heuristic: if freshness metric exists but no target, treat <= 360 min as pass.
                if freshness_value is not None and float(freshness_value) <= 360.0:
                    passed_checks += 1
                else:
                    violations.append(
                        f"Freshness warning: age {float(freshness_value or 0):.2f} min without explicit SLA"
                    )

        if total_checks == 0:
            # Backward-compatible fallback for runs without freshness metric.
            total_checks = 1
            passed_checks = 1

        score = (passed_checks / total_checks * 100) if total_checks > 0 else 100.0
        status = "PASS" if score >= 95 else ("WARN" if score >= 80 else "FAIL")

        return DimensionScore(
            name="Timeliness",
            score=score,
            weight=self.weights["Timeliness"],
            status=status,
            check_count={"total": total_checks, "passed": passed_checks, "failed": total_checks - passed_checks},
            violations=violations
        )

    def _calculate_consistency(self, anomaly_report: Dict) -> DimensionScore:
        """
        Consistency = Distribution drift + Historical baseline comparison

        Sources:
        - AnomalyDetector: Z-score anomalies (drift detection)
        """
        total_checks = 0
        passed_checks = 0
        violations = []

        if anomaly_report.get("status") == "ANOMALY_DETECTED":
            for anomaly in anomaly_report.get("anomalies", []):
                total_checks += 1
                z_score = anomaly.get("z_score", 0)
                metric_name = anomaly.get("metric_name") or anomaly.get("metric") or "unknown_metric"
                violations.append(
                    f"{metric_name}: Z-score {z_score:.2f} - {anomaly.get('reason', '')}"
                )
        else:
            # Check all metrics for consistency
            for metric_name, metric_data in anomaly_report.get("metrics", {}).items():
                total_checks += 1
                if not metric_data.get("is_anomaly", False):
                    passed_checks += 1

        score = (passed_checks / total_checks * 100) if total_checks > 0 else 100.0
        status = "PASS" if score >= 95 else ("WARN" if score >= 80 else "FAIL")

        return DimensionScore(
            name="Consistency",
            score=score,
            weight=self.weights["Consistency"],
            status=status,
            check_count={"total": total_checks, "passed": passed_checks, "failed": total_checks - passed_checks},
            violations=violations
        )

    def _determine_remediation_status(self, dimensions: List[DimensionScore], overall_score: float) -> str:
        """Determine if remediation is needed based on scores."""
        if overall_score < 50:
            return "OPEN_INCIDENT"
        elif any(d.status == "FAIL" for d in dimensions):
            return "OPEN_INCIDENT"
        elif overall_score < 80:
            return "MONITORING"
        else:
            return "NO_ACTION_NEEDED"


if __name__ == "__main__":
    # Example usage
    scorer = DimensionScorer()

    # Mock data for testing
    schema_result = {"status": "pass", "passed_checks": 10, "issues": []}
    profile_report = {
        "overall_quality_score": 85.0,
        "column_profiles": {
            "Age": {"violations": ["NOT NULL violation: 159 nulls"]},
            "Email": {"violations": []}
        },
        "constraint_violations": [],
        "custom_check_results": []
    }
    anomaly_report = {"status": "PASS", "anomalies": [], "metrics": {}}

    report = scorer.calculate_dimension_scores(
        "test_dataset", schema_result, profile_report, anomaly_report
    )

    print(json.dumps(report.to_dict(), indent=2))
