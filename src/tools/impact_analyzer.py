"""
Impact Analyzer Tool - Downstream Dependency & Criticality Analysis

This tool prevents "Alert Fatigue" by checking if a data failure actually matters.
It reads a lineage graph (YAML) to determine what downstream systems (Dashboards, ML Models)
depend on the failing dataset.

Key Features:
1. Loads lineage graph from config/lineage.yaml
2. Identifies all downstream consumers (Consumer + Criticality)
3. Provides context for the Agent to escalate or suppress alerts.

Example Output:
{
    "dataset": "transactions",
    "criticality": "HIGH",
    "impacted_systems": [
        {"name": "Executive_Dashboard", "type": "dashboard", "owner": "CEO", "criticality": "HIGH"},
        {"name": "Fraud_Detection_Model", "type": "ml_model", "owner": "Data Science", "criticality": "MEDIUM"}
    ]
}
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional

class ImpactAnalyzer:
    """
    The Business Context Engine - Determines the "Blast Radius" of a data failure.
    """
    
    def __init__(self, lineage_path: str = "config/lineage.yaml"):
        """
        Initialize the Impact Analyzer.
        
        Args:
            lineage_path: Path to the lineage configuration file.
        """
        self.lineage_path = Path(lineage_path)
        self.lineage_graph = self._load_lineage()

    @staticmethod
    def _extract_ref_name(value: Any) -> Optional[str]:
        if isinstance(value, str):
            candidate = value.strip()
            return candidate or None
        if isinstance(value, dict):
            candidate = str(value.get("name") or "").strip()
            return candidate or None
        return None

    @classmethod
    def _normalize_consumers(cls, value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        normalized: List[Dict[str, Any]] = []
        for item in value:
            if isinstance(item, dict):
                name = str(item.get("name") or "").strip()
                normalized.append(
                    {
                        "name": name or "unknown_consumer",
                        "type": item.get("type", "unknown"),
                        "owner": item.get("owner", "Unknown"),
                        "criticality": str(item.get("criticality", "LOW")).upper(),
                    }
                )
                continue

            if isinstance(item, str):
                consumer_name = item.strip()
                if consumer_name:
                    normalized.append(
                        {
                            "name": consumer_name,
                            "type": "unknown",
                            "owner": "Unknown",
                            "criticality": "LOW",
                        }
                    )

        return normalized

    @classmethod
    def _normalize_upstream_refs(cls, value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        normalized: List[Dict[str, Any]] = []
        for item in value:
            if isinstance(item, dict):
                name = str(item.get("name") or "").strip()
                if not name:
                    continue
                normalized.append(
                    {
                        "name": name,
                        "type": item.get("type", "dataset"),
                        "owner": item.get("owner", "Unknown"),
                        "criticality": str(item.get("criticality", "UNKNOWN")).upper(),
                        "managed": bool(item.get("managed", False)),
                    }
                )
                continue
            if isinstance(item, str):
                value_name = item.strip()
                if value_name:
                    normalized.append(
                        {
                            "name": value_name,
                            "type": "dataset",
                            "owner": "Unknown",
                            "criticality": "UNKNOWN",
                            "managed": False,
                        }
                    )
        return normalized

    def _datasets_map(self) -> Dict[str, Dict[str, Any]]:
        datasets = self.lineage_graph.get("datasets", {})
        if not isinstance(datasets, dict):
            return {}
        return {str(k): (v if isinstance(v, dict) else {}) for k, v in datasets.items()}

    def _collect_managed_downstream(
        self,
        dataset_name: str,
        *,
        datasets: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Infer downstream dataset dependencies by reversing upstream links.
        """
        datasets = datasets or self._datasets_map()
        downstream: List[Dict[str, Any]] = []
        for candidate_name, info in datasets.items():
            upstream = self._normalize_upstream_refs(info.get("upstream", []))
            names = {str(item.get("name") or "") for item in upstream}
            if dataset_name not in names:
                continue
            downstream.append(
                {
                    "name": candidate_name,
                    "type": "dataset",
                    "owner": info.get("owner", "Unknown"),
                    "criticality": str(info.get("criticality", "LOW")).upper(),
                    "inferred": True,
                }
            )
        return downstream
        
    def _load_lineage(self) -> Dict[str, Any]:
        """Load and parse the lineage YAML file."""
        if not self.lineage_path.exists():
            print(f"⚠️ WARNING: Lineage file not found at {self.lineage_path}. Assuming no downstream dependencies.")
            return {}
            
        try:
            with open(self.lineage_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"❌ ERROR: Failed to parse lineage file: {e}")
            return {}

    def refresh(self) -> Dict[str, Any]:
        """Reload lineage config from disk."""
        self.lineage_graph = self._load_lineage()
        return self.lineage_graph

    def summarize_lineage(self, graph: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Build factual lineage metadata and validation issues.
        """
        current = graph if isinstance(graph, dict) else self.lineage_graph
        datasets = current.get("datasets", {}) if isinstance(current, dict) else {}
        if not isinstance(datasets, dict):
            datasets = {}

        dataset_names = set(datasets.keys())
        dataset_count = len(dataset_names)
        owner_count = 0
        consumer_count = 0
        upstream_edge_count = 0
        downstream_edge_count = 0
        managed_upstream_edge_count = 0
        managed_consumer_edge_count = 0
        isolated_dataset_count = 0
        external_upstream_refs: List[Dict[str, str]] = []
        invalid_upstream_refs: List[Dict[str, str]] = []

        for dataset_name, info in datasets.items():
            if not isinstance(info, dict):
                continue
            if info.get("owner"):
                owner_count += 1

            consumers = self._normalize_consumers(info.get("consumers", []))
            consumer_count += len(consumers)
            for consumer in consumers:
                consumer_name = self._extract_ref_name(consumer)
                if consumer_name and consumer_name in dataset_names:
                    managed_consumer_edge_count += 1
                    downstream_edge_count += 1

            upstream = info.get("upstream", [])
            if not isinstance(upstream, list):
                upstream = []
            for ref in upstream:
                upstream_edge_count += 1
                ref_name = self._extract_ref_name(ref)
                if not ref_name:
                    invalid_upstream_refs.append({"dataset": dataset_name, "upstream": str(ref)})
                    continue
                if ref_name in dataset_names:
                    managed_upstream_edge_count += 1
                    downstream_edge_count += 1
                else:
                    external_upstream_refs.append({"dataset": dataset_name, "upstream": str(ref_name)})

            if not upstream and not consumers:
                isolated_dataset_count += 1

        owner_coverage_pct = round((owner_count / dataset_count) * 100, 2) if dataset_count else 0.0

        return {
            "summary": {
                "dataset_count": dataset_count,
                "upstream_edge_count": upstream_edge_count,
                "downstream_edge_count": downstream_edge_count,
                "managed_upstream_edge_count": managed_upstream_edge_count,
                "managed_consumer_edge_count": managed_consumer_edge_count,
                "external_upstream_count": len(external_upstream_refs),
                "consumer_count": consumer_count,
                "isolated_dataset_count": isolated_dataset_count,
                "owner_coverage_pct": owner_coverage_pct,
            },
            "issues": {
                "external_upstream_refs": external_upstream_refs,
                "invalid_upstream_refs": invalid_upstream_refs,
            },
            "graph": self.build_graph_view(current),
        }

    def get_downstream_impact(self, dataset_name: str) -> Dict[str, Any]:
        """
        Identify all downstream consumers for a given dataset.
        
        Args:
            dataset_name: Name of the dataset (e.g., 'transactions')
            
        Returns:
            Dictionary containing overall criticality and list of impacted systems.
        """
        impact_report = {
            "dataset": dataset_name,
            "overall_criticality": "LOW",
            "impacted_consumers": []
        }
        
        # 1. Find the dataset in the lineage graph
        # Structure assumption: 
        # datasets:
        #   transactions:
        #     consumers:
        #       - name: safe_executive_dashboard
        #         type: dashboard
        #         criticality: HIGH
        
        dataset_info = self.lineage_graph.get("datasets", {}).get(dataset_name)
        
        if not dataset_info:
            return impact_report
            
        consumers = self._normalize_consumers(dataset_info.get("consumers", []))
        dataset_map = self._datasets_map()
        inferred_downstream = self._collect_managed_downstream(dataset_name, datasets=dataset_map)

        merged: List[Dict[str, Any]] = []
        seen = set()
        for item in [*consumers, *inferred_downstream]:
            key = str(item.get("name") or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(item)

        impact_report["impacted_consumers"] = merged
        
        # 2. Determine Overall Criticality (Max of all consumers)
        criticality_levels = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
        max_level = 0
        
        for consumer in merged:
            level_str = str(consumer.get("criticality", "LOW")).upper()
            level_val = criticality_levels.get(level_str, 1)
            if level_val > max_level:
                max_level = level_val
                # Update the string representation
                impact_report["overall_criticality"] = level_str
                
        return impact_report

    def get_lineage_context(
        self,
        dataset_name: str,
        max_depth: int = 2,
        graph: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Return bounded upstream/downstream context for a dataset.
        """
        if isinstance(graph, dict):
            raw = graph.get("datasets", {})
            datasets = {str(k): (v if isinstance(v, dict) else {}) for k, v in (raw or {}).items()} if isinstance(raw, dict) else {}
        else:
            datasets = self._datasets_map()
        if dataset_name not in datasets:
            return {"dataset": dataset_name, "upstream": [], "downstream": [], "max_depth": max_depth}

        safe_depth = max(1, min(int(max_depth), 5))
        upstream_nodes: List[Dict[str, Any]] = []
        downstream_nodes: List[Dict[str, Any]] = []

        # Upstream BFS
        frontier = [(dataset_name, 0)]
        seen = {dataset_name}
        while frontier:
            node, depth = frontier.pop(0)
            if depth >= safe_depth:
                continue
            upstream = self._normalize_upstream_refs(datasets.get(node, {}).get("upstream", []))
            for item in upstream:
                name = str(item.get("name") or "")
                if not name:
                    continue
                upstream_nodes.append({"name": name, "depth": depth + 1, "managed": name in datasets})
                if name in datasets and name not in seen:
                    seen.add(name)
                    frontier.append((name, depth + 1))

        # Downstream BFS (reverse edges from upstream declarations)
        frontier = [(dataset_name, 0)]
        seen = {dataset_name}
        while frontier:
            node, depth = frontier.pop(0)
            if depth >= safe_depth:
                continue
            for candidate in self._collect_managed_downstream(node, datasets=datasets):
                name = str(candidate.get("name") or "")
                if not name:
                    continue
                downstream_nodes.append({"name": name, "depth": depth + 1, "managed": True})
                if name in datasets and name not in seen:
                    seen.add(name)
                    frontier.append((name, depth + 1))

        return {
            "dataset": dataset_name,
            "max_depth": safe_depth,
            "upstream": upstream_nodes,
            "downstream": downstream_nodes,
        }

    def build_graph_view(self, graph: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Build a graph payload for visualization surfaces.
        """
        current = graph if isinstance(graph, dict) else self.lineage_graph
        datasets = current.get("datasets", {}) if isinstance(current, dict) else {}
        if not isinstance(datasets, dict):
            datasets = {}

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []
        dataset_names = set(datasets.keys())
        external_nodes: set[str] = set()

        for name, info in datasets.items():
            info = info if isinstance(info, dict) else {}
            nodes.append(
                {
                    "id": name,
                    "kind": "dataset",
                    "owner": info.get("owner", "Unknown"),
                    "criticality": str(info.get("criticality", "UNKNOWN")).upper(),
                }
            )

            for up in self._normalize_upstream_refs(info.get("upstream", [])):
                up_name = str(up.get("name") or "")
                if not up_name:
                    continue
                managed = up_name in dataset_names
                edges.append(
                    {
                        "source": up_name,
                        "target": name,
                        "relation": "upstream",
                        "managed": managed,
                    }
                )
                if not managed:
                    external_nodes.add(up_name)

            for consumer in self._normalize_consumers(info.get("consumers", [])):
                consumer_name = str(consumer.get("name") or "")
                if not consumer_name:
                    continue
                managed = consumer_name in dataset_names
                edges.append(
                    {
                        "source": name,
                        "target": consumer_name,
                        "relation": "consumer",
                        "managed": managed,
                    }
                )
                if not managed:
                    external_nodes.add(consumer_name)

        for external in sorted(external_nodes):
            nodes.append({"id": external, "kind": "external", "owner": "Unknown", "criticality": "UNKNOWN"})

        return {"nodes": nodes, "edges": edges}

if __name__ == "__main__":
    # Create a dummy lineage file for testing if it doesn't exist
    dummy_lineage = {
        "datasets": {
            "transactions": {
                "consumers": [
                    {"name": "CEO_Revenue_Dashboard", "type": "dashboard", "owner": "Executive Team", "criticality": "HIGH"},
                    {"name": "Churn_Prediction_Model", "type": "ml_model", "owner": "Data Science", "criticality": "MEDIUM"}
                ]
            },
            "logs": {
                "consumers": [
                    {"name": "Dev_Debug_Tool", "type": "app", "owner": "Engineering", "criticality": "LOW"}
                ]
            }
        }
    }
    
    Path("config").mkdir(exist_ok=True)
    with open("config/lineage.yaml", "w") as f:
        yaml.dump(dummy_lineage, f)
        
    # Test the Analyzer
    analyzer = ImpactAnalyzer()
    
    print("\n🔍 Analyzing Impact for 'transactions':")
    report = analyzer.get_downstream_impact("transactions")
    print(yaml.dump(report, sort_keys=False))
    
    print("\n🔍 Analyzing Impact for 'logs':")
    report = analyzer.get_downstream_impact("logs")
    print(yaml.dump(report, sort_keys=False))
