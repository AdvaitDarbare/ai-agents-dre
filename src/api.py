from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import shutil
import uuid

# Import our MonitorAgent
from src.agents.monitor_agent import MonitorAgent
from src.utils.database import get_connection, init_tables

load_dotenv()

class RemediationApplyRequest(BaseModel):
    dataset_name: str
    proposed_yaml: str
    error_context: str

class GovernanceRollbackRequest(BaseModel):
    dataset_name: str
    filename: str

class ContractProposeRequest(BaseModel):
    dataset_name: str
    file_path: Optional[str] = None

app = FastAPI(title="Agentic DRE API")

# Enable CORS for the React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances (simplified for demo)
agent = MonitorAgent(contracts_path="config/expectations", lineage_path="config/lineage.yaml")

@app.get("/health")
def health_check():
    return {"status": "operational", "timestamp": datetime.now().isoformat()}

@app.get("/datasets")
def get_datasets():
    """Discover and return all dataset metadata."""
    try:
        datasets = agent.discover_datasets()
        return datasets
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pulse")
def get_pulse():
    """Get the current health status of all datasets."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT dataset_name, last_status, criticality, lifecycle,
                           last_file_mtime, last_scanned, scan_count
                    FROM dataset_registry
                """)
                rows = cur.fetchall()

                results = []
                for r in rows:
                    name = r[0]
                    history = agent.get_run_history(dataset_name=name, limit=10)
                    results.append({
                        "name": name,
                        "status": r[1],
                        "criticality": r[2],
                        "lifecycle": r[3],
                        "last_mtime": r[4],
                        "last_scanned": r[5].isoformat() if r[5] else None,
                        "scan_count": r[6],
                        "history": [h["quality_score"] for h in reversed(history)],
                        "quality_score": history[0]["quality_score"] if history else 100.0,
                        "reason": history[0].get("reason", "") if history else ""
                    })
                return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/evaluate/{dataset_name}")
def evaluate_dataset(dataset_name: str):
    """Trigger a health check for a specific dataset."""
    try:
        # Find the data file
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        if not meta or not meta.get("data_file"):
            raise HTTPException(status_code=404, detail=f"Data file for {dataset_name} not found.")
        
        result = agent.evaluate_data_file(meta["data_file"], dataset_name)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/datasets/{dataset_name}/data")
def get_dataset_data(dataset_name: str, limit: int = 100):
    """Get a sample/preview of the dataset content."""
    try:
        data = agent.get_dataset_sample(dataset_name, limit)
        return data
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Dataset file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/runs")
def get_recent_runs(limit: int = 50):
    """Get recent run history across all datasets."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT dataset_name, status, timestamp, quality_score,
                           reason, duration_ms
                    FROM run_history
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()

                results = []
                for i, r in enumerate(rows):
                    try:
                        dt = r[2]
                        if isinstance(dt, str):
                            dt = datetime.fromisoformat(dt)
                        time_str = dt.strftime("%I:%M %p")
                        date_str = dt.strftime("%Y-%m-%d")
                        if date_str == datetime.now().strftime("%Y-%m-%d"):
                            date_str = "Today"
                    except Exception:
                        time_str = "Unknown"
                        date_str = "Unknown"

                    duration_ms = r[5] or 0
                    results.append({
                        "id": f"run-{i}-{r[0]}",
                        "dataset": r[0],
                        "status": r[1],
                        "time": time_str,
                        "date": date_str,
                        "duration": f"{duration_ms / 1000:.1f}s" if duration_ms else "N/A",
                        "quality_score": r[3],
                        "reason": r[4]
                    })
                return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{dataset_name}")
def get_history(dataset_name: str, limit: int = 50):
    """Get run history for a dataset."""
    try:
        return agent.get_run_history(dataset_name, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/chat")
def chat_with_copilot(query: str):
    """Interact with the Agent reasoning engine."""
    try:
        context_data = {
            "discovered": agent.discover_datasets(),
            "results": {}
        }

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT dataset_name, last_status, criticality FROM dataset_registry")
                rows = cur.fetchall()
                for r in rows:
                    name = r[0]
                    history = agent.get_run_history(name, limit=1)
                    latest = history[0] if history else {}
                    context_data["results"][name] = {
                        "status": r[1],
                        "reason": latest.get("reason", "No recent run data"),
                        "anomalies": [],
                        "schema_evolution": {}
                    }

        response = agent.request_copilot_chat(query, context_data)
        return {"response": response}
    except Exception as e:
        print(f"Chat Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/lineage")
def get_lineage(dataset: Optional[str] = None):
    """Get the dependency graph, optionally filtered by dataset."""
    try:
        full_graph = agent.impact_analyzer.lineage_graph
        if not dataset:
            return full_graph
            
        # Filter for specific dataset context
        if dataset not in full_graph.get("datasets", {}):
             return {"datasets": {}}

        subset = {"datasets": {}}
        target_node = full_graph["datasets"][dataset]
        subset["datasets"][dataset] = target_node
        
        # Add Upstream nodes if they exist in the graph
        for up in target_node.get("upstream", []):
            name = up if isinstance(up, str) else up.get("name")
            if name and name in full_graph["datasets"]:
                subset["datasets"][name] = full_graph["datasets"][name]
                
        # Add Downstream nodes if they exist in the graph
        for consumer in target_node.get("consumers", []):
            name = consumer.get("name")
            if name and name in full_graph["datasets"]:
                 subset["datasets"][name] = full_graph["datasets"][name]

        return subset
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health/system")
def get_system_health():
    """Check health of all upstream services defined in lineage."""
    try:
        lineage = agent.impact_analyzer.lineage_graph
        health_results = []
        
        datasets = lineage.get("datasets", {})
        for ds_name, ds_info in datasets.items():
            upstreams = ds_info.get("upstream", [])
            for upstream_config in upstreams:
                # Run the health check tool
                status = agent.system_health.check_upstream_health(upstream_config)
                health_results.append({
                    "dataset": ds_name,
                    "upstream": status
                })
                
        return health_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/global")
def get_global_stats():
    """Get global platform statistics."""
    try:
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) as total_runs,
                        COUNT(CASE WHEN status = 'PASSED' THEN 1 END) as passed_runs
                    FROM run_history
                    WHERE timestamp >= %s
                """, (today_start,))
                today_stats = cur.fetchone()

                cur.execute("SELECT AVG(duration_ms) FROM run_history")
                avg_dur = cur.fetchone()[0]

                pass_rate = 0.0
                if today_stats and today_stats[0] > 0:
                    pass_rate = (today_stats[1] / today_stats[0]) * 100.0

                return {
                    "total_runs_today": today_stats[0] if today_stats else 0,
                    "pass_rate_today": round(pass_rate, 1),
                    "avg_duration": round(avg_dur or 0, 0)
                }
    except Exception:
        return {"total_runs_today": 0, "pass_rate_today": 0, "avg_duration": 0}

@app.get("/metrics/{dataset_name}")
def get_dataset_metrics(dataset_name: str):
    """Get latest cached metrics for a dataset."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT run_id, timestamp FROM run_history
                    WHERE dataset_name = %s
                    ORDER BY timestamp DESC LIMIT 1
                """, (dataset_name,))
                last_run = cur.fetchone()

                if not last_run:
                    return {"metrics": {}}

                cur.execute("""
                    SELECT metric_name, metric_value
                    FROM metric_history
                    WHERE run_id = %s
                """, (last_run[0],))
                metrics_rows = cur.fetchall()
                metrics = {r[0]: r[1] for r in metrics_rows}
                return {
                    "run_timestamp": last_run[1].isoformat() if last_run[1] else None,
                    "metrics": metrics
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/profile/{dataset_name}")
def get_dataset_profile(dataset_name: str):
    """Run a deep data profile check."""
    try:
        import pandas as pd
        from pathlib import Path
        
        # 1. Locate the dataset
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        
        if not meta or not meta.get("data_file"):
            raise HTTPException(status_code=404, detail="Dataset or data file not found.")
            
        data_path = Path(meta["data_file"])
        contract_val = meta.get("contract_path")
        contract_path = Path(contract_val) if contract_val else Path("dummy_non_existent.yaml")
        
        # 2. Load Data (Support CSV, limited to 5000 rows for real-time profiling)
        # In prod, this would be a SQL query or Spark job
        if data_path.suffix == ".csv":
            df = pd.read_csv(data_path, nrows=5000)
        else:
            df = pd.read_parquet(data_path)
            if len(df) > 5000:
                df = df.head(5000)
                
        # 3. Run Profiler
        profile = agent.profiler.profile(df, contract_path, dataset_name)
        return profile.to_dict()
        
    except Exception as e:
        print(f"Profile Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/remediation/{dataset_name}")
def get_remediation_plan(dataset_name: str):
    """Get remediation for a broken dataset (hybrid deterministic + LLM)."""
    try:
        # 1. Check last run status
        history = agent.get_run_history(dataset_name, limit=1)
        if not history:
            return {"status": "no_history", "message": "No run history found."}
            
        last_run = history[0]
        if last_run["status"] == "PASSED":
            return {"status": "healthy", "message": "Dataset is healthy. No remediation needed."}
            
        # 2. Get the failure reason
        error_details = last_run.get("reason", "Unknown error")
        
        # 3. Load current contract
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        contract_path = meta["contract_path"]
        
        with open(contract_path, "r") as f:
            current_yaml = f.read()
            
        # 4. Get Impact Context for better remediation
        try:
            impact = agent.impact_analyzer.get_downstream_impact(dataset_name)
            downstream_str = ", ".join(impact.get("downstream_systems", ["None"]))
            impact_context = f"This dataset is {impact.get('overall_criticality')} criticality. Downstream affected systems: {downstream_str}."
        except Exception:
            impact_context = "Impact unknown."

        # 5. Generate Fix using Hybrid Remediation
        proposed_yaml = None
        deterministic_yaml = None
        observed_yaml = None
        merge_summary = None
        generation_meta = None

        data_path = meta.get("data_file") if meta else None
        if data_path and Path(data_path).exists():
            try:
                hybrid = agent.remediator.propose_schema_update_hybrid(
                    current_yaml=current_yaml,
                    data_path=data_path,
                    error_details=error_details,
                    impact_context=impact_context,
                    enable_llm=True,
                )
                deterministic_yaml = hybrid.get("deterministic_yaml")
                proposed_yaml = hybrid.get("llm_yaml") or deterministic_yaml
                observed_yaml = hybrid.get("observed_yaml")
                merge_summary = hybrid.get("merge_summary")
                generation_meta = hybrid.get("generation")
            except Exception as hybrid_err:
                proposed_yaml = f"# Error generating fix: {hybrid_err}"
        else:
            # Fallback to LLM-only if we have no data file
            try:
                proposed_yaml = agent.remediator.propose_schema_update(current_yaml, error_details, impact_context)
            except Exception as llm_err:
                proposed_yaml = f"# Error generating fix: {llm_err}"

        return {
            "status": "remediation_available",
            "dataset": dataset_name,
            "error": error_details,
            "original_yaml": current_yaml,
            "proposed_yaml": proposed_yaml,
            "deterministic_yaml": deterministic_yaml,
            "observed_yaml": observed_yaml,
            "merge_summary": merge_summary,
            "generation": generation_meta,
        }
    except Exception as e:
        print(f"Error getting remediation plan for {dataset_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
        
@app.post("/remediation/apply")
def apply_remediation(request: RemediationApplyRequest):
    """Apply an AI-generated fix and log it to the audit trail."""
    try:
        # 1. Find the contract path
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == request.dataset_name), None)
        if not meta:
            raise HTTPException(status_code=404, detail="Dataset not found")
            
        contract_path = meta["contract_path"]
        
        # 2. Get original YAML for logging
        with open(contract_path, "r") as f:
            original_yaml = f.read()
            
        # 3. Apply the fix (creates backup + writes file)
        # Note: apply_fix now returns the path to the NEW version in /history
        version_path = agent.remediator.apply_fix(contract_path, request.proposed_yaml)
        version_filename = os.path.basename(version_path)
        
        # 4. Log to remediation_history AND schema_audit_log
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO remediation_history (dataset_name, error_context, original_yaml, proposed_yaml, backup_path)
                    VALUES (%s, %s, %s, %s, %s)
                """, (request.dataset_name, request.error_context, original_yaml, request.proposed_yaml, version_path))

                cur.execute("""
                    INSERT INTO schema_audit_log (id, dataset_name, filename, timestamp, change_summary)
                    VALUES (%s, %s, %s, NOW(), %s)
                """, (str(uuid.uuid4()), request.dataset_name, version_filename, f"AI Remediation: {request.error_context}"))
            
        return {
            "status": "success",
            "message": f"Remediation applied. Version saved: {version_filename}",
            "backup_path": version_path
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Governance Endpoints ---

@app.get("/governance/{dataset_name}/history")
def get_governance_history(dataset_name: str):
    """Get the full history of schema versions for a dataset."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT filename, timestamp, change_summary
                    FROM schema_audit_log
                    WHERE dataset_name = %s
                    ORDER BY timestamp DESC
                """, (dataset_name,))
                rows = cur.fetchall()
                return [
                    {
                        "filename": r[0],
                        "timestamp": r[1].isoformat() if r[1] else "",
                        "summary": r[2]
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/governance/file/{filename}")
def get_historical_file(filename: str):
    """Read the content of a historical schema version."""
    try:
        # Security: Ensure we only read from config/history
        history_dir = Path("config/history")
        file_path = history_dir / filename
        
        # Simple path traversal check
        if not file_path.resolve().is_relative_to(history_dir.resolve()):
             raise HTTPException(status_code=403, detail="Access denied")
             
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
            
        with open(file_path, "r") as f:
            return {"content": f.read()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/governance/rollback")
def rollback_schema(request: GovernanceRollbackRequest):
    """Revert the active schema to a selected historical version."""
    try:
        # 1. Verify files
        history_dir = Path("config/history")
        source_path = history_dir / request.filename
        
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == request.dataset_name), None)
        if not meta:
            raise HTTPException(status_code=404, detail="Dataset not found")
        target_path = Path(meta["contract_path"])
        
        if not source_path.exists():
             raise HTTPException(status_code=404, detail="Historical version not found")

        # 2. Perform Rollback (Copy history -> active)
        # We also create a NEW history entry for the rollback event itself
        import shutil
        import uuid
        
        # Read content to log it
        with open(source_path, "r") as f:
            content = f.read()
            
        # Overwrite active file
        shutil.copy2(source_path, target_path)
        
        # Log the rollback
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO schema_audit_log (id, dataset_name, filename, timestamp, change_summary)
                    VALUES (%s, %s, %s, NOW(), %s)
                """, (str(uuid.uuid4()), request.dataset_name, request.filename, f"Rollback to version: {request.filename}"))

        # 3. Trigger Smart Scan (Immediate Validation)
        scan_result = {}
        try:
            # Re-using the evaluate_dataset logic directly
            scan_result = evaluate_dataset(request.dataset_name)
        except Exception as scan_err:
            print(f"Post-rollback scan failed: {scan_err}")
            scan_result = {"status": "ERROR", "message": "Rollback successful, but immediate scan failed."}
            
        return {
            "status": "success", 
            "message": f"Rolled back to {request.filename}. Scan Result: {scan_result.get('status', 'Unknown')}",
            "scan_details": scan_result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/contracts/propose")
def propose_contract_endpoint(request: ContractProposeRequest):
    """Generate a proposed contract from data."""
    try:
        # Determine data path
        data_path = request.file_path
        if not data_path:
            # Try to infer from existing discovery
            datasets = agent.discover_datasets()
            meta = next((d for d in datasets if d["name"] == request.dataset_name), None)
            if meta:
                data_path = meta.get("data_file")
        
        # Fallback
        if not data_path:
             data_path = f"data/{request.dataset_name}.csv"
             
        if not Path(data_path).exists():
             raise HTTPException(status_code=404, detail=f"Data file not found at {data_path}")

        proposal = agent.propose_contract(request.dataset_name, data_path, include_metadata=True)
        return {
            "status": "success",
            "proposed_yaml": proposal.get("yaml_content", ""),
            "generation": {
                "engine": proposal.get("engine"),
                "success": proposal.get("success"),
                "cli_available": proposal.get("cli_available"),
                "errors": proposal.get("errors", []),
                "warnings": proposal.get("warnings", []),
                "generated_at": proposal.get("generated_at"),
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/contracts/{dataset_name}")
def get_active_contract_endpoint(dataset_name: str):
    """Get the current active contract content."""
    try:
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == dataset_name), None)
        if not meta:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        contract_path = meta["contract_path"]
        if not os.path.exists(contract_path):
             return {"content": "# No contract found"}
             
        with open(contract_path, "r") as f:
            return {"content": f.read()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class ContractSaveRequest(BaseModel):
    dataset_name: str
    yaml_content: str
    summary: str = "Manual definition update"

@app.post("/contracts/save")
def save_contract_endpoint(request: ContractSaveRequest):
    """Save a user-approved contract to file."""
    try:
        # Find the contract path
        datasets = agent.discover_datasets()
        meta = next((d for d in datasets if d["name"] == request.dataset_name), None)
        
        if not meta or not meta.get("contract_path"):
            # Fallback path if not found in metadata
            contract_path = f"config/expectations/{request.dataset_name}.yaml"
        else:
            contract_path = meta["contract_path"]

        # Use Remediator to apply safely (creates backup + history)
        from src.tools.schema_remediator import SchemaRemediator
        remediator = SchemaRemediator()
        
        # apply_fix returns the path to the backup/history file
        remediator.apply_fix(contract_path, request.yaml_content)
        
        # Log to audit table
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO schema_audit_log (id, dataset_name, filename, timestamp, change_summary)
                    VALUES (%s, %s, %s, NOW(), %s)
                """, (str(uuid.uuid4()), request.dataset_name, "manual_update.yaml", request.summary))

        return {"status": "success", "message": "Contract saved successfully", "path": contract_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/contract/{dataset_name}")
def get_current_contract(dataset_name: str):
    """Get the current active contract YAML content."""
    try:
        import yaml
        contract_path = f"config/expectations/{dataset_name}.yaml"

        if not Path(contract_path).exists():
            raise HTTPException(status_code=404, detail=f"Contract not found: {contract_path}")

        with open(contract_path, 'r') as f:
            yaml_content = f.read()

        return {"yaml_content": yaml_content, "path": contract_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/contract-history/{dataset_name}")
def get_contract_history(dataset_name: str):
    """Get version history for a contract from contract_versions table."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, dataset_name, created_at, created_by, contract_content, change_type
                    FROM contract_versions
                    WHERE dataset_name = %s
                    ORDER BY created_at DESC
                    LIMIT 50
                """, (dataset_name,))

                rows = cur.fetchall()
                versions = []
                for row in rows:
                    versions.append({
                        "version_id": str(row[0]),  # Use id as version_id
                        "dataset_name": row[1],
                        "timestamp": row[2].isoformat() if row[2] else None,
                        "changed_by": row[3] or "user",
                        "yaml_content": row[4],
                        "change_type": row[5] or "manual_edit"
                    })

                return versions
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/contract/{dataset_name}/version/{version_id}")
def get_contract_version(dataset_name: str, version_id: str):
    """Get a specific version of a contract."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT contract_content
                    FROM contract_versions
                    WHERE dataset_name = %s AND id = %s
                """, (dataset_name, int(version_id)))

                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Version not found")

                return {"yaml_content": row[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/contract/{dataset_name}")
def save_contract_version(dataset_name: str, request: dict):
    """Save a new version of the contract."""
    try:
        import uuid
        from datetime import datetime

        yaml_content = request.get("yaml_content")
        change_type = request.get("change_type", "manual_edit")
        changed_by = request.get("changed_by", "user")

        if not yaml_content:
            raise HTTPException(status_code=400, detail="yaml_content is required")

        # Save to file
        contract_path = f"config/expectations/{dataset_name}.yaml"
        with open(contract_path, 'w') as f:
            f.write(yaml_content)

        # Calculate hash for the content
        import hashlib
        contract_hash = hashlib.sha256(yaml_content.encode()).hexdigest()

        # Save version to database
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO contract_versions (dataset_name, contract_path, contract_content, contract_hash, created_by, change_type)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (dataset_name, contract_path, yaml_content, contract_hash, changed_by, change_type))

                version_id = cur.fetchone()[0]

        return {"status": "success", "version_id": str(version_id), "message": "Contract saved successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/contract/{dataset_name}/ai-modify")
def ai_modify_contract(dataset_name: str, request: dict):
    """Use AI to modify the contract based on natural language instruction."""
    try:
        import yaml as yaml_lib
        from agno.agent import Agent
        from agno.models.openai import OpenAIChat

        instruction = request.get("instruction")
        current_yaml = request.get("current_yaml")

        if not instruction or not current_yaml:
            raise HTTPException(status_code=400, detail="instruction and current_yaml are required")

        # Use Agno to modify the contract
        agent = Agent(
            model=OpenAIChat(id="gpt-4o"),
            markdown=False
        )

        prompt = f"""You are a data contract modification assistant. The user wants to modify a YAML data contract.

Current YAML contract:
```yaml
{current_yaml}
```

User instruction: {instruction}

Please modify the YAML contract according to the instruction and return ONLY the modified YAML (no explanations, no markdown code blocks, just the raw YAML).

Common modifications:
- "enforce null values for X" or "make X not nullable" → set nullable: false
- "make X nullable" → set nullable: true
- "add pattern to X" → add pattern: 'regex'
- "add allowed_values to X" → add allowed_values: [...]
- "add unique constraint to X" → add isPrimaryKey: true
- "set min/max for X" → add min_value/max_value

Return the complete modified YAML contract."""

        response = agent.run(prompt)
        modified_yaml = response.content.strip()

        # Remove markdown code blocks if present
        if modified_yaml.startswith("```"):
            lines = modified_yaml.split("\n")
            modified_yaml = "\n".join(lines[1:-1]) if len(lines) > 2 else modified_yaml

        # Validate it's valid YAML
        try:
            yaml_lib.safe_load(modified_yaml)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"AI generated invalid YAML: {str(e)}")

        return {
            "modified_yaml": modified_yaml,
            "explanation": f"Modified contract based on: {instruction}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- New Visualization Endpoints ---

@app.get("/incidents")
def get_incidents(limit: int = 50):
    """Get BLOCKED/WARNING runs as incidents for the Incident Feed."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT run_id, timestamp, dataset_name, status,
                           quality_score, anomaly_count, z_score_max, reason, duration_ms
                    FROM run_history
                    WHERE status IN ('BLOCKED', 'WARNING')
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
                return [
                    {
                        "run_id": r[0],
                        "timestamp": r[1].isoformat() if r[1] else None,
                        "dataset": r[2],
                        "severity": "CRITICAL" if r[3] == "BLOCKED" else "WARNING",
                        "status": r[3],
                        "quality_score": r[4],
                        "anomaly_count": r[5],
                        "z_score_max": r[6],
                        "reason": r[7],
                        "duration_ms": r[8],
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/quality-dimensions/{dataset_name}")
def get_quality_dimensions(dataset_name: str):
    """
    Get 6-dimensional quality scores for a dataset from the latest run.

    Returns:
        {
            "dataset_name": "orders",
            "timestamp": "2026-02-15T14:00:00Z",
            "overall_score": 88.5,
            "dimensions": [
                {"name": "Validity", "score": 98.0, "weight": 0.25, "status": "PASS", ...},
                {"name": "Completeness", "score": 75.2, "weight": 0.25, "status": "FAIL", ...},
                ...
            ]
        }
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Get latest run with dimension_scores
                cur.execute("""
                    SELECT dimension_scores
                    FROM run_history
                    WHERE dataset_name = %s AND dimension_scores IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (dataset_name,))

                row = cur.fetchone()

                if not row or not row[0]:
                    raise HTTPException(status_code=404, detail="No dimension scores found. Run evaluation first.")

                # psycopg2 automatically parses JSONB to dict
                return row[0]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/{dataset_name}/timeseries")
def get_metric_timeseries(dataset_name: str, metric: str = "row_count", limit: int = 30):
    """Get time-series data for a specific metric (for charts)."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT mh.timestamp, mh.metric_value, mh.run_id, mh.day_of_week
                    FROM metric_history mh
                    WHERE mh.dataset_name = %s AND mh.metric_name = %s
                    ORDER BY mh.timestamp DESC
                    LIMIT %s
                """, (dataset_name, metric, limit))
                rows = cur.fetchall()

                # Also get baseline for confidence bands
                cur.execute("""
                    SELECT baseline_mean, baseline_std, baseline_type, sample_count
                    FROM learned_thresholds
                    WHERE dataset_name = %s AND metric_name = %s
                """, (dataset_name, metric))
                baseline_row = cur.fetchone()

                baseline = None
                if baseline_row:
                    baseline = {
                        "mean": baseline_row[0],
                        "std": baseline_row[1],
                        "type": baseline_row[2],
                        "sample_count": baseline_row[3],
                        "upper_3sigma": baseline_row[0] + 3 * baseline_row[1],
                        "lower_3sigma": baseline_row[0] - 3 * baseline_row[1],
                        "upper_2sigma": baseline_row[0] + 2 * baseline_row[1],
                        "lower_2sigma": baseline_row[0] - 2 * baseline_row[1],
                    }

                return {
                    "dataset": dataset_name,
                    "metric": metric,
                    "baseline": baseline,
                    "data": [
                        {
                            "timestamp": r[0].isoformat() if r[0] else None,
                            "value": r[1],
                            "run_id": r[2],
                            "day_of_week": r[3],
                        }
                        for r in reversed(rows)  # chronological order
                    ]
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/baselines/{dataset_name}")
def get_baselines(dataset_name: str):
    """Get all learned thresholds/baselines for a dataset."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT metric_name, baseline_mean, baseline_std,
                           baseline_type, last_updated, sample_count
                    FROM learned_thresholds
                    WHERE dataset_name = %s
                    ORDER BY metric_name
                """, (dataset_name,))
                rows = cur.fetchall()
                return [
                    {
                        "metric": r[0],
                        "mean": r[1],
                        "std": r[2],
                        "type": r[3],
                        "last_updated": r[4].isoformat() if r[4] else None,
                        "sample_count": r[5],
                        "upper_3sigma": r[1] + 3 * r[2] if r[2] else r[1],
                        "lower_3sigma": r[1] - 3 * r[2] if r[2] else r[1],
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
