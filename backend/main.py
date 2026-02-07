from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/reports")
def list_reports():
    import os
    from pathlib import Path
    
    reports_path = Path("reports")
    if not reports_path.exists():
         reports_path = Path("/Users/advaitdarbare/Desktop/ai-agents-dre/reports")

    # Return list of filenames sorted by time (newest first)
    files = sorted(reports_path.glob("monitor_report_*.json"), key=os.path.getmtime, reverse=True)
    return [f.name for f in files]

@app.get("/api/health")
def get_health(report: str = None):
    import json
    import os
    from pathlib import Path
    
    reports_path = Path("reports")
    if not reports_path.exists():
         reports_path = Path("/Users/advaitdarbare/Desktop/ai-agents-dre/reports")

    if report:
        target_file = reports_path / report
        if not target_file.exists():
            return {"status": "ERROR", "message": "Report not found"}
    else:
        # Default to latest
        json_files = list(reports_path.glob("monitor_report_*.json"))
        if not json_files:
            return {"status": "NO_DATA", "message": "No reports found"}
        target_file = max(json_files, key=os.path.getmtime)
    
    with open(target_file, "r") as f:
        data = json.load(f)
    
    data["report_filename"] = target_file.name
    return data

@app.get("/api/file-content")
def get_file_content(path: str):
    import csv
    from pathlib import Path
    
    # Security: Prevent directory traversal
    if ".." in path or path.startswith("/"):
        # Allow absolute path if it matches known project root
        known_root = "/Users/advaitdarbare/Desktop/ai-agents-dre"
        if not path.startswith(known_root):
             # Try to adjust relative path
             pass
    
    # Resolve Path
    # If running from backend directory, data is at ../data
    # If running from root, data is at ./data
    # The report usually says "data/filename.csv"
    
    # Try finding the file in typical locations
    possible_paths = [
        Path(path), # Absolute or relative to CWD
        Path("..") / path, # Relative to backend
        Path("/Users/advaitdarbare/Desktop/ai-agents-dre") / path
    ]
    
    target_file = None
    for p in possible_paths:
        if p.exists() and p.is_file():
            target_file = p
            break
            
    if not target_file:
        return {"status": "ERROR", "message": f"File not found: {path}"}

    try:
        data = []
        with open(target_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Limit to 2000 rows to prevent huge payloads
            for i, row in enumerate(reader):
                if i >= 2000: break
                data.append(row)
        return {"status": "OK", "data": data, "count": len(data)}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

from pydantic import BaseModel

class ContractDraft(BaseModel):
    table_name: str
    yaml_content: str
    change_message: str = "Updated via UI"
    
@app.post("/api/contracts")
def save_contract(draft: ContractDraft):
    from pathlib import Path
    import shutil
    import yaml
    from datetime import datetime
    
    root_path = Path("/Users/advaitdarbare/Desktop/ai-agents-dre")
    contract_path = root_path / "contracts" / f"{draft.table_name}.yaml"
    archive_dir = root_path / "contracts" / "archive"
    
    # Create dir if not exists (though it should)
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # Archive Logic (Version History)
    if contract_path.exists():
        try:
            with open(contract_path, 'r') as f:
                content = f.read()
                # Simple parsing to get version (avoid full deps if possible, but yaml is safe)
                try:
                    data = yaml.safe_load(content)
                    version = data.get('info', {}).get('version', '0.0.0')
                except:
                    version = "unknown"
                
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_filename = f"{draft.table_name}_v{version}_{timestamp}.yaml"
            shutil.copy(contract_path, archive_dir / archive_filename)
        except Exception as e:
            print(f"Warning: Failed to archive contract: {e}")
    
    try:
        with open(contract_path, "w") as f:
            f.write(draft.yaml_content)
        return {"status": "SUCCESS", "message": f"Contract saved to {contract_path}"}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

@app.get("/api/contracts/{table_name}/history")
def get_contract_history(table_name: str):
    from pathlib import Path
    import os
    
    root_path = Path("/Users/advaitdarbare/Desktop/ai-agents-dre")
    archive_dir = root_path / "contracts" / "archive"
    
    history = []
    
    if archive_dir.exists():
        # strict glob for table_name
        # Pattern: table_name_vVersion_Timestamp.yaml
        for f in archive_dir.glob(f"{table_name}_*.yaml"):
            try:
                # Parse filename
                # Example: users_v1.0.0_20260207_035000.yaml
                parts = f.stem.split('_')
                # timestamp is last 2 parts (date_time)
                timestamp_str = f"{parts[-2]}_{parts[-1]}"
                version_str = parts[-3] if len(parts) >= 3 else "unknown"
                
                history.append({
                    "filename": f.name,
                    "version": version_str,
                    "timestamp": timestamp_str,
                    "path": str(f)
                })
            except:
                pass
                
    # Sort by timestamp descending
    history.sort(key=lambda x: x['timestamp'], reverse=True)
    return history

@app.get("/api/contracts/archive/{filename}")
def get_archived_contract(filename: str):
    from pathlib import Path
    
    # Security: basic check
    if ".." in filename or "/" in filename:
        return {"status": "ERROR", "message": "Invalid filename"}
        
    root_path = Path("/Users/advaitdarbare/Desktop/ai-agents-dre")
    archive_path = root_path / "contracts" / "archive" / filename
    
    if not archive_path.exists():
        return {"status": "ERROR", "message": "File not found"}
        
    try:
        with open(archive_path, "r") as f:
            content = f.read()
        return {"status": "SUCCESS", "content": content}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

class PipelineRunRequest(BaseModel):
    file_path: str
    table_name: str

@app.post("/api/pipeline/run")
def run_pipeline(request: PipelineRunRequest):
    import subprocess
    import sys
    from pathlib import Path
    
    # Determine root directory (assuming valid project structure)
    # If running from backend dir, root is ..
    root_dir = Path("..").resolve()
    if not (root_dir / "main.py").exists():
        # Fallback to current dir if running from root
        root_dir = Path(".").resolve()
    
    if not (root_dir / "main.py").exists():
         return {"status": "ERROR", "message": "Could not locate main.py"}

    # Run the main.py script
    # We use sys.executable to ensure we use the same python interpreter
    cmd = [sys.executable, "main.py", request.file_path, request.table_name]
    
    try:
        # Run process (wait for completion)
        result = subprocess.run(
            cmd, 
            cwd=str(root_dir),
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            return {
                "status": "SUCCESS", 
                "message": "Pipeline completed successfully",
                "output": result.stdout
            }
        else:
            return {
                "status": "FAIL",
                "message": "Pipeline failed (check logic or validation errors)",
                "output": result.stdout,
                "error": result.stderr
            }
            
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
