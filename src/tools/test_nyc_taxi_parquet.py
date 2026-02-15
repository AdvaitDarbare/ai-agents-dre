
import os
import json
from src.agents.monitor_agent import MonitorAgent

def run_nyc_tests():
    # Enable Mock Mode for Doris Loader
    os.environ["DORIS_MOCK_MODE"] = "True"
    
    # Initialize Agent
    agent = MonitorAgent(contracts_path="config/expectations", lineage_path="config/lineage.yaml")
    # 2. Iterate through scenarios
    # Run Valid file multiple times to build history for Anomaly Detector (needs >= 3 runs)
    test_files = [
        ("NYC Taxi Parquet (Direct from Source)", "data/test/yellow_tripdata_2025-01.parquet")
    ]
    
    print("🚖 Running NYC Taxi Agentic Checks (PARQUET EDITION)...\n")
    
    for name, file_path in test_files:
        print(f"👉 Testing: {name} ({file_path})")
        if not os.path.exists(file_path):
            print(f"   ❌ File not found: {file_path}")
            continue
            
        result = agent.evaluate_data_file(file_path, "nyc_taxi")
        
        status = result["status"]
        score = result.get("quality_score", "N/A")
        reason = result.get("reason", "No reason provided")
        
        icon = "✅" if status == "PASSED" else "mw-parser-output"
        if status == "BLOCKED": icon = "🔥"
        
        print(f"   {icon} Result: {status} (Score: {score})")
        print(f"   📝 Reason: {reason}\n")

if __name__ == "__main__":
    run_nyc_tests()
