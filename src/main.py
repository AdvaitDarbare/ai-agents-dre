
import os
import pandas as pd
import json
import random
from src.agents.monitor_agent import MonitorAgent

# 0. Setup Mock Data
os.makedirs("data/test", exist_ok=True)
mock_file = "data/test/transactions.csv"

# Create a "Good" file first
print("📝 Creating 'Perfect' Mock Data...")
df = pd.DataFrame({
    "transaction_id": [f"txn_{i}" for i in range(100)],
    "user_id": [f"user_{i}" for i in range(100)],
    "amount": [100.0] * 100,
    "timestamp": pd.to_datetime(["2023-01-01"] * 100),
    "status": ["completed"] * 100
})
df.to_csv(mock_file, index=False)

# 1. Initialize Agent
print("🤖 Initializing Monitor Agent...")
agent = MonitorAgent(contracts_path="config/expectations", lineage_path="config/lineage.yaml")

# 2. Run Test 1 (Perfect Data)
print("\n📊 Running Test 1: Perfect Transaction Data")
result = agent.evaluate_data_file(mock_file, "transactions")
print(json.dumps(result, indent=2))

# 3. Run Test 2 (Critical Anomaly + High Impact)
print("\n🔥 Running Test 2: Critical Anomaly (Volume Drop)")
# Overwrite with BAD data (Volume Drop)
df_bad = pd.DataFrame({
    "transaction_id": [f"txn_{i}" for i in range(5)],  # Volume = 5 (Expected ~100)
    "user_id": [f"user_{i}" for i in range(5)],
    "amount": [100.0] * 5,
    "timestamp": pd.to_datetime(["2023-01-01"] * 5),
    "status": ["completed"] * 5
})
df_bad.to_csv(mock_file, index=False)

result_bad = agent.evaluate_data_file(mock_file, "transactions")
print(json.dumps(result_bad, indent=2))

# 4. Run Test 3 (Schema Evolution)
print("\n🌱 Running Test 3: Schema Evolution (New Column)")
# Overwrite with NEW COLUMN data
df_new_col = pd.DataFrame({
    "transaction_id": [f"txn_{i}" for i in range(100)],
    "user_id": [f"user_{i}" for i in range(100)],
    "amount": [100.0] * 100,
    "timestamp": pd.to_datetime(["2023-01-01"] * 100),
    "status": ["completed"] * 100,
    "loyalty_points": [10] * 100   # New Column
})
df_new_col.to_csv(mock_file, index=False)

result_evolve = agent.evaluate_data_file(mock_file, "transactions")
print(json.dumps(result_evolve, indent=2))

# 5. Run Test 4 (Distribution Drift)
print("\n📈 Running Test 4: Data Drift (Price Doubling)")
# Overwrite with DRIFTED data (Prices Doubled)
df_drift = pd.DataFrame({
    "transaction_id": [f"txn_{i}" for i in range(100)],
    "user_id": [f"user_{i}" for i in range(100)],
    "amount": [200.0] * 100, # Mean is now 200.0 (Doubled from baseline 100.0)
    "timestamp": [pd.Timestamp("2023-01-01")] * 100,
    "status": ["completed"] * 100
})
df_drift.to_csv(mock_file, index=False)

result_drift = agent.evaluate_data_file(mock_file, "transactions")
print(json.dumps(result_drift, indent=2))
