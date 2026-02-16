
import os
import pandas as pd
import json
import random
from dotenv import load_dotenv
load_dotenv()
from src.agents.monitor_agent import MonitorAgent

# 0. Setup Mock Data
os.makedirs("data/test", exist_ok=True)

# 1. Initialize Agent
print("🤖 Initializing Monitor Agent...")
agent = MonitorAgent(contracts_path="config/expectations", lineage_path="config/lineage.yaml")

# Logic for transactions dataset has been removed.
