
import pandas as pd
import numpy as np
from src.tools.anomaly_detector import AnomalyDetector
import json

# Initialize Detector
detector = AnomalyDetector()

print("🧠 Training Memory with 10 Clean DataFrames (0% NULLS)...")
# Simulate 10 days of clean data (0% nulls in 'email')
for i in range(10):
    # Create fake dataframe
    df = pd.DataFrame({
        'transaction_id': range(100),
        'email': [f"user{x}@example.com" for x in range(100)],  # 0% nulls
        'amount': np.random.normal(100, 10, 100)
    })
    
    # Evaluate (which saves metrics internally in a real run, here we're just training)
    # Note: In our current implementation, evaluate_run doesn't auto-save to history.
    # So we must manually save the metrics to simulate "History Learning".
    
    # Calculate metrics manually for training simulation
    metrics = {
        'row_count': len(df),
        'null_rate_email': df['email'].isnull().mean(),
        'mean_amount': df['amount'].mean()
    }
    detector.save_run_metrics('transactions_drift_test', metrics)

print("✅ Training Complete.")

# ---------------------------------------------------------
# Test Case: Distribution Drift (20% Nulls)
# ---------------------------------------------------------
print("\n🔍 Evaluating DIRTY Run (20% Nulls in 'email')...")

# Create dirty dataframe
df_dirty = pd.DataFrame({
    'transaction_id': range(100),
    'email': [f"user{x}@example.com" if x < 80 else None for x in range(100)], # 20% nulls
    'amount': np.random.normal(100, 10, 100)
})

# Run Detection
# passing the dataframe allows the tool to auto-calculate null rates
report = detector.evaluate_run('transactions_drift_test', {'row_count': len(df_dirty)}, dataframe=df_dirty)

# Check Results
null_rate_metric = report['metrics'].get('null_rate_email')
print("\n📊 RESULT Analysis:")
print(f"   Metric: null_rate_email")
print(f"   Value: {null_rate_metric['value']:.2%}")
print(f"   Z-Score: {null_rate_metric['z_score']:.2f}")
print(f"   Is Anomaly: {null_rate_metric['is_anomaly']}")
print(f"   Reason: {null_rate_metric['reason']}")

if null_rate_metric['is_anomaly']:
    print("\n✅ SUCCESS: Detected Distribution Drift!")
else:
    print("\n❌ FAILURE: Failed to detect Drift.")
