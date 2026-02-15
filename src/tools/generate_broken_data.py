
import pandas as pd
import numpy as np
import os
import shutil

# Setup
landing_path = "data/landing"
test_path = "data/test"
os.makedirs(test_path, exist_ok=True)

source_file = f"{landing_path}/nyc_taxi.csv"
print(f"Reading source file: {source_file}")

# Load Valid Data
df = pd.read_csv(source_file)
print(f"Original Data Shape: {df.shape}")

# Shift timestamps to current year for freshness check pass
current_year = pd.Timestamp.now().year
# Assuming date format is MM/DD/YYYY HH:MM:SS AM/PM
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], format="%m/%d/%Y %I:%M:%S %p")
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], format="%m/%d/%Y %I:%M:%S %p")

# Replace year
df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].apply(lambda dt: dt.replace(year=current_year))
df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].apply(lambda dt: dt.replace(year=current_year))

# Also ensure valid data doesn't trigger row count max (max is 2500, we have 2000)
df = df.head(2000)

# 1. Valid File (Golden Copy)
# Just copy the landing file to test as the "valid" version for reference
shutil.copy(source_file, f"{test_path}/nyc_taxi.csv")
print(f"✅ Created Valid File: {test_path}/nyc_taxi.csv")


# 2. Schema Drift: Rename 'VendorID' to 'id'
print("\n🔥 Creating Broken File 1: Schema Drift...")
df_drift = df.copy()
df_drift = df_drift.rename(columns={"VendorID": "id"})
df_drift.to_csv(f"{test_path}/nyc_taxi_schema_drift.csv", index=False)
print(f"   Saved: {test_path}/nyc_taxi_schema_drift.csv")


# 3. Volume Anomaly: Add 500 rows to a 1,000-row file
# (Our sample is 2000 rows, so let's add 50% more rows to trigger anomaly if threshold is sensitive)
print("\n🔥 Creating Broken File 2: Volume Anomaly...")
df_volume = df.copy()
# Duplicate the first 1000 rows and append
extra_rows = df.head(1000).copy()
df_volume = pd.concat([df_volume, extra_rows], ignore_index=True)
df_volume.to_csv(f"{test_path}/nyc_taxi_volume_anomaly.csv", index=False)
print(f"   Saved: {test_path}/nyc_taxi_volume_anomaly.csv (Rows: {len(df_volume)})")


# 4. Null Spike: Change 20% of 'total_amount' to NULL
print("\n🔥 Creating Broken File 3: Null Spike...")
df_nulls = df.copy()
mask = np.random.choice([True, False], size=len(df_nulls), p=[0.2, 0.8])
df_nulls.loc[mask, "total_amount"] = np.nan
df_nulls.to_csv(f"{test_path}/nyc_taxi_null_spike.csv", index=False)
print(f"   Saved: {test_path}/nyc_taxi_null_spike.csv")

print("\n✨ Done! Broken data files are ready for the Agent.")
