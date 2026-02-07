"""Generate sample test data for the DRE system."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Create data directory
Path("data").mkdir(exist_ok=True)

# Generate sample transaction data
np.random.seed(42)
n_rows = 1000

data = {
    'transaction_id': range(1, n_rows + 1),
    'amount': np.random.exponential(scale=50, size=n_rows),  # Skewed distribution
    'timestamp': [
        (datetime.now() - timedelta(hours=i)).isoformat() 
        for i in range(n_rows)
    ],
    'user_id': np.random.randint(1, 100, size=n_rows),
    'user_comment': [
        f"Comment {i}" if np.random.random() > 0.3 else None 
        for i in range(n_rows)
    ],
    'category': np.random.choice(['A', 'B', 'C', None], size=n_rows, p=[0.4, 0.3, 0.25, 0.05])
}

df = pd.DataFrame(data)

# Add some outliers (for testing outlier detection)
outlier_indices = np.random.choice(n_rows, size=20, replace=False)
df.loc[outlier_indices, 'amount'] = np.random.uniform(500, 1000, size=20)

# Save clean data
df.to_csv('data/transactions_clean.csv', index=False)
print(f"✅ Generated clean data: data/transactions_clean.csv ({len(df)} rows)")

# Generate data with critical errors (missing critical column)
df_bad = df.copy()
df_bad = df_bad.drop(columns=['transaction_id'])
df_bad.to_csv('data/transactions_bad.csv', index=False)
print(f"❌ Generated bad data: data/transactions_bad.csv (missing transaction_id)")

# Generate data with warnings (high null percentage)
df_warnings = df.copy()
df_warnings.loc[df_warnings.sample(frac=0.6).index, 'user_comment'] = None
df_warnings.to_csv('data/transactions_warnings.csv', index=False)
print(f"⚠️  Generated warning data: data/transactions_warnings.csv (60% null comments)")

print("\n✅ Test data generation complete!")
