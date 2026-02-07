"""
Generate PERFECT test data that passes all Monitor Agent checks.
No warnings, no errors - pure SUCCESS.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

# Generate 1000 perfect transactions
n = 1000

data = {
    'transaction_id': range(1, n + 1),
    'amount': np.random.exponential(50, n),  # Positive amounts
    'timestamp': [(datetime.now() - timedelta(hours=i)).isoformat() for i in range(n)],
    'user_id': np.random.randint(1, 100, n),
    'user_comment': [f'Comment {i}' for i in range(n)],  # NO NULLS
    'category': np.random.choice(['A', 'B', 'C'], n)  # NO NULLS
}

df = pd.DataFrame(data)

# Save
output_file = 'data/transactions_perfect.csv'
df.to_csv(output_file, index=False)

print(f"✅ Generated PERFECT data: {output_file} ({len(df)} rows)")
print(f"   - 0% nulls in all columns")
print(f"   - All required columns present")
print(f"   - All data types correct")
print(f"   - Should result in: PASS (no warnings)")
