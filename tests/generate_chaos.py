"""
Generate Chaos - Data Quality Test File Generator

This script generates CSV files with various data quality issues to test
the Schema Validator against the ODCS contract.

Outputs files to: data/landing/
"""

import pandas as pd
from pathlib import Path
import random
from datetime import datetime, timedelta


class ChaosGenerator:
    """Generate test CSV files with intentional data quality issues."""
    
    def __init__(self, output_dir: str = "data/landing"):
        """
        Initialize the chaos generator.
        
        Args:
            output_dir: Directory where CSV files will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_perfect_transactions(self, filename: str = "transactions_perfect.csv", num_rows: int = 100):
        """
        Generate a perfect transactions file matching the ODCS contract.
        
        Args:
            filename: Name of the output CSV file
            num_rows: Number of transaction rows to generate
        """
        print(f"\n🎯 Generating PERFECT transactions file...")
        
        data = {
            'transaction_id': [f'TXN{i:05d}' for i in range(1, num_rows + 1)],
            'user_id': [f'USER{random.randint(1, 1000):04d}' for _ in range(num_rows)],
            'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],
            'timestamp': [(datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S') 
                         for _ in range(num_rows)],
            'status': [random.choice(['completed', 'pending', 'failed', 'refunded']) for _ in range(num_rows)]
        }
        
        df = pd.DataFrame(data)
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=False)
        
        print(f"✅ Created: {filepath}")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        
    def generate_missing_columns(self, filename: str = "transactions_missing_columns.csv", num_rows: int = 100):
        """
        Generate transactions file with MISSING required columns.
        
        Missing: timestamp, status
        
        Args:
            filename: Name of the output CSV file
            num_rows: Number of transaction rows to generate
        """
        print(f"\n❌ Generating transactions with MISSING COLUMNS...")
        
        # Only include transaction_id, user_id, amount (missing timestamp and status)
        data = {
            'transaction_id': [f'TXN{i:05d}' for i in range(1, num_rows + 1)],
            'user_id': [f'USER{random.randint(1, 1000):04d}' for _ in range(num_rows)],
            'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],
        }
        
        df = pd.DataFrame(data)
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=False)
        
        print(f"✅ Created: {filepath}")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        print(f"   ⚠️  Missing: timestamp, status")
        
    def generate_schema_drift(self, filename: str = "transactions_drift.csv", num_rows: int = 100):
        """
        Generate transactions file with SCHEMA DRIFT (extra columns).
        
        Extra columns: loyalty_score, credit_score, region
        
        Args:
            filename: Name of the output CSV file
            num_rows: Number of transaction rows to generate
        """
        print(f"\n⚠️  Generating transactions with SCHEMA DRIFT...")
        
        data = {
            'transaction_id': [f'TXN{i:05d}' for i in range(1, num_rows + 1)],
            'user_id': [f'USER{random.randint(1, 1000):04d}' for _ in range(num_rows)],
            'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],
            'timestamp': [(datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S') 
                         for _ in range(num_rows)],
            'status': [random.choice(['completed', 'pending', 'failed', 'refunded']) for _ in range(num_rows)],
            # Extra columns not in contract
            'loyalty_score': [random.randint(500, 1000) for _ in range(num_rows)],
            'credit_score': [random.randint(600, 850) for _ in range(num_rows)],
            'region': [random.choice(['US-East', 'US-West', 'EU', 'APAC']) for _ in range(num_rows)]
        }
        
        df = pd.DataFrame(data)
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=False)
        
        print(f"✅ Created: {filepath}")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        print(f"   ⚠️  Unexpected columns: loyalty_score, credit_score, region")
        
    def generate_mixed_chaos(self, filename: str = "transactions_mixed_chaos.csv", num_rows: int = 100):
        """
        Generate transactions file with MIXED issues (missing columns + drift).
        
        Missing: user_id, timestamp
        Extra: payment_method, device_type
        
        Args:
            filename: Name of the output CSV file
            num_rows: Number of transaction rows to generate
        """
        print(f"\n💥 Generating transactions with MIXED CHAOS...")
        
        # Missing user_id and timestamp, but adding new columns
        data = {
            'transaction_id': [f'TXN{i:05d}' for i in range(1, num_rows + 1)],
            'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],
            'status': [random.choice(['completed', 'pending', 'failed', 'refunded']) for _ in range(num_rows)],
            # Extra columns
            'payment_method': [random.choice(['credit_card', 'debit_card', 'paypal', 'crypto']) for _ in range(num_rows)],
            'device_type': [random.choice(['mobile', 'desktop', 'tablet']) for _ in range(num_rows)]
        }
        
        df = pd.DataFrame(data)
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=False)
        
        print(f"✅ Created: {filepath}")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        print(f"   ❌ Missing: user_id, timestamp")
        print(f"   ⚠️  Unexpected: payment_method, device_type")
        
    def generate_empty_file(self, filename: str = "transactions_empty.csv"):
        """
        Generate an empty transactions file (headers only).
        
        Args:
            filename: Name of the output CSV file
        """
        print(f"\n📭 Generating EMPTY transactions file...")
        
        # Create empty dataframe with correct headers
        df = pd.DataFrame(columns=['transaction_id', 'user_id', 'amount', 'timestamp', 'status'])
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=False)
        
        print(f"✅ Created: {filepath}")
        print(f"   Rows: 0 (headers only)")
        print(f"   Columns: {list(df.columns)}")
        
    def generate_all(self, num_rows: int = 100):
        """
        Generate all chaos test files.
        
        Args:
            num_rows: Number of rows for each file (except empty)
        """
        print("=" * 70)
        print("🌪️  CHAOS GENERATOR - Creating Test Files")
        print("=" * 70)
        
        self.generate_perfect_transactions(num_rows=num_rows)
        self.generate_missing_columns(num_rows=num_rows)
        self.generate_schema_drift(num_rows=num_rows)
        self.generate_mixed_chaos(num_rows=num_rows)
        self.generate_empty_file()
        self.generate_future_transactions(num_rows=15)
        
        print("\n" + "=" * 70)
        print("✅ CHAOS GENERATION COMPLETE!")
        print(f"📂 Files saved to: {self.output_dir.absolute()}")
        print("=" * 70)
    
    def generate_future_transactions(self, filename: str = "transactions_future.csv", num_rows: int = 15):
        """
        Generate transactions with future timestamps (violates consistency rule).
        
        Args:
            filename: Name of the output CSV file
            num_rows: Number of transaction rows to generate
        """
        print(f"\n⏰ Generating transactions with FUTURE timestamps...")
        
        data = {
            'transaction_id': [f'TXN{i:05d}' for i in range(1, num_rows + 1)],
            'user_id': [f'USER{random.randint(1, 1000):04d}' for _ in range(num_rows)],
            'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],
            'timestamp': [(datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S') 
                         for _ in range(num_rows)],
            'status': [random.choice(['completed', 'pending', 'failed', 'refunded']) for _ in range(num_rows)]
        }
        
        df = pd.DataFrame(data)
        
        # Add a FUTURE transaction (violates "timestamp <= now()" rule)
        df.loc[5, 'timestamp'] = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=False)
        
        print(f"   💾 Saved to: {filepath}")
        print(f"   ✅ Generated {len(df)} rows with 1 future timestamp")
        
        return filepath


if __name__ == '__main__':
    # Generate all test files
    generator = ChaosGenerator()
    generator.generate_all(num_rows=100)
    
    print("\n💡 Next Steps:")
    print("   1. Run the schema validator against these files")
    print("   2. Check that it correctly identifies all issues")
    print("   3. Review the formatted validation reports")
