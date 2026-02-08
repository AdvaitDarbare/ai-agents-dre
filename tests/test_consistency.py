"""
Unit tests for Consistency (Logic) checks in DataProfiler.

These tests verify that custom SQL business logic rules are correctly
enforced against transaction data.
"""

import unittest
import pandas as pd
from pathlib import Path
import tempfile
import shutil
from datetime import datetime, timedelta
from src.tools.data_profiler import DataProfiler


class TestConsistencyChecks(unittest.TestCase):
    """Test suite for SQL-based consistency checks."""
    
    @classmethod
    def setUpClass(cls):
        """Set up temporary directory for test CSV files."""
        cls.temp_dir = Path(tempfile.mkdtemp())
        cls.contracts_path = "config/expectations"
    
    @classmethod
    def tearDownClass(cls):
        """Clean up temporary directory."""
        shutil.rmtree(cls.temp_dir)
    
    def _create_csv(self, filename: str, num_rows: int = 15, 
                    future_timestamp: bool = False,
                    high_value_incomplete: bool = False) -> Path:
        """
        Helper method to create test CSV files.
        
        Args:
            filename: Name of the CSV file
            num_rows: Number of rows to generate
            future_timestamp: If True, add a future timestamp (violates rule)
            high_value_incomplete: If True, add high-value transaction without completed status
            
        Returns:
            Path to the created CSV file
        """
        data = {
            'transaction_id': [f'TXN{i:05d}' for i in range(1, num_rows + 1)],
            'user_id': [f'USER{i:04d}' for i in range(1, num_rows + 1)],
            'amount': [100.0 for _ in range(num_rows)],
            'timestamp': [(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S') 
                         for _ in range(num_rows)],
            'status': ['completed' for _ in range(num_rows)]
        }
        
        df = pd.DataFrame(data)
        
        # Introduce violations if requested
        if future_timestamp:
            df.loc[5, 'timestamp'] = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        
        if high_value_incomplete:
            df.loc[10, 'amount'] = 6000.0  # Above 5000 threshold
            df.loc[10, 'status'] = 'pending'  # Not completed
        
        filepath = self.temp_dir / filename
        df.to_csv(filepath, index=False)
        
        return filepath
    
    def test_happy_path_consistency(self):
        """Test Case: Happy Path - All consistency rules pass."""
        print("\n--- Test Results ---")
        
        csv_path = self._create_csv("consistency_pass.csv", num_rows=15)
        profiler = DataProfiler(contracts_path=self.contracts_path)
        errors = profiler.analyze(str(csv_path), "transactions")
        
        # Should have no consistency errors
        consistency_errors = [e for e in errors if "CONSISTENCY" in e]
        
        self.assertEqual(len(consistency_errors), 0, 
                        f"Expected no consistency errors, but got: {consistency_errors}")
        
        if not consistency_errors:
            print("✅ All consistency checks passed!")
    
    def test_future_timestamp_violation(self):
        """Test Case: Future Timestamp - Violates 'timestamp <= now()' rule."""
        print("\n--- Test Results ---")
        
        csv_path = self._create_csv("future_timestamp.csv", num_rows=15, future_timestamp=True)
        profiler = DataProfiler(contracts_path=self.contracts_path)
        errors = profiler.analyze(str(csv_path), "transactions")
        
        # Should have a consistency error for future transactions
        self.assertTrue(any("CONSISTENCY" in e and "No Future Transactions" in e for e in errors),
                       f"Expected 'No Future Transactions' error, but got: {errors}")
        
        # Find and print the specific error
        for error in errors:
            if "CONSISTENCY" in error and "No Future Transactions" in error:
                print(error)
                # Verify it says "1 rows"
                self.assertIn("1 rows", error)
    
    def test_high_value_incomplete_violation(self):
        """Test Case: High Value Incomplete - Violates high value flag check."""
        print("\n--- Test Results ---")
        
        csv_path = self._create_csv("high_value_incomplete.csv", 
                                   num_rows=15, 
                                   high_value_incomplete=True)
        profiler = DataProfiler(contracts_path=self.contracts_path)
        errors = profiler.analyze(str(csv_path), "transactions")
        
        # Should have a consistency error for high value flag check
        self.assertTrue(any("CONSISTENCY" in e and "High Value Flag Check" in e for e in errors),
                       f"Expected 'High Value Flag Check' error, but got: {errors}")
        
        # Find and print the specific error
        for error in errors:
            if "CONSISTENCY" in error and "High Value Flag Check" in error:
                print(error)
                # Verify it says "1 rows"
                self.assertIn("1 rows", error)
    
    def test_multiple_consistency_violations(self):
        """Test Case: Multiple Violations - Both consistency rules fail."""
        print("\n--- Test Results ---")
        
        csv_path = self._create_csv("multiple_violations.csv", 
                                   num_rows=15,
                                   future_timestamp=True,
                                   high_value_incomplete=True)
        profiler = DataProfiler(contracts_path=self.contracts_path)
        errors = profiler.analyze(str(csv_path), "transactions")
        
        # Should have both consistency errors
        consistency_errors = [e for e in errors if "CONSISTENCY" in e]
        
        self.assertEqual(len(consistency_errors), 2,
                        f"Expected 2 consistency errors, but got {len(consistency_errors)}: {consistency_errors}")
        
        # Check for both specific errors
        self.assertTrue(any("No Future Transactions" in e for e in consistency_errors))
        self.assertTrue(any("High Value Flag Check" in e for e in consistency_errors))
        
        print("Found multiple violations:")
        for error in consistency_errors:
            print(f"  {error}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
