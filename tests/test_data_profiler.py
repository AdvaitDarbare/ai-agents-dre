"""
Unittest suite for DataProfiler
Tests completeness (nulls), volume, and range validation against transactions.yaml contract.
"""

import unittest
import pandas as pd
from pathlib import Path
import tempfile
import shutil
from src.tools.data_profiler import DataProfiler


class TestDataProfiler(unittest.TestCase):
    """Test suite for DataProfiler with exact output matching."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests."""
        cls.temp_dir = Path(tempfile.mkdtemp())
        cls.profiler = DataProfiler()
        
        # Contract constraints from transactions.yaml:
        # - Min Rows: 10
        # - Amount: Min 0.0, Max 10,000.0
        # - Required Cols: transaction_id, user_id, amount, timestamp, status
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests."""
        if cls.temp_dir.exists():
            shutil.rmtree(cls.temp_dir)
    
    def _create_csv(self, filename: str, data: pd.DataFrame) -> Path:
        """Helper to create CSV files for testing."""
        filepath = self.temp_dir / filename
        data.to_csv(filepath, index=False)
        return filepath
    
    def test_happy_path(self):
        """
        Test Case: Happy Path
        
        Input: 15 rows (passes min_rows=10). amount = 100.0. No nulls.
        Expected: [] (Empty list)
        """
        print("\n--- Test Results ---")
        
        # Create valid data
        data = pd.DataFrame({
            'transaction_id': [f'TXN{i:03d}' for i in range(1, 16)],
            'user_id': [f'USER{i:03d}' for i in range(1, 16)],
            'amount': [100.0] * 15,
            'timestamp': ['2024-01-01 10:00:00'] * 15,
            'status': ['completed'] * 15
        })
        
        csv_path = self._create_csv("happy_path.csv", data)
        
        # Analyze
        errors = self.profiler.analyze(str(csv_path), 'transactions')
        
        # Print output
        if not errors:
            print("✅ All data quality checks passed!")
        else:
            for error in errors:
                print(error)
        
        # Assert
        self.assertEqual(errors, [], f"Expected no errors, but got: {errors}")
    
    def test_volume_failure(self):
        """
        Test Case: Volume Failure
        
        Input: 5 rows (fails min_rows=10)
        Expected: String containing "Volume" or "rows"
        """
        print("\n--- Test Results ---")
        
        # Create data with insufficient rows
        data = pd.DataFrame({
            'transaction_id': [f'TXN{i:03d}' for i in range(1, 6)],
            'user_id': [f'USER{i:03d}' for i in range(1, 6)],
            'amount': [100.0] * 5,
            'timestamp': ['2024-01-01 10:00:00'] * 5,
            'status': ['completed'] * 5
        })
        
        csv_path = self._create_csv("volume_failure.csv", data)
        
        # Analyze
        errors = self.profiler.analyze(str(csv_path), 'transactions')
        
        # Print output
        for error in errors:
            print(error)
        
        # Assert
        self.assertTrue(len(errors) > 0, "Expected at least one error")
        
        # Check that error contains "Volume" or "rows"
        volume_error_found = False
        for error in errors:
            if 'VOLUME' in error or 'rows' in error:
                volume_error_found = True
                break
        
        self.assertTrue(volume_error_found, 
                       f"Expected error containing 'Volume' or 'rows', got: {errors}")
    
    def test_null_failure(self):
        """
        Test Case: NULL Failure
        
        Input: 15 rows. user_id is None for 1 row.
        Expected: String containing "user_id" and "NULL"
        """
        print("\n--- Test Results ---")
        
        # Create data with NULL in required field
        data = pd.DataFrame({
            'transaction_id': [f'TXN{i:03d}' for i in range(1, 16)],
            'user_id': [f'USER{i:03d}' if i != 5 else None for i in range(1, 16)],
            'amount': [100.0] * 15,
            'timestamp': ['2024-01-01 10:00:00'] * 15,
            'status': ['completed'] * 15
        })
        
        csv_path = self._create_csv("null_failure.csv", data)
        
        # Analyze
        errors = self.profiler.analyze(str(csv_path), 'transactions')
        
        # Print output
        for error in errors:
            print(error)
        
        # Assert
        self.assertTrue(len(errors) > 0, "Expected at least one error")
        
        # Check that error contains "user_id" and "NULL"
        null_error_found = False
        for error in errors:
            if 'user_id' in error and 'NULL' in error:
                null_error_found = True
                break
        
        self.assertTrue(null_error_found,
                       f"Expected error containing 'user_id' and 'NULL', got: {errors}")
    
    def test_negative_amount_failure(self):
        """
        Test Case: Negative Amount Failure
        
        Input: 15 rows. One row has amount = -5.0.
        Expected: String containing "amount" and "min value"
        """
        print("\n--- Test Results ---")
        
        # Create data with negative amount
        amounts = [100.0] * 15
        amounts[5] = -5.0  # One negative amount
        
        data = pd.DataFrame({
            'transaction_id': [f'TXN{i:03d}' for i in range(1, 16)],
            'user_id': [f'USER{i:03d}' for i in range(1, 16)],
            'amount': amounts,
            'timestamp': ['2024-01-01 10:00:00'] * 15,
            'status': ['completed'] * 15
        })
        
        csv_path = self._create_csv("negative_amount.csv", data)
        
        # Analyze
        errors = self.profiler.analyze(str(csv_path), 'transactions')
        
        # Print output
        for error in errors:
            print(error)
        
        # Assert
        self.assertTrue(len(errors) > 0, "Expected at least one error")
        
        # Check that error contains "amount" and "min value"
        range_error_found = False
        for error in errors:
            if 'amount' in error and 'min value' in error:
                range_error_found = True
                break
        
        self.assertTrue(range_error_found,
                       f"Expected error containing 'amount' and 'min value', got: {errors}")
    
    def test_large_amount_failure(self):
        """
        Test Case: Large Amount Failure
        
        Input: 15 rows. One row has amount = 50000.0.
        Expected: String containing "amount" and "max value"
        """
        print("\n--- Test Results ---")
        
        # Create data with amount exceeding max
        amounts = [100.0] * 15
        amounts[10] = 50000.0  # One amount exceeding 10,000.0 limit
        
        data = pd.DataFrame({
            'transaction_id': [f'TXN{i:03d}' for i in range(1, 16)],
            'user_id': [f'USER{i:03d}' for i in range(1, 16)],
            'amount': amounts,
            'timestamp': ['2024-01-01 10:00:00'] * 15,
            'status': ['completed'] * 15
        })
        
        csv_path = self._create_csv("large_amount.csv", data)
        
        # Analyze
        errors = self.profiler.analyze(str(csv_path), 'transactions')
        
        # Print output
        for error in errors:
            print(error)
        
        # Assert
        self.assertTrue(len(errors) > 0, "Expected at least one error")
        
        # Check that error contains "amount" and "max value"
        range_error_found = False
        for error in errors:
            if 'amount' in error and 'max value' in error:
                range_error_found = True
                break
        
        self.assertTrue(range_error_found,
                       f"Expected error containing 'amount' and 'max value', got: {errors}")
    
    def test_multiple_violations(self):
        """
        Test Case: Multiple Violations (Comprehensive)
        
        Input: 8 rows (volume), NULL in transaction_id, amount = -10.0 and 20000.0
        Expected: Multiple errors covering volume, completeness, and range
        """
        print("\n--- Test Results ---")
        
        # Create data with multiple violations
        data = pd.DataFrame({
            'transaction_id': ['TXN001', None, 'TXN003', 'TXN004', 'TXN005', 'TXN006', 'TXN007', 'TXN008'],
            'user_id': [f'USER{i:03d}' for i in range(1, 9)],
            'amount': [-10.0, 100.0, 100.0, 20000.0, 100.0, 100.0, 100.0, 100.0],
            'timestamp': ['2024-01-01 10:00:00'] * 8,
            'status': ['completed'] * 8
        })
        
        csv_path = self._create_csv("multiple_violations.csv", data)
        
        # Analyze
        errors = self.profiler.analyze(str(csv_path), 'transactions')
        
        # Print output
        for error in errors:
            print(error)
        
        # Assert
        self.assertTrue(len(errors) >= 3, f"Expected at least 3 errors, got {len(errors)}")
        
        # Check for volume error
        has_volume_error = any('VOLUME' in err or 'rows' in err for err in errors)
        self.assertTrue(has_volume_error, "Expected volume error")
        
        # Check for NULL error
        has_null_error = any('NULL' in err and 'transaction_id' in err for err in errors)
        self.assertTrue(has_null_error, "Expected NULL error for transaction_id")
        
        # Check for range errors
        has_min_error = any('min value' in err and 'amount' in err for err in errors)
        has_max_error = any('max value' in err and 'amount' in err for err in errors)
        self.assertTrue(has_min_error or has_max_error, "Expected range error")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
