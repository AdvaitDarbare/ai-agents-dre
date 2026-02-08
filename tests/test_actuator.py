"""
Unit tests for File Actuator and Monitor Agent Integration.

These tests verify that files are physically moved based on validation results,
preventing re-scanning and protecting Apache Doris from bad data.
"""

import unittest
import tempfile
import shutil
import pandas as pd
from pathlib import Path
from src.agents.file_actuator import FileActuator
from src.agents.monitor_agent import MonitorAgent


class TestFileActuator(unittest.TestCase):
    """Test suite for the File Actuator."""
    
    @classmethod
    def setUpClass(cls):
        """Set up temporary directories for testing."""
        cls.temp_dir = Path(tempfile.mkdtemp())
        cls.landing_dir = cls.temp_dir / "landing"
        cls.staging_dir = cls.temp_dir / "staging"
        cls.quarantine_dir = cls.temp_dir / "quarantine"
        
        cls.landing_dir.mkdir(parents=True, exist_ok=True)
        
        cls.actuator = FileActuator(
            staging_dir=str(cls.staging_dir),
            quarantine_dir=str(cls.quarantine_dir)
        )
    
    @classmethod
    def tearDownClass(cls):
        """Clean up temporary directories."""
        shutil.rmtree(cls.temp_dir)
    
    def _create_test_file(self, filename: str) -> Path:
        """Helper to create a test CSV file."""
        filepath = self.landing_dir / filename
        df = pd.DataFrame({
            'transaction_id': ['TXN001'],
            'user_id': ['USER001'],
            'amount': [100.0],
            'timestamp': ['2024-01-01 10:00:00'],
            'status': ['completed']
        })
        df.to_csv(filepath, index=False)
        return filepath
    
    def test_move_to_staging(self):
        """Test Case: Move validated file to staging."""
        print("\n--- Test Results ---")
        
        # Create test file
        test_file = self._create_test_file("good_file.csv")
        self.assertTrue(test_file.exists())
        
        # Mock validation results
        validation_results = {
            'overall_status': 'PASS',
            'can_load': True
        }
        
        # Move to staging
        new_path = self.actuator.move_to_staging(str(test_file), validation_results)
        
        # Verify file moved
        self.assertFalse(test_file.exists(), "Original file should be removed")
        self.assertTrue(new_path.exists(), "File should exist in staging")
        self.assertEqual(new_path.name, "good_file.csv")
        
        # Verify metadata file created
        metadata_path = new_path.with_suffix('.csv.meta.json')
        self.assertTrue(metadata_path.exists(), "Metadata file should be created")
        
        print("✅ File successfully moved to staging with metadata")
    
    def test_move_to_quarantine(self):
        """Test Case: Move failed file to quarantine."""
        print("\n--- Test Results ---")
        
        # Create test file
        test_file = self._create_test_file("bad_file.csv")
        self.assertTrue(test_file.exists())
        
        # Mock validation results with errors
        validation_results = {
            'overall_status': 'FAIL',
            'can_load': False,
            'schema': {
                'passed': False,
                'errors': ['Missing column: user_id']
            },
            'profiling': {
                'passed': False,
                'errors': ['Volume check failed']
            }
        }
        
        # Move to quarantine
        new_path = self.actuator.move_to_quarantine(str(test_file), validation_results)
        
        # Verify file moved
        self.assertFalse(test_file.exists(), "Original file should be removed")
        self.assertTrue(new_path.exists(), "File should exist in quarantine")
        self.assertIn("bad_file", new_path.name)
        
        # Verify error report created
        error_report_path = new_path.with_suffix('.csv.error.json')
        self.assertTrue(error_report_path.exists(), "Error report should be created")
        
        print("✅ File successfully quarantined with error report")
    
    def test_get_staging_files(self):
        """Test Case: List files in staging."""
        print("\n--- Test Results ---")
        
        # Create and move a file to staging
        test_file = self._create_test_file("staging_test.csv")
        validation_results = {'overall_status': 'PASS', 'can_load': True}
        self.actuator.move_to_staging(str(test_file), validation_results)
        
        # Get staging files
        staging_files = self.actuator.get_staging_files()
        
        self.assertGreater(len(staging_files), 0, "Should have files in staging")
        print(f"✅ Found {len(staging_files)} file(s) in staging")
    
    def test_get_quarantined_files(self):
        """Test Case: List files in quarantine."""
        print("\n--- Test Results ---")
        
        # Create and move a file to quarantine
        test_file = self._create_test_file("quarantine_test.csv")
        validation_results = {
            'overall_status': 'FAIL',
            'can_load': False,
            'schema': {'passed': False, 'errors': ['Test error']}
        }
        self.actuator.move_to_quarantine(str(test_file), validation_results)
        
        # Get quarantined files
        quarantined_files = self.actuator.get_quarantined_files()
        
        self.assertGreater(len(quarantined_files), 0, "Should have files in quarantine")
        print(f"✅ Found {len(quarantined_files)} file(s) in quarantine")
    
    def test_quarantine_report_retrieval(self):
        """Test Case: Retrieve error report for quarantined file."""
        print("\n--- Test Results ---")
        
        # Create and quarantine a file
        test_file = self._create_test_file("report_test.csv")
        validation_results = {
            'overall_status': 'FAIL',
            'can_load': False,
            'schema': {'passed': False, 'errors': ['Critical error']},
            'profiling': {'passed': True, 'errors': []}
        }
        quarantined_path = self.actuator.move_to_quarantine(str(test_file), validation_results)
        
        # Retrieve report
        report = self.actuator.get_quarantine_report(quarantined_path)
        
        self.assertIsNotNone(report, "Report should exist")
        self.assertEqual(report['status'], 'QUARANTINED')
        self.assertIn('error_summary', report)
        self.assertGreater(report['error_summary']['total_errors'], 0)
        
        print(f"✅ Retrieved error report: {report['error_summary']['total_errors']} total errors")


if __name__ == '__main__':
    unittest.main(verbosity=2)
