"""
Unit tests for Timeliness Check in Monitor Agent.

These tests verify that file age validation works correctly and
identifies stale files that should not be processed.
"""

import unittest
import tempfile
import shutil
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from src.agents.monitor_agent import MonitorAgent


class TestTimelinessCheck(unittest.TestCase):
    """Test suite for timeliness (file age) checks."""
    
    @classmethod
    def setUpClass(cls):
        """Set up temporary directory for test files."""
        cls.temp_dir = Path(tempfile.mkdtemp())
        cls.agent = MonitorAgent()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up temporary directory."""
        shutil.rmtree(cls.temp_dir)
    
    def _create_file(self, filename: str, age_hours: float = 0) -> Path:
        """
        Helper method to create a test file with a specific age.
        
        Args:
            filename: Name of the file
            age_hours: How many hours old the file should appear to be
            
        Returns:
            Path to the created file
        """
        filepath = self.temp_dir / filename
        
        # Create the file
        filepath.write_text("test data")
        
        # Modify the file's timestamp to make it appear older
        if age_hours > 0:
            current_time = time.time()
            old_time = current_time - (age_hours * 3600)  # Convert hours to seconds
            os.utime(filepath, (old_time, old_time))
        
        return filepath
    
    def test_fresh_file_passes(self):
        """Test Case: Fresh File - File is recent and should pass."""
        print("\n--- Test Results ---")
        
        # Create a file that's 1 hour old
        file_path = self._create_file("fresh_file.csv", age_hours=1)
        
        # Check with 24-hour threshold
        is_fresh, error = self.agent.check_timeliness(str(file_path), max_age_hours=24)
        
        self.assertTrue(is_fresh, f"Expected file to be fresh, but got: {error}")
        self.assertIsNone(error)
        
        print("✅ Fresh file passed timeliness check")
    
    def test_stale_file_fails(self):
        """Test Case: Stale File - File is too old and should fail."""
        print("\n--- Test Results ---")
        
        # Create a file that's 48 hours old
        file_path = self._create_file("stale_file.csv", age_hours=48)
        
        # Check with 24-hour threshold
        is_fresh, error = self.agent.check_timeliness(str(file_path), max_age_hours=24)
        
        self.assertFalse(is_fresh, "Expected file to be stale")
        self.assertIsNotNone(error)
        self.assertIn("TIMELINESS", error)
        self.assertIn("48", error)
        self.assertIn("24", error)
        
        print(error)
    
    def test_file_exactly_at_threshold(self):
        """Test Case: Boundary - File exactly at the age threshold."""
        print("\n--- Test Results ---")
        
        # Create a file that's exactly 24 hours old
        file_path = self._create_file("boundary_file.csv", age_hours=24.0)
        
        # Check with 24-hour threshold
        is_fresh, error = self.agent.check_timeliness(str(file_path), max_age_hours=24)
        
        # At exactly the threshold (24.0 hours), it should fail (using >= comparison)
        self.assertFalse(is_fresh, "Expected file at threshold to be stale")
        self.assertIn("24.0 hours old", error)
        
        print(error)
    
    def test_file_just_over_threshold(self):
        """Test Case: Just Over - File slightly exceeds threshold."""
        print("\n--- Test Results ---")
        
        # Create a file that's 24.5 hours old
        file_path = self._create_file("just_over.csv", age_hours=24.5)
        
        # Check with 24-hour threshold
        is_fresh, error = self.agent.check_timeliness(str(file_path), max_age_hours=24)
        
        self.assertFalse(is_fresh, "Expected file to be stale")
        self.assertIn("24.5", error)
        
        print(error)
    
    def test_file_not_found(self):
        """Test Case: Missing File - File doesn't exist."""
        print("\n--- Test Results ---")
        
        # Check a non-existent file
        is_fresh, error = self.agent.check_timeliness("nonexistent_file.csv", max_age_hours=24)
        
        self.assertFalse(is_fresh, "Expected check to fail for missing file")
        self.assertIn("File not found", error)
        
        print(error)
    
    def test_custom_threshold(self):
        """Test Case: Custom Threshold - Use different age thresholds."""
        print("\n--- Test Results ---")
        
        # Create a file that's 2 hours old
        file_path = self._create_file("custom_threshold.csv", age_hours=2)
        
        # Should pass with 24-hour threshold
        is_fresh_24h, _ = self.agent.check_timeliness(str(file_path), max_age_hours=24)
        self.assertTrue(is_fresh_24h)
        print("✅ 2-hour old file passes 24-hour threshold")
        
        # Should fail with 1-hour threshold
        is_fresh_1h, error = self.agent.check_timeliness(str(file_path), max_age_hours=1)
        self.assertFalse(is_fresh_1h)
        print(f"❌ 2-hour old file fails 1-hour threshold")
        print(f"   {error}")
    
    def test_very_fresh_file(self):
        """Test Case: Just Created - File was just created (0 hours old)."""
        print("\n--- Test Results ---")
        
        # Create a file right now (0 hours old)
        file_path = self._create_file("just_created.csv", age_hours=0)
        
        # Should definitely pass
        is_fresh, error = self.agent.check_timeliness(str(file_path), max_age_hours=24)
        
        self.assertTrue(is_fresh, f"Expected just-created file to be fresh, but got: {error}")
        self.assertIsNone(error)
        
        print("✅ Just-created file is fresh")


if __name__ == '__main__':
    unittest.main(verbosity=2)
