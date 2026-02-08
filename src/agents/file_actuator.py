"""
File Actuator - Physical File Movement System

This module handles the physical movement of files based on data quality checks.
It's the "muscle" of the Monitor Agent - it takes action based on the Detector's findings.

File Movement Rules:
- ✅ PASS → data/staging/ (ready for Apache Doris)
- ❌ FAIL → data/quarantine/ (requires human intervention)

This prevents:
1. Re-scanning the same bad files repeatedly
2. Polluting the Agentic Brain's memory with duplicate errors
3. Bad data from accidentally entering the Data Warehouse
"""

import shutil
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional


class FileActuator:
    """
    Physical file movement system for data quality enforcement.
    
    This is the Actuator - it moves files based on validation results.
    Think of it as the "security guard's arm" that physically blocks or allows entry.
    """
    
    def __init__(self, 
                 staging_dir: str = "data/staging",
                 quarantine_dir: str = "data/quarantine"):
        """
        Initialize the File Actuator.
        
        Args:
            staging_dir: Directory for validated, ready-to-load data
            quarantine_dir: Directory for failed data requiring review
        """
        self.staging_dir = Path(staging_dir)
        self.quarantine_dir = Path(quarantine_dir)
        
        # Ensure directories exist
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
    
    def move_to_staging(self, file_path: str, validation_results: Dict) -> Path:
        """
        Move a validated file to staging (VIP lounge).
        
        Files in staging are ready for Apache Doris to consume.
        
        Args:
            file_path: Path to the file to move
            validation_results: Results from Monitor Agent validation
            
        Returns:
            Path to the file in staging directory
            
        Example:
            >>> actuator = FileActuator()
            >>> new_path = actuator.move_to_staging('data/landing/transactions.csv', results)
            >>> print(new_path)
            data/staging/transactions.csv
        """
        source = Path(file_path)
        destination = self.staging_dir / source.name
        
        # Move the file
        shutil.move(str(source), str(destination))
        
        # Create metadata file (audit trail)
        metadata = {
            'original_file': str(source),
            'moved_to': str(destination),
            'timestamp': datetime.now().isoformat(),
            'status': 'APPROVED',
            'validation_results': validation_results
        }
        
        metadata_path = destination.with_suffix(destination.suffix + '.meta.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"✅ ACTUATOR: Moved to STAGING → {destination}")
        print(f"   📋 Metadata: {metadata_path}")
        
        return destination
    
    def move_to_quarantine(self, file_path: str, validation_results: Dict) -> Path:
        """
        Move a failed file to quarantine (jail).
        
        Files in quarantine require human intervention before they can proceed.
        
        Args:
            file_path: Path to the file to quarantine
            validation_results: Results from Monitor Agent validation (with errors)
            
        Returns:
            Path to the file in quarantine directory
            
        Example:
            >>> actuator = FileActuator()
            >>> new_path = actuator.move_to_quarantine('data/landing/bad.csv', results)
            >>> print(new_path)
            data/quarantine/bad.csv
        """
        source = Path(file_path)
        
        # Add timestamp to filename to avoid overwrites in quarantine
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        quarantine_filename = f"{source.stem}_{timestamp}{source.suffix}"
        destination = self.quarantine_dir / quarantine_filename
        
        # Move the file
        shutil.move(str(source), str(destination))
        
        # Create detailed error report
        error_report = {
            'original_file': str(source),
            'quarantined_to': str(destination),
            'timestamp': datetime.now().isoformat(),
            'status': 'QUARANTINED',
            'validation_results': validation_results,
            'error_summary': self._create_error_summary(validation_results)
        }
        
        # Save error report
        report_path = destination.with_suffix(destination.suffix + '.error.json')
        with open(report_path, 'w') as f:
            json.dump(error_report, f, indent=2)
        
        print(f"❌ ACTUATOR: Moved to QUARANTINE → {destination}")
        print(f"   📋 Error Report: {report_path}")
        
        return destination
    
    def _create_error_summary(self, validation_results: Dict) -> Dict:
        """
        Create a human-readable error summary.
        
        Args:
            validation_results: Full validation results from Monitor Agent
            
        Returns:
            Dictionary with categorized errors
        """
        summary = {
            'total_errors': 0,
            'timeliness_issues': [],
            'schema_issues': [],
            'profiling_issues': []
        }
        
        # Timeliness
        if not validation_results.get('timeliness', {}).get('passed', True):
            summary['timeliness_issues'].append(
                validation_results['timeliness'].get('error', 'Unknown timeliness error')
            )
            summary['total_errors'] += 1
        
        # Schema
        schema_errors = validation_results.get('schema', {}).get('errors', [])
        summary['schema_issues'] = schema_errors
        summary['total_errors'] += len(schema_errors)
        
        # Profiling
        profiling_errors = validation_results.get('profiling', {}).get('errors', [])
        summary['profiling_issues'] = profiling_errors
        summary['total_errors'] += len(profiling_errors)
        
        return summary
    
    def get_staging_files(self) -> list:
        """
        List all files currently in staging.
        
        Returns:
            List of file paths in staging directory
        """
        return [f for f in self.staging_dir.glob('*.csv')]
    
    def get_quarantined_files(self) -> list:
        """
        List all files currently in quarantine.
        
        Returns:
            List of file paths in quarantine directory
        """
        return [f for f in self.quarantine_dir.glob('*.csv')]
    
    def get_quarantine_report(self, quarantined_file: Path) -> Optional[Dict]:
        """
        Retrieve the error report for a quarantined file.
        
        Args:
            quarantined_file: Path to quarantined CSV file
            
        Returns:
            Error report dictionary, or None if report doesn't exist
        """
        report_path = quarantined_file.with_suffix(quarantined_file.suffix + '.error.json')
        
        if report_path.exists():
            with open(report_path, 'r') as f:
                return json.load(f)
        
        return None


if __name__ == '__main__':
    # Example usage
    actuator = FileActuator()
    
    print("\n" + "=" * 80)
    print("📦 FILE ACTUATOR - Physical File Movement System")
    print("=" * 80)
    print("\nDirectories:")
    print(f"  ✅ Staging:    {actuator.staging_dir}")
    print(f"  ❌ Quarantine: {actuator.quarantine_dir}")
    print("\nThe Actuator is ready to move files based on validation results!")
    print("=" * 80)
