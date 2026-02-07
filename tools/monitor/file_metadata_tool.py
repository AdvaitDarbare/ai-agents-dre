"""
Tool A: FileMetadataTool
Checks file existence, size, creation time, and calculates MD5 hash.
"""
import os
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any


class FileMetadataTool:
    """Sanity check for file freshness and duplication."""
    
    def __init__(self, freshness_hours: int = 24):
        self.freshness_hours = freshness_hours
        self.seen_hashes = set()  # In production, this would be in SQLite
    
    def run(self, file_path: str, override_freshness_hours: int = None) -> Dict[str, Any]:
        """
        Check if file is fresh and not a duplicate.
        """
        # Determine freshness limit
        freshness_limit = override_freshness_hours if override_freshness_hours is not None else self.freshness_hours
        
        # Check existence
        if not os.path.exists(file_path):
            return {
                "status": "missing",
                "decision": "STOP",
                "reason": f"File not found: {file_path}"
            }
        
        # Get file stats
        stat = os.stat(file_path)
        size_mb = stat.st_size / (1024 * 1024)
        created_at = datetime.fromtimestamp(stat.st_ctime)
        
        # Calculate MD5 hash
        file_hash = self._calculate_hash(file_path)
        
        # Check for duplicates
        if file_hash in self.seen_hashes:
            return {
                "status": "duplicate",
                "size_mb": round(size_mb, 2),
                "hash": file_hash,
                "created_at": created_at.isoformat(),
                "decision": "STOP",
                "reason": "File hash already processed"
            }
        
        # Check freshness
        age_hours = (datetime.now() - created_at).total_seconds() / 3600
        is_fresh = age_hours <= freshness_limit
        
        status = "fresh" if is_fresh else "stale"
        decision = "CONTINUE" if is_fresh else "STOP"
        
        # Mark as seen
        if is_fresh:
            self.seen_hashes.add(file_hash)
        
        return {
            "status": status,
            "size_mb": round(size_mb, 2),
            "hash": file_hash,
            "created_at": created_at.isoformat(),
            "age_hours": round(age_hours, 2),
            "decision": decision,
            "reason": None if is_fresh else f"File is {round(age_hours, 1)}h old (max: {freshness_limit}h)"
        }
    
    def _calculate_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
