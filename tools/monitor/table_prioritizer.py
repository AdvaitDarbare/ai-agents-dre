Table Prioritizer Tool
Prioritizes tables by downstream impact using lineage and usage metrics.
"""
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional


class TablePrioritizer:
    """
    Intelligent scanning prioritization based on:
    - Downstream lineage (how many tables/dashboards depend on this)
    - Query volume (how often is this table accessed)
    - Certification status (is this a "gold" trusted dataset)
    - Freshness requirements (real-time vs batch)
    
    Key principle: Quality issues on high-impact tables are flagged first.
    """
    
    def __init__(self, db_path: str = "data/metrics_history.db"):
        self.db_path = db_path
        self._init_tables()
    
    def _init_tables(self):
        """Create tables for priority metadata."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Table metadata and priority scores
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS table_metadata (
                table_name TEXT PRIMARY KEY,
                certification TEXT DEFAULT 'none',  -- 'gold', 'silver', 'bronze', 'none'
                downstream_count INTEGER DEFAULT 0,
                query_count_7d INTEGER DEFAULT 0,
                last_successful_run TIMESTAMP,
                freshness_sla_hours INTEGER DEFAULT 24,
                priority_score REAL DEFAULT 0,
                tags TEXT,  -- JSON array of tags
                owner TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Lineage tracking (simplified)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS table_lineage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_table TEXT NOT NULL,
                target_table TEXT NOT NULL,
                relationship_type TEXT DEFAULT 'derives',  -- 'derives', 'joins', 'aggregates'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
    
    def register_table(self, table_name: str, metadata: Dict[str, Any]):
        """
        Register a table with its metadata.
        
        metadata can include:
        - certification: 'gold', 'silver', 'bronze', 'none'
        - freshness_sla_hours: expected update frequency
        - owner: team or person responsible
        - tags: list of tags
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        import json
        tags_json = json.dumps(metadata.get('tags', []))
        
        cursor.execute("""
            INSERT OR REPLACE INTO table_metadata 
            (table_name, certification, freshness_sla_hours, owner, tags, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            table_name,
            metadata.get('certification', 'none'),
            metadata.get('freshness_sla_hours', 24),
            metadata.get('owner', 'unknown'),
            tags_json,
            datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
        
        # Recalculate priority
        self._update_priority_score(table_name)
    
    def add_lineage(self, source_table: str, target_table: str, 
                    relationship_type: str = 'derives'):
        """Record that target_table depends on source_table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO table_lineage (source_table, target_table, relationship_type)
            VALUES (?, ?, ?)
        """, (source_table, target_table, relationship_type))
        
        conn.commit()
        conn.close()
        
        # Update downstream count
        self._update_downstream_count(source_table)
    
    def _update_downstream_count(self, table_name: str):
        """Count how many tables depend on this one."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) FROM table_lineage WHERE source_table = ?
        """, (table_name,))
        
        count = cursor.fetchone()[0]
        
        cursor.execute("""
            UPDATE table_metadata SET downstream_count = ?, updated_at = ?
            WHERE table_name = ?
        """, (count, datetime.now().isoformat(), table_name))
        
        conn.commit()
        conn.close()
        
        self._update_priority_score(table_name)
    
    def record_query(self, table_name: str):
        """Record that this table was queried (for usage tracking)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE table_metadata 
            SET query_count_7d = query_count_7d + 1, updated_at = ?
            WHERE table_name = ?
        """, (datetime.now().isoformat(), table_name))
        
        conn.commit()
        conn.close()
    
    def _update_priority_score(self, table_name: str):
        """
        Calculate priority score based on multiple factors.
        
        Score formula:
        - Certification: gold=100, silver=50, bronze=25, none=0
        - Downstream: each dependent table adds 10 points
        - Query volume: log(queries + 1) * 5
        - Freshness SLA: tighter SLA = higher priority
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT certification, downstream_count, query_count_7d, freshness_sla_hours
            FROM table_metadata WHERE table_name = ?
        """, (table_name,))
        
        row = cursor.fetchone()
        if not row:
            conn.close()
            return
        
        cert, downstream, queries, sla = row
        
        # Calculate score components
        cert_scores = {'gold': 100, 'silver': 50, 'bronze': 25, 'none': 0}
        cert_score = cert_scores.get(cert, 0)
        
        downstream_score = downstream * 10
        
        import math
        query_score = math.log(queries + 1) * 5
        
        sla_score = max(0, (24 - sla) * 2)  # Tighter SLA = higher score
        
        total_score = cert_score + downstream_score + query_score + sla_score
        
        cursor.execute("""
            UPDATE table_metadata SET priority_score = ?, updated_at = ?
            WHERE table_name = ?
        """, (total_score, datetime.now().isoformat(), table_name))
        
        conn.commit()
        conn.close()
    
    def get_priority(self, table_name: str) -> Dict[str, Any]:
        """Get priority information for a table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT certification, downstream_count, query_count_7d, 
                   freshness_sla_hours, priority_score, owner
            FROM table_metadata WHERE table_name = ?
        """, (table_name,))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return {
                "table_name": table_name,
                "priority_score": 0,
                "priority_tier": "UNKNOWN",
                "context": "Table not registered in metadata"
            }
        
        cert, downstream, queries, sla, score, owner = row
        
        # Determine tier
        if score >= 100:
            tier = "CRITICAL"
            scan_frequency = "Every run"
        elif score >= 50:
            tier = "HIGH"
            scan_frequency = "Hourly"
        elif score >= 25:
            tier = "MEDIUM"
            scan_frequency = "Daily"
        else:
            tier = "LOW"
            scan_frequency = "Weekly"
        
        return {
            "table_name": table_name,
            "priority_score": round(score, 1),
            "priority_tier": tier,
            "recommended_scan_frequency": scan_frequency,
            "certification": cert,
            "downstream_tables": downstream,
            "query_count_7d": queries,
            "freshness_sla_hours": sla,
            "owner": owner
        }
    
    def get_downstream_impact(self, table_name: str) -> Dict[str, Any]:
        """
        Get the downstream impact if this table has issues.
        Used for root cause analysis.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get direct dependents
        cursor.execute("""
            SELECT target_table, relationship_type 
            FROM table_lineage WHERE source_table = ?
        """, (table_name,))
        
        direct_dependents = [
            {"table": row[0], "relationship": row[1]} 
            for row in cursor.fetchall()
        ]
        
        # Recursive count (simplified - just 2 levels)
        affected_count = len(direct_dependents)
        for dep in direct_dependents:
            cursor.execute("""
                SELECT COUNT(*) FROM table_lineage WHERE source_table = ?
            """, (dep['table'],))
            affected_count += cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "source_table": table_name,
            "direct_dependents": direct_dependents,
            "total_affected_tables": affected_count,
            "impact_severity": "CRITICAL" if affected_count >= 5 else 
                              "HIGH" if affected_count >= 2 else "NORMAL"
        }
