"""
System Health Check Tool
------------------------
Simulates checking the health of upstream services (APIs, Databases, etc.)
defined in lineage.yaml.
"""
import requests
import random
import time
from typing import Dict, Any

class SystemHealthCheck:
    def __init__(self):
        pass

    def check_upstream_health(self, upstream_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Checks the health of an upstream service.
        In a real scenario, this would ping the 'endpoint'.
        For this demo, we mock the response.
        """
        name = upstream_config.get("name", "Unknown Service").lower()
        endpoint = upstream_config.get("endpoint", "")
        
        # Simulate check duration
        latency = random.randint(10, 150)
        
        # Demo Logic: 
        # - "Payment Gateway" is usually UP
        # - "Auth Service" has a 20% chance of being DOWN (for demo)
        status = "UP"
        details = "Service reachable"
        
        if "auth" in name:
            # Randomly simulate flap
            if random.random() < 0.3:
                status = "DOWN"
                details = f"Connection timeout to {endpoint}"
        
        # Fail if endpoint contains 'fail' (for manual testing)
        if "fail" in endpoint:
             status = "DOWN"
             details = "simulated_failure"

        return {
            "name": upstream_config.get("name"),
            "status": status,
            "latency_ms": latency,
            "details": details,
            "timestamp": time.time()
        }
