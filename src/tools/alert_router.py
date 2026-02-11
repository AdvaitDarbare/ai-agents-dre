"""
Alert Router Tool
-----------------
Routes data quality alerts to the appropriate channels (Slack, PagerDuty, Email)
based on severity and dataset criticality, as defined in alerts.yaml.
"""
import yaml
import os
import json
from pathlib import Path
from typing import Dict, Any, List

class AlertRouter:
    def __init__(self, config_path: str = "config/alerts.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            print(f"⚠️ Alert config not found at {self.config_path}")
            return {}
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"⚠️ Failed to load alert config: {e}")
            return {}

    def send_alert(self, verdict: Dict[str, Any], dataset_metadata: Dict[str, Any] = None):
        """
        Route the alert based on verdict status and dataset criticality.
        """
        if not self.config:
            return

        status = verdict.get("status", "UNKNOWN")
        dataset_name = verdict.get("dataset", "Unknown")
        
        # Don't alert on PASS (unless configured otherwise, but usually silent)
        if status == "PASSED":
            return

        # Get criticality
        criticality = "UNKNOWN"
        owner = "Unknown"
        if dataset_metadata:
            criticality = dataset_metadata.get("criticality", "UNKNOWN")
            owner = dataset_metadata.get("owner", "Unknown")
            
        # Determine channels from routing rules
        routing = self.config.get("routing", {})
        rule = routing.get(status, routing.get("DEFAULT", {}))
        
        target_channels = []
        
        # Check criticality filter
        required_crit = rule.get("required_criticality", [])
        if required_crit and criticality not in required_crit:
            # Skip if dataset isn't critical enough for this severity
            print(f"🔕 Alert suppressed for '{dataset_name}' ({status}): Criticality '{criticality}' not in {required_crit}")
            return
            
        target_channels = rule.get("channels", [])
        
        # Dispatch to channels
        for channel_name in target_channels:
            channel_conf = self.config.get("channels", {}).get(channel_name)
            if channel_conf:
                self._dispatch(channel_name, channel_conf, verdict, dataset_name, criticality, owner)

    def _dispatch(self, channel_name: str, channel_conf: Dict, verdict: Dict, dataset_name: str, criticality: str, owner: str):
        """
        Simulate sending the alert.
        """
        channel_type = channel_conf.get("type")
        status = verdict.get("status")
        reason = verdict.get("reason")
        
        icon = "🚨" if status == "BLOCKED" else "⚠️"
        
        if channel_type == "slack":
            # Simulate Slack Block Kit
            print(f"\n📨 [Alert Sent] To: Slack (#{channel_name}) | Owner: {owner}")
            print(f"   {icon} *{status}: {dataset_name}*")
            print(f"   > Criticality: {criticality}")
            print(f"   > Reason: {reason}")
            if "Root Cause:" in reason:
                print(f"   > 🕵️ Root Cause Detected!")
                
        elif channel_type == "pagerduty":
            # Simulate PagerDuty Incident
            print(f"\nQRY [Alert Sent] To: PagerDuty (Key: *******) | Owner: {owner}")
            print(f"   🔥 INCIDENT TRIGGERED: {dataset_name} is {status}")
            print(f"   Severity: critical")
            print(f"   Summary: {reason}")

