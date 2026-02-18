"""
Alert Router Tool
-----------------
Routes data quality alerts to the appropriate channels (Slack, PagerDuty, Email)
based on severity and dataset criticality, as defined in alerts.yaml.
"""
import yaml
import os
import time
import hashlib
import json
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests
from src.utils.database import get_connection

class AlertRouter:
    def __init__(self, config_path: str = "config/alerts.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._recent_alert_cache: Dict[str, float] = {}
        
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

    @staticmethod
    def _shorten(value: Any, max_len: int = 700) -> str:
        text = str(value or "").strip()
        if len(text) <= max_len:
            return text
        return text[: max_len - 3].rstrip() + "..."

    @staticmethod
    def _status_icon(status: str) -> str:
        normalized = str(status or "").upper()
        if normalized == "BLOCKED":
            return ":rotating_light:"
        if normalized == "WARNING":
            return ":warning:"
        if normalized == "PASSED":
            return ":white_check_mark:"
        return ":information_source:"

    def _build_slack_payload(
        self,
        verdict: Dict[str, Any],
        dataset_name: str,
        criticality: str,
        owner: str,
        severity: str,
        incident_id: Optional[str] = None,
        open_incident_count: int = 0,
        impacted_consumers: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        status = str(verdict.get("status", "UNKNOWN")).upper()
        reason = self._shorten(verdict.get("reason", "No reason provided."), 400)
        llm_report = self._shorten(verdict.get("llm_advice", ""), 900)
        quality_score = verdict.get("profile", {}).get(
            "weighted_quality_score",
            verdict.get("profile", {}).get("overall_quality_score"),
        )
        anomaly_count = len(verdict.get("anomalies", []) or [])
        run_id = str(verdict.get("run_id") or "").strip()

        summary = (
            f"{self._status_icon(status)} *{status}* dataset=`{dataset_name}` "
            f"(criticality={criticality}, owner={owner}, severity={severity})"
        )
        metric_line = (
            f"quality_score={quality_score if quality_score is not None else 'N/A'} | "
            f"anomalies={anomaly_count} | "
            f"run_id={run_id or 'N/A'}"
        )

        blocks: List[Dict[str, Any]] = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": summary},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Reason:* {reason}"},
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": metric_line}],
            },
        ]

        actions = verdict.get("actions")
        if isinstance(actions, list) and actions:
            action_text = ", ".join(str(item) for item in actions[:6])
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Suggested Actions:* {action_text}"},
                }
            )

        consumers = impacted_consumers if isinstance(impacted_consumers, list) else []
        if incident_id or consumers:
            consumer_names = [
                str(item.get("name") or "")
                for item in consumers
                if isinstance(item, dict) and item.get("name")
            ]
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Incident:* `{incident_id or 'N/A'}` "
                            f"(open={int(open_incident_count)})\n"
                            f"*Lineage Impact:* "
                            f"{', '.join(consumer_names[:5]) if consumer_names else 'No managed consumers'}"
                        ),
                    },
                }
            )

        if llm_report:
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*LLM Report*\n{llm_report}"},
                }
            )

        return {
            "text": f"{status}: {dataset_name} - {reason}",
            "blocks": blocks,
        }

    def _send_slack(self, webhook_url: str, payload: Dict[str, Any], channel_name: str) -> None:
        timeout_seconds = float(os.getenv("ALERTS_SLACK_TIMEOUT_SECONDS", "5"))
        try:
            response = requests.post(webhook_url, json=payload, timeout=max(1.0, timeout_seconds))
            if response.status_code >= 400:
                print(
                    f"⚠️ Slack alert failed for channel '{channel_name}': "
                    f"{response.status_code} {response.text}"
                )
                return
            print(f"📨 [Alert Sent] To Slack channel '{channel_name}'")
        except Exception as exc:
            print(f"⚠️ Slack dispatch error for channel '{channel_name}': {exc}")

    @staticmethod
    def _alert_severity(status: str, criticality: str) -> str:
        status_norm = str(status or "").upper()
        crit_norm = str(criticality or "UNKNOWN").upper()
        if status_norm == "BLOCKED":
            return "critical" if crit_norm in {"HIGH", "CRITICAL"} else "high"
        if status_norm == "WARNING":
            return "high" if crit_norm in {"HIGH", "CRITICAL"} else "medium"
        return "info"

    @staticmethod
    def _fingerprint(dataset_name: str, status: str, reason: str) -> str:
        payload = f"{dataset_name}|{status}|{reason[:300]}".encode("utf-8", errors="ignore")
        return hashlib.sha1(payload).hexdigest()

    def _owner_channels(self, owner: str) -> List[str]:
        routes = self.config.get("owner_routes", {})
        if not isinstance(routes, dict):
            return []
        owner_norm = str(owner or "").strip()
        if not owner_norm:
            return []
        direct = routes.get(owner_norm)
        if isinstance(direct, list):
            return [str(ch) for ch in direct if str(ch).strip()]
        lower_map = {str(k).lower(): v for k, v in routes.items()}
        lower_hit = lower_map.get(owner_norm.lower())
        if isinstance(lower_hit, list):
            return [str(ch) for ch in lower_hit if str(ch).strip()]
        return []

    @staticmethod
    def _open_incident_context(dataset_name: str) -> Dict[str, Any]:
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT incident_id, status, severity, created_at
                        FROM incidents
                        WHERE dataset_name = %s
                          AND status IN ('OPEN', 'ACK')
                        ORDER BY updated_at DESC NULLS LAST, created_at DESC
                        LIMIT 1
                        """,
                        (dataset_name,),
                    )
                    latest = cur.fetchone()

                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM incidents
                        WHERE dataset_name = %s
                          AND status IN ('OPEN', 'ACK')
                        """,
                        (dataset_name,),
                    )
                    count_row = cur.fetchone()

            return {
                "incident_id": latest[0] if latest else None,
                "incident_status": latest[1] if latest else None,
                "incident_severity": latest[2] if latest else None,
                "incident_created_at": latest[3].isoformat() if latest and latest[3] else None,
                "open_incident_count": int((count_row or [0])[0] or 0),
            }
        except Exception:
            return {
                "incident_id": None,
                "incident_status": None,
                "incident_severity": None,
                "incident_created_at": None,
                "open_incident_count": 0,
            }

    def _should_suppress_duplicate(
        self,
        *,
        fingerprint: str,
        dataset_name: str,
        status: str,
        cooldown_minutes: int,
    ) -> bool:
        if cooldown_minutes <= 0:
            return False

        now_ts = time.time()
        cutoff_ts = now_ts - (cooldown_minutes * 60)

        cached = self._recent_alert_cache.get(fingerprint)
        if cached and cached >= cutoff_ts:
            return True

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM action_audit_log
                        WHERE action = 'alert_sent'
                          AND dataset_name = %s
                          AND metadata ->> 'status' = %s
                          AND metadata ->> 'fingerprint' = %s
                          AND timestamp >= NOW() - (%s * INTERVAL '1 minute')
                        LIMIT 1
                        """,
                        (dataset_name, status, fingerprint, cooldown_minutes),
                    )
                    return cur.fetchone() is not None
        except Exception:
            return False

    def _record_alert_delivery(
        self,
        *,
        dataset_name: str,
        status: str,
        criticality: str,
        channel: str,
        fingerprint: str,
        severity: str,
        incident_id: Optional[str] = None,
        open_incident_count: int = 0,
    ) -> None:
        self._recent_alert_cache[fingerprint] = time.time()
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO action_audit_log (id, actor, source, action, dataset_name, status, metadata)
                        VALUES (%s, 'system', 'alert_router', 'alert_sent', %s, %s, %s::jsonb)
                        """,
                        (
                            str(uuid.uuid4()),
                            dataset_name,
                            status,
                            json.dumps(
                                {
                                    "channel": channel,
                                    "status": status,
                                    "fingerprint": fingerprint,
                                    "criticality": criticality,
                                    "severity": severity,
                                    "incident_id": incident_id,
                                    "open_incident_count": int(open_incident_count),
                                }
                            ),
                        ),
                    )
        except Exception:
            return

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
        lineage_impact = dataset_metadata.get("lineage_impact", {}) if isinstance(dataset_metadata, dict) else {}
        impacted_consumers = (
            lineage_impact.get("impacted_consumers", [])
            if isinstance(lineage_impact, dict)
            else []
        )
        severity = self._alert_severity(status, criticality)
        reason = str(verdict.get("reason", "No reason provided."))
        fingerprint = self._fingerprint(dataset_name, status, reason)
        incident_ctx = self._open_incident_context(dataset_name)
            
        # Determine channels from routing rules
        routing = self.config.get("routing", {})
        rule = routing.get(status, routing.get("DEFAULT", {}))
        cooldown_minutes = int(rule.get("cooldown_minutes", os.getenv("ALERTS_COOLDOWN_MINUTES", "15")))
        
        target_channels = []
        
        # Check criticality filter
        required_crit = rule.get("required_criticality", [])
        if required_crit and criticality not in required_crit:
            # Skip if dataset isn't critical enough for this severity
            print(f"🔕 Alert suppressed for '{dataset_name}' ({status}): Criticality '{criticality}' not in {required_crit}")
            return
            
        target_channels = [str(ch) for ch in (rule.get("channels", []) or []) if str(ch).strip()]
        owner_channels = self._owner_channels(owner)
        for channel in owner_channels:
            if channel not in target_channels:
                target_channels.append(channel)

        noise_controls = self.config.get("noise_controls", {})
        if (
            isinstance(noise_controls, dict)
            and int(incident_ctx.get("open_incident_count") or 0) > 0
            and str(status).upper() == "WARNING"
        ):
            multiplier = max(1, int(noise_controls.get("open_incident_warning_cooldown_multiplier", 2)))
            cooldown_minutes *= multiplier

        if self._should_suppress_duplicate(
            fingerprint=fingerprint,
            dataset_name=dataset_name,
            status=status,
            cooldown_minutes=max(0, cooldown_minutes),
        ):
            print(
                f"🔕 Alert suppressed for '{dataset_name}' ({status}): duplicate within cooldown window ({cooldown_minutes}m)"
            )
            return
        
        # Dispatch to channels
        for channel_name in target_channels:
            channel_conf = self.config.get("channels", {}).get(channel_name)
            if channel_conf:
                self._dispatch(
                    channel_name,
                    channel_conf,
                    verdict,
                    dataset_name,
                    criticality,
                    owner,
                    severity,
                    fingerprint,
                    incident_id=str(incident_ctx.get("incident_id") or "") or None,
                    open_incident_count=int(incident_ctx.get("open_incident_count") or 0),
                    impacted_consumers=impacted_consumers if isinstance(impacted_consumers, list) else [],
                )

    def _dispatch(
        self,
        channel_name: str,
        channel_conf: Dict,
        verdict: Dict,
        dataset_name: str,
        criticality: str,
        owner: str,
        severity: str,
        fingerprint: str,
        incident_id: Optional[str] = None,
        open_incident_count: int = 0,
        impacted_consumers: Optional[List[Dict[str, Any]]] = None,
    ):
        """
        Dispatch alert to concrete integration channels.
        """
        channel_type = channel_conf.get("type")
        status = verdict.get("status")
        reason = verdict.get("reason")

        if channel_type == "slack":
            if os.getenv("ALERTS_SLACK_ENABLED", "1").strip() in {"0", "false", "False"}:
                print("🔕 Slack alerts disabled via ALERTS_SLACK_ENABLED")
                return

            webhook_env = str(channel_conf.get("webhook_env") or "SLACK_WEBHOOK_URL").strip()
            webhook_url = str(os.getenv(webhook_env, "")).strip() or str(channel_conf.get("webhook_url") or "").strip()
            if not webhook_url:
                print(
                    f"⚠️ Slack channel '{channel_name}' skipped: missing webhook URL "
                    f"(env '{webhook_env}')"
                )
                return

            payload = self._build_slack_payload(
                verdict=verdict,
                dataset_name=dataset_name,
                criticality=criticality,
                owner=owner,
                severity=severity,
                incident_id=incident_id,
                open_incident_count=open_incident_count,
                impacted_consumers=impacted_consumers,
            )
            self._send_slack(webhook_url, payload, channel_name)
            self._record_alert_delivery(
                dataset_name=dataset_name,
                status=status,
                criticality=criticality,
                channel=channel_name,
                fingerprint=fingerprint,
                severity=severity,
                incident_id=incident_id,
                open_incident_count=open_incident_count,
            )
                
        elif channel_type == "pagerduty":
            # Simulate PagerDuty Incident
            print(f"\nQRY [Alert Sent] To: PagerDuty (Key: *******) | Owner: {owner}")
            print(f"   🔥 INCIDENT TRIGGERED: {dataset_name} is {status}")
            print(f"   Severity: critical")
            print(f"   Summary: {reason}")
            self._record_alert_delivery(
                dataset_name=dataset_name,
                status=status,
                criticality=criticality,
                channel=channel_name,
                fingerprint=fingerprint,
                severity=severity,
                incident_id=incident_id,
                open_incident_count=open_incident_count,
            )
