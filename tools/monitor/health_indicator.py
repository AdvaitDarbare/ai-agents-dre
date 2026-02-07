"""
Health Indicator Tool (Hybrid: Math + LLM)
Provides consistent health signals across the platform.

Architecture:
- Math-based scoring: Deterministic, fast, safe (for critical decisions)
- LLM-based insights: Contextual, intelligent (for recommendations)

Inspired by Databricks' unified health indicator.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum
import os
import json


class HealthStatus(Enum):
    """Standardized health status across all tables."""
    HEALTHY = "HEALTHY"           # ✅ All checks pass, safe to use
    DEGRADED = "DEGRADED"         # ⚠️ Some issues, use with caution
    CRITICAL = "CRITICAL"         # ❌ Major issues, do not use
    UNKNOWN = "UNKNOWN"           # ❓ Not enough data to determine
    STALE = "STALE"              # 🕐 Data is outdated


class HealthIndicator:
    """
    Hybrid Health Indicator for data assets.
    
    Key features:
    - Math-based scoring (deterministic, always runs)
    - LLM-based insights (contextual, optional)
    - Consistent health signals across all tables
    - Clear indication if data is safe to use
    """
    
    def __init__(self, use_llm_insights: bool = True):
        """
        Initialize Health Indicator.
        
        Args:
            use_llm_insights: If True, use OpenAI for contextual insights.
                             If False, use rule-based insights only.
        """
        self.health_cache = {}
        self.use_llm_insights = use_llm_insights
        self.llm_client = None
        
        # Initialize OpenAI client if LLM insights are enabled
        if use_llm_insights:
            try:
                from openai import OpenAI
                self.llm_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            except ImportError:
                print("⚠️  OpenAI package not installed. Install with: pip install openai")
                self.use_llm_insights = False
            except Exception as e:
                print(f"⚠️  Failed to initialize OpenAI client: {e}")
                self.use_llm_insights = False
    
    def calculate_health(self, quality_report: Dict[str, Any], 
                        monitor_report: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Calculate unified health indicator from various reports.
        
        HYBRID APPROACH:
        1. Math-based scoring (always runs, deterministic)
        2. LLM-based insights (optional, adds context)
        
        Args:
            quality_report: Output from QualityMetricsTool
            monitor_report: Output from Monitor Agent
        
        Returns:
            {
                "status": HealthStatus,
                "score": float (0-100),
                "badge": str,
                "safe_to_use": bool,  # CRITICAL: Always math-based
                "summary": str,
                "recommendations": List[str],
                "risk_assessment": str
            }
        """
        # STEP 1: Math-based scoring (ALWAYS runs - deterministic)
        score, status, badge, safe_to_use, issues = self._calculate_score_math(
            quality_report, monitor_report
        )
        
        # STEP 2: Generate insights (LLM or rule-based)
        if self.use_llm_insights and self.llm_client:
            insights = self._generate_llm_insights(
                score, status, quality_report, monitor_report, issues
            )
        else:
            insights = self._generate_rule_based_insights(
                score, status, quality_report, monitor_report, issues
            )
        
        # Combine deterministic outputs with contextual insights
        result = {
            # Deterministic outputs (math-based, never from LLM)
            "status": status.value,
            "score": round(score, 1),
            "badge": badge,
            "safe_to_use": safe_to_use,  # CRITICAL: Never LLM-generated
            "issue_count": len(issues),
            "issues": issues[:5],
            "timestamp": datetime.now().isoformat(),
            
            # Contextual outputs (LLM-enhanced or rule-based)
            "summary": insights["summary"],
            "recommendations": insights["recommendations"],
            "risk_assessment": insights["risk_assessment"]
        }
        
        return result
    
    def _calculate_score_math(self, quality_report: Dict[str, Any], 
                              monitor_report: Dict[str, Any]) -> tuple:
        """
        Pure math-based scoring. Deterministic, fast, safe.
        
        Returns: (score, status, badge, safe_to_use, issues)
        """
        scores = []
        issues = []
        
        # From quality metrics (50% weight)
        if quality_report:
            scores.append(quality_report.get('overall_health_score', 100))
            
            if quality_report.get('health_status') == 'CRITICAL':
                issues.append("Critical quality issues detected")
            elif quality_report.get('health_status') == 'DEGRADED':
                issues.append("Some quality metrics are degraded")
        
        # From monitor report (50% weight)
        if monitor_report:
            mon_status = monitor_report.get('status', 'UNKNOWN')
            
            if mon_status == 'PASS':
                scores.append(100)
            elif mon_status == 'PASS_WITH_WARNINGS':
                scores.append(70)
                issues.extend(monitor_report.get('warnings', []))
            elif mon_status == 'FAIL':
                scores.append(0)
                issues.extend(monitor_report.get('critical_errors', []))
        
        # Calculate final score
        final_score = sum(scores) / len(scores) if scores else 50
        
        # Determine status (rule-based thresholds)
        if final_score >= 90:
            status = HealthStatus.HEALTHY
            badge = "✅"
            safe_to_use = True
        elif final_score >= 70:
            status = HealthStatus.DEGRADED
            badge = "⚠️"
            safe_to_use = True  # Usable with caution
        elif final_score >= 30:
            status = HealthStatus.CRITICAL
            badge = "❌"
            safe_to_use = False
        else:
            status = HealthStatus.CRITICAL
            badge = "🚫"
            safe_to_use = False
        
        # Hard override: FAIL status = not safe (safety rule)
        if monitor_report and monitor_report.get('status') == 'FAIL':
            safe_to_use = False
        
        return final_score, status, badge, safe_to_use, issues
    
    def _generate_llm_insights(self, score: float, status: HealthStatus,
                               quality_report: Dict[str, Any],
                               monitor_report: Dict[str, Any],
                               issues: List[str]) -> Dict[str, Any]:
        """
        Generate contextual insights using OpenAI LLM.
        
        Returns: {summary, recommendations, risk_assessment}
        """
        try:
            # Build context for LLM
            context = {
                "score": round(score, 1),
                "status": status.value,
                "issue_count": len(issues),
                "issues_sample": issues[:3]
            }
            
            # Add quality metrics if available
            if quality_report and 'metrics' in quality_report:
                metrics = quality_report['metrics']
                context["freshness"] = metrics.get('freshness', {}).get('status', 'UNKNOWN')
                context["completeness"] = metrics.get('completeness', {}).get('score', 0)
                context["validity"] = metrics.get('validity', {}).get('score', 0)
                context["uniqueness"] = metrics.get('uniqueness', {}).get('score', 0)
            
            # Add monitor status
            if monitor_report:
                context["monitor_status"] = monitor_report.get('status', 'UNKNOWN')
                context["warnings_count"] = len(monitor_report.get('warnings', []))
                context["critical_errors_count"] = len(monitor_report.get('critical_errors', []))
            
            # Construct prompt
            prompt = f"""You are a data quality expert. Analyze this health assessment and provide actionable insights.

Health Score: {context['score']}/100
Status: {context['status']}
Monitor Status: {context.get('monitor_status', 'N/A')}

Quality Metrics:
- Freshness: {context.get('freshness', 'N/A')}
- Completeness: {context.get('completeness', 'N/A')}%
- Validity: {context.get('validity', 'N/A')}%
- Uniqueness: {context.get('uniqueness', 'N/A')}%

Issues Found: {context['issue_count']}
Sample Issues: {', '.join(context['issues_sample'][:2]) if context['issues_sample'] else 'None'}

Provide a JSON response with:
1. "summary": One concise sentence explaining the overall health
2. "recommendations": Array of 2-3 specific, actionable recommendations
3. "risk_assessment": One word - "Low", "Medium", or "High"

Keep it concise and actionable. Focus on what engineers should do next."""

            # Call OpenAI
            response = self.llm_client.chat.completions.create(
                model="gpt-4o-mini",  # Fast, cost-effective model
                messages=[
                    {"role": "system", "content": "You are a data quality expert providing concise, actionable insights."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=250,
                temperature=0.3  # Lower temperature for more consistent outputs
            )
            
            # Parse response
            llm_output = response.choices[0].message.content.strip()
            
            # Try to parse as JSON (handle markdown code blocks)
            try:
                # Remove markdown code blocks if present
                if llm_output.startswith('```'):
                    # Extract JSON from markdown code block
                    lines = llm_output.split('\n')
                    # Remove first line (```json or ```)
                    lines = lines[1:]
                    # Remove last line (```)
                    if lines and lines[-1].strip() == '```':
                        lines = lines[:-1]
                    llm_output = '\n'.join(lines)
                
                insights = json.loads(llm_output)
                return {
                    "summary": insights.get("summary", "LLM analysis completed"),
                    "recommendations": insights.get("recommendations", []),
                    "risk_assessment": insights.get("risk_assessment", "Medium")
                }
            except json.JSONDecodeError as e:
                # Fallback: Use LLM output as summary
                print(f"  ⚠️  Could not parse LLM JSON response: {e}")
                return {
                    "summary": llm_output[:200] if len(llm_output) > 200 else llm_output,
                    "recommendations": ["Review LLM analysis for details"],
                    "risk_assessment": "Medium"
                }
        
        except Exception as e:
            print(f"⚠️  LLM insights generation failed: {e}")
            # Fallback to rule-based
            return self._generate_rule_based_insights(score, status, quality_report, monitor_report, issues)
    
    def _generate_rule_based_insights(self, score: float, status: HealthStatus,
                                      quality_report: Dict[str, Any],
                                      monitor_report: Dict[str, Any],
                                      issues: List[str]) -> Dict[str, Any]:
        """
        Generate insights using deterministic rules (no LLM).
        
        Fallback when LLM is disabled or fails.
        """
        # Determine summary based on score
        if score >= 90:
            summary = "Data is healthy and ready for production use"
            recommendations = [
                "Continue monitoring for any degradation",
                "No immediate action required"
            ]
            risk = "Low"
        elif score >= 70:
            summary = "Data has minor issues but is usable with caution"
            recommendations = [
                "Review non-critical warnings",
                "Monitor for further degradation",
                "Consider fixing issues during next maintenance window"
            ]
            risk = "Medium"
        elif score >= 50:
            summary = "Data quality is degraded and requires attention"
            recommendations = [
                "Investigate root causes of quality issues",
                "Consider blocking new data ingestion",
                "Alert data source owners"
            ]
            risk = "High"
        else:
            summary = "Data has critical issues and should not be used"
            recommendations = [
                "Block data from production immediately",
                "Investigate critical errors",
                "Contact data source team urgently"
            ]
            risk = "High"
        
        # Add specific recommendations based on issues
        if quality_report and 'metrics' in quality_report:
            metrics = quality_report['metrics']
            
            if metrics.get('completeness', {}).get('score', 100) < 80:
                recommendations.append("High null rate detected - investigate data collection")
            
            if metrics.get('freshness', {}).get('status') == 'STALE':
                recommendations.append("Data is stale - check upstream pipeline delays")
            
            if metrics.get('uniqueness', {}).get('score', 100) < 90:
                recommendations.append("Duplicate records found - check for pipeline replays")
        
        return {
            "summary": summary,
            "recommendations": recommendations[:3],  # Limit to top 3
            "risk_assessment": risk
        }
    
    def get_health_badge(self, table_name: str) -> str:
        """
        Get a simple health badge for display in UIs.
        
        Returns something like: "✅ HEALTHY (95)"
        """
        if table_name in self.health_cache:
            cached = self.health_cache[table_name]
            return f"{cached['badge']} {cached['status']} ({cached['score']})"
        
        return "❓ UNKNOWN"
    
    def format_for_dashboard(self, health_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format health indicator for dashboard display.
        
        Returns colors and styles for visualization.
        """
        status = health_result.get('status', 'UNKNOWN')
        
        color_map = {
            'HEALTHY': '#2ECC71',    # Green
            'DEGRADED': '#F39C12',   # Yellow/Orange
            'CRITICAL': '#E74C3C',   # Red
            'UNKNOWN': '#95A5A6',    # Gray
            'STALE': '#9B59B6'       # Purple
        }
        
        return {
            **health_result,
            "color": color_map.get(status, '#95A5A6'),
            "display_text": f"{health_result['badge']} {status}",
            "tooltip": health_result.get('summary', ''),
            "css_class": f"health-{status.lower()}"
        }
    
    def aggregate_schema_health(self, table_healths: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate health across multiple tables in a schema.
        
        Returns schema-level health summary.
        """
        if not table_healths:
            return {
                "status": "UNKNOWN",
                "table_count": 0,
                "healthy_count": 0,
                "degraded_count": 0,
                "critical_count": 0
            }
        
        healthy = sum(1 for t in table_healths if t.get('status') == 'HEALTHY')
        degraded = sum(1 for t in table_healths if t.get('status') == 'DEGRADED')
        critical = sum(1 for t in table_healths if t.get('status') == 'CRITICAL')
        
        avg_score = sum(t.get('score', 0) for t in table_healths) / len(table_healths)
        
        # Schema is only healthy if all tables are healthy
        if critical > 0:
            schema_status = "CRITICAL"
            badge = "❌"
        elif degraded > 0:
            schema_status = "DEGRADED"
            badge = "⚠️"
        else:
            schema_status = "HEALTHY"
            badge = "✅"
        
        return {
            "status": schema_status,
            "badge": badge,
            "score": round(avg_score, 1),
            "table_count": len(table_healths),
            "healthy_count": healthy,
            "degraded_count": degraded,
            "critical_count": critical,
            "healthy_pct": round(healthy / len(table_healths) * 100, 1)
        }
