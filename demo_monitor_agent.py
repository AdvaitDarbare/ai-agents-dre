"""
Demo: Monitor Agent with Timeliness Check

This script demonstrates the complete Monitor Agent that orchestrates
all data quality checks including the new Timeliness check.
"""

from src.agents.monitor_agent import MonitorAgent


def demo_monitor_agent():
    """Demonstrate the Monitor Agent with all checks."""
    
    print("\n" + "=" * 100)
    print("🤖 MONITOR AGENT DEMO - Complete Data Quality Orchestration")
    print("=" * 100)
    print("""
The Monitor Agent is a deterministic orchestrator that runs all data quality checks:
1. Timeliness - Ensures files are fresh (not stale)
2. Schema Validation - Validates structure and uniqueness
3. Data Profiling - Checks volume, completeness, range, and consistency

Let's see it in action...
""")
    
    agent = MonitorAgent()
    
    # Test 1: Perfect Data - All Checks Pass
    print("\n" + "🎯" * 50)
    print("TEST 1: Perfect Transaction Data (All Checks Should Pass)")
    print("🎯" * 50)
    
    results = agent.run_all_checks(
        file_path='data/landing/transactions_perfect.csv',
        table_name='transactions',
        max_age_hours=24
    )
    
    print(f"\n📊 Final Verdict: {'✅ CAN LOAD' if results['can_load'] else '❌ CANNOT LOAD'}")
    
    # Test 2: File with Data Quality Issues
    print("\n\n" + "🎯" * 50)
    print("TEST 2: Transactions with Future Timestamps (Consistency Violation)")
    print("🎯" * 50)
    
    results = agent.run_all_checks(
        file_path='data/landing/transactions_future.csv',
        table_name='transactions',
        max_age_hours=24
    )
    
    print(f"\n📊 Final Verdict: {'✅ CAN LOAD' if results['can_load'] else '❌ CANNOT LOAD'}")
    
    # Summary
    print("\n\n" + "=" * 100)
    print("📋 SUMMARY - Monitor Agent Capabilities")
    print("=" * 100)
    print("""
The Monitor Agent provides a single entry point for all data quality checks:

✓ Timeliness Check:
  - Validates file age against threshold
  - Fails fast if file is stale
  - Prevents processing old data

✓ Schema Validation:
  - Column existence and data types
  - Schema drift detection
  - Primary key uniqueness

✓ Data Profiling:
  - Volume (row count)
  - Completeness (NULL values)
  - Range (min/max values)
  - Consistency (business logic)

✓ Orchestration:
  - Runs checks in optimal order
  - Fails fast when appropriate
  - Provides clear pass/fail status
  - Returns structured results for logging

This deterministic agent ensures only high-quality data enters Apache Doris!
""")
    
    print("=" * 100)
    print("✅ Demo Complete!")
    print("=" * 100)


if __name__ == '__main__':
    demo_monitor_agent()
