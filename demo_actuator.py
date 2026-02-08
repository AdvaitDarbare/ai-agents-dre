"""
Demo: Complete Monitor Agent with Actuator (Detector + Action)

This demonstrates the complete self-healing data pipeline:
1. Detector: Identifies data quality issues
2. Actuator: Takes physical action (moves files)

Result: Bad files CANNOT enter Apache Doris. They are physically blocked.
"""

import shutil
from pathlib import Path
from src.agents.monitor_agent import MonitorAgent


def setup_demo_files():
    """Copy test files to landing for demo."""
    landing = Path("data/landing")
    
    # Copy perfect file
    if not (landing / "demo_perfect.csv").exists():
        shutil.copy("data/landing/transactions_perfect.csv", "data/landing/demo_perfect.csv")
    
    # Copy bad file (future timestamp)
    if not (landing / "demo_bad.csv").exists():
        shutil.copy("data/landing/transactions_future.csv", "data/landing/demo_bad.csv")
    
    return landing / "demo_perfect.csv", landing / "demo_bad.csv"


def demo_complete_workflow():
    """Demonstrate the complete Detector + Actuator workflow."""
    
    print("\n" + "=" * 100)
    print("🚦 COMPLETE WORKFLOW DEMO: Detector + Actuator")
    print("=" * 100)
    print("""
SCENARIO: We are protecting Apache Doris from bad data.

The Monitor Agent acts as a Traffic Control Center:
- 🔍 Detector: Scans files for quality issues
- 🔧 Actuator: Physically moves files based on results

Exit Ramps:
- ✅ data/staging/ → VIP Lounge (Apache Doris loads ONLY from here)
- ❌ data/quarantine/ → Jail (Bad data locked away for human review)

Let's see it in action...
""")
    
    # Setup
    perfect_file, bad_file = setup_demo_files()
    agent = MonitorAgent()
    
    # Test 1: Perfect File → Should go to STAGING
    print("\n" + "🎯" * 50)
    print("TEST 1: Perfect Transaction File")
    print("🎯" * 50)
    print(f"📁 Processing: {perfect_file}")
    print()
    
    results_perfect = agent.process_file(
        file_path=str(perfect_file),
        table_name='transactions',
        max_age_hours=24,
        take_action=True  # 🔧 ACTUATOR ENABLED
    )
    
    print(f"\n📊 FINAL STATUS: {results_perfect['overall_status']}")
    print(f"📦 ACTION TAKEN: {results_perfect['action_taken']}")
    print(f"📍 NEW LOCATION: {results_perfect['new_location']}")
    
    # Test 2: Bad File → Should go to QUARANTINE
    print("\n\n" + "🎯" * 50)
    print("TEST 2: Transaction File with Data Quality Issues")
    print("🎯" * 50)
    print(f"📁 Processing: {bad_file}")
    print()
    
    results_bad = agent.process_file(
        file_path=str(bad_file),
        table_name='transactions',
        max_age_hours=24,
        take_action=True  # 🔧 ACTUATOR ENABLED
    )
    
    print(f"\n📊 FINAL STATUS: {results_bad['overall_status']}")
    print(f"📦 ACTION TAKEN: {results_bad['action_taken']}")
    print(f"📍 NEW LOCATION: {results_bad['new_location']}")
    
    # Show final state
    print("\n\n" + "=" * 100)
    print("📋 FINAL STATE OF THE SYSTEM")
    print("=" * 100)
    
    staging_files = list(Path("data/staging").glob("demo_*.csv"))
    quarantine_files = list(Path("data/quarantine").glob("demo_*.csv"))
    landing_files = list(Path("data/landing").glob("demo_*.csv"))
    
    print(f"\n📁 data/landing/: {len(landing_files)} file(s)")
    for f in landing_files:
        print(f"   - {f.name} (STILL HERE - NOT PROCESSED)")
    
    print(f"\n✅ data/staging/: {len(staging_files)} file(s)")
    for f in staging_files:
        print(f"   - {f.name} ← APPROVED, ready for Apache Doris")
    
    print(f"\n❌ data/quarantine/: {len(quarantine_files)} file(s)")
    for f in quarantine_files:
        print(f"   - {f.name} ← BLOCKED, requires human review")
        # Show error report
        error_report = f.with_suffix('.csv.error.json')
        if error_report.exists():
            print(f"     📄 Error report: {error_report.name}")
    
    # Key Insight
    print("\n" + "=" * 100)
    print("🔑 KEY INSIGHT: The Physical Layer is Complete!")
    print("=" * 100)
    print("""
✅ BEFORE (What We Just Built):
   - Bad files are PHYSICALLY MOVED to quarantine
   - They CANNOT enter Apache Doris
   - They will NOT be re-scanned on the next run
   - The Agentic Brain's memory stays clean

❌ WITHOUT THIS (What Would Happen):
   - Bad files stay in landing/
   - Agent re-scans them every time (waste of resources)
   - Error logs fill up with duplicate messages
   - Risk of bad data accidentally entering Doris
   - Agentic Brain gets confused by repeated errors

🚀 NEXT STEP: Add the Cognitive Layer (AI)
   - Anomaly Detection (ML-based)
   - Root Cause Analysis (AI reasoning)
   - Auto-repair suggestions (GenAI)
   - Pattern recognition across failures

The Traffic Cop (Detector + Actuator) is ready.
Now we can add the Detective (AI Agent)!
""")
    
    print("=" * 100)
    print("✅ Demo Complete!")
    print("=" * 100)


if __name__ == '__main__':
    demo_complete_workflow()
