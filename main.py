"""
Autonomous Data Reliability Engineering (DRE) System
A team of 4 specialized AI agents that collaborate autonomously.
"""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from agents import MonitorAgent, DiagnoserAgent, HealerAgent, ValidatorAgent


def run_autonomous_pipeline(file_path: str, table_name: str = "transactions"):
    """
    Run the full autonomous DRE pipeline.
    
    Pipeline Flow:
    1. Monitor Agent → Validates data against contract
    2. Diagnoser Agent → Analyzes failures and determines root cause
    3. Healer Agent → Attempts auto-remediation
    4. Validator Agent → Verifies healing was successful
    """
    print("\n" + "="*80)
    print("🤖 AUTONOMOUS DATA RELIABILITY ENGINEERING SYSTEM")
    print("="*80)
    print(f"Pipeline: Monitor → Diagnoser → Healer → Validator")
    print("="*80 + "\n")
    
    # Initialize agents
    monitor = MonitorAgent(contracts_dir="contracts")
    diagnoser = DiagnoserAgent()
    healer = HealerAgent()
    validator = ValidatorAgent()
    
    # STEP 1: Monitor Agent (The Gatekeeper)
    monitor_report = monitor.run(file_path, table_name)
    monitor.save_report(monitor_report)
    
    # --- HUMAN-IN-THE-LOOP: Missing Contract ---
    if monitor_report.get('status') == 'CONTRACT_MISSING':
        print("\n" + "="*80)
        print("🤖 HUMAN-IN-THE-LOOP REQUIRED")
        print("="*80)
        print(f"No contract found for table '{table_name}'.")
        print("\n📝 A DRAFT CONTRACT HAS BEEN GENERATED:")
        print("-" * 60)
        print(monitor_report.get('inferred_contract'))
        print("-" * 60)
        
        try:
            choice = input(f"\n❓ [HITL] Approval Required: Save this contract to 'contracts/{table_name}.yaml'? (y/n): ").strip().lower()
        except EOFError:
            choice = 'n'
            
        if choice == 'y':
            contract_path = f"contracts/{table_name}.yaml"
            with open(contract_path, 'w') as f:
                f.write(monitor_report.get('inferred_contract'))
            print(f"\n✅ Contract saved to {contract_path}")
            print("\n🔄 RE-RUNNING MONITOR AGENT WITH NEW CONTRACT...")
            monitor_report = monitor.run(file_path, table_name)
        else:
            print("\n❌ Contract rejected. Proceeding without validation (expect failure/skips).")

    monitor.save_report(monitor_report)
    
    # Check if we need to proceed to diagnosis
    if monitor_report['status'] == 'PASS':
        print("\n✅ Pipeline Complete: Data passed all checks!")
        return monitor_report
    
    # STEP 2: Diagnoser Agent (The Analyzer)
    diagnosis_report = diagnoser.run(monitor_report)
    
    # Check if issue is auto-fixable
    if not diagnosis_report['auto_fixable']:
        print("\n⚠️  Pipeline Halted: Issue requires manual intervention")
        return {
            "monitor_report": monitor_report,
            "diagnosis_report": diagnosis_report,
            "final_status": "MANUAL_INTERVENTION_REQUIRED"
        }
    
    # STEP 3: Healer Agent (The Fixer)
    healing_report = healer.run(
        diagnosis_report, 
        dataframe=monitor_report.get('dataframe')
    )
    
    # STEP 4: Validator Agent (The Verifier)
    validation_report = validator.run(healing_report, monitor_agent=monitor)
    
    # Final summary
    print("\n" + "="*80)
    print("📊 PIPELINE SUMMARY")
    print("="*80)
    print(f"Monitor Status: {monitor_report['status']}")
    print(f"Diagnosis: {diagnosis_report['diagnosis']}")
    print(f"Healing Applied: {healing_report['healing_applied']}")
    print(f"Final Validation: {validation_report['final_status']}")
    print("="*80 + "\n")
    
    return {
        "monitor_report": monitor_report,
        "diagnosis_report": diagnosis_report,
        "healing_report": healing_report,
        "validation_report": validation_report,
        "final_status": validation_report['final_status']
    }


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python main.py <file_path> [table_name]")
        print("\nExample:")
        print("  python main.py data/transactions.csv transactions")
        sys.exit(1)
    
    file_path = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else "default"
    
    # Verify file exists
    if not Path(file_path).exists():
        print(f"❌ Error: File not found: {file_path}")
        sys.exit(1)
    
    # Run the pipeline
    result = run_autonomous_pipeline(file_path, table_name)
    
    # Exit with appropriate code
    # Status can be in 'final_status' (full pipeline) or 'status' (monitor only)
    status = result.get('final_status') or result.get('status')
    
    if status in ['APPROVED', 'PASS', 'PASS_WITH_WARNINGS']:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
