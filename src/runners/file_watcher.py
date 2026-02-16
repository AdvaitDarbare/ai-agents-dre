"""
File Watcher - Event-Driven Data Quality Validation

Watches data/landing/ for new files and triggers validation.

Human-in-the-Loop Flow:
1. New file detected
2. Check if contract exists
   - YES → Auto-validate
   - NO → Generate proposed contract → Wait for human approval
3. Once approved, validate file
4. PASS → Keep in landing, BLOCKED → Move to quarantine

Usage:
    python -m src.runners.file_watcher
"""

import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError:
    print("❌ watchdog library not installed. Run: pip install watchdog")
    exit(1)

from src.agents.monitor_agent import MonitorAgent
from src.workflows.hitl_contract_workflow import HITLContractWorkflow


class DataLandingHandler(FileSystemEventHandler):
    """
    Handles new file events in data/landing/
    """

    def __init__(self, agent: MonitorAgent):
        self.agent = agent
        self.hitl_workflow = HITLContractWorkflow(agent=agent, contract_store=agent.contract_store)
        self.processing = set()

        # Ensure directories exist
        self.landing_dir = Path("data/landing")
        self.quarantine_dir = Path("data/quarantine")
        self.pending_dir = Path("data/pending_approval")
        self.proposals_dir = Path("config/proposals")

        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self.proposals_dir.mkdir(parents=True, exist_ok=True)

    def extract_dataset_name(self, file_path: Path) -> str:
        """
        Extract dataset name from filename.
        Examples:
            orders_2026-02-15.csv → orders
            customers.parquet → customers
            transactions_latest.csv → transactions
        """
        # Remove date patterns and common suffixes
        name = file_path.stem

        # Split by underscore and take first part (common convention)
        parts = name.split('_')
        if len(parts) > 1:
            # Check if second part looks like a date/timestamp
            if parts[1].replace('-', '').replace(':', '').isdigit():
                return parts[0]
            # Check if it's "latest", "current", "new", etc.
            if parts[1] in ['latest', 'current', 'new', 'final', 'v1', 'v2']:
                return parts[0]

        # Otherwise, use the whole stem
        return name

    def contract_exists(self, dataset_name: str) -> bool:
        """Check if approved contract exists."""
        return self.agent.contract_store.exists(dataset_name)

    def proposal_exists(self, dataset_name: str) -> bool:
        """Check if proposed contract exists (pending approval)."""
        proposal_path = self.proposals_dir / f"{dataset_name}.yaml"
        return proposal_path.exists()

    def generate_contract_proposal(self, dataset_name: str, file_path: Path) -> bool:
        """
        Generate AI-powered contract proposal for new dataset.
        Returns True if successful.
        """
        try:
            print(f"   🤖 Generating contract proposal using AI...")

            # Request metadata payload so watcher can store proposal + audit metadata.
            proposal = self.agent.propose_contract(
                dataset_name=dataset_name,
                data_path=str(file_path),
                include_metadata=True
            )

            # Save proposal to proposals directory
            proposal_path = self.proposals_dir / f"{dataset_name}.yaml"
            with open(proposal_path, 'w') as f:
                f.write(proposal.get("yaml_content", ""))

            # Create metadata file with proposal info
            stats = proposal.get("stats", {}) if isinstance(proposal, dict) else {}
            metadata_path = self.proposals_dir / f"{dataset_name}.meta.json"
            with open(metadata_path, 'w') as f:
                json.dump({
                    "dataset_name": dataset_name,
                    "proposed_at": datetime.now().isoformat(),
                    "source_file": str(file_path),
                    "status": "pending_approval",
                    "row_count": stats.get("row_count"),
                    "column_count": stats.get("column_count")
                }, f, indent=2)

            print(f"   ✅ Proposal saved to {proposal_path}")
            return True

        except Exception as e:
            print(f"   ❌ Failed to generate proposal: {str(e)}")
            return False

    def validate_file(self, file_path: Path, dataset_name: str) -> dict:
        """Run validation on file with existing contract."""
        print(f"   📊 Running validation pipeline...")

        verdict = self.agent.evaluate_data_file(file_path=str(file_path), dataset_name=dataset_name)

        # Write verdict file next to data file
        verdict_path = file_path.with_suffix(file_path.suffix + '.verdict.json')
        with open(verdict_path, 'w') as f:
            json.dump(verdict, f, indent=2)

        return verdict

    def handle_verdict(self, file_path: Path, verdict: dict):
        """Take action based on verdict."""
        status = verdict['status']

        if status == 'BLOCKED':
            # Move to quarantine
            dest = self.quarantine_dir / file_path.name
            shutil.move(str(file_path), str(dest))
            print(f"   🚫 BLOCKED - Moved to quarantine: {file_path.name}")
            print(f"      Reason: {verdict.get('reason', 'Unknown')}")

        elif status == 'WARNING':
            print(f"   ⚠️  WARNING - File kept in landing with warnings")
            print(f"      Reason: {verdict.get('reason', 'Unknown')}")

        else:  # PASSED
            print(f"   ✅ PASSED - File ready for downstream processing")
            quality_score = verdict.get("profile", {}).get("overall_quality_score", "N/A")
            print(f"      Quality Score: {quality_score}")

    def on_created(self, event):
        """Handle new file creation events."""
        if event.is_directory:
            return

        file_path = Path(event.src_path)

        # Only process data files
        if file_path.suffix not in ['.csv', '.parquet', '.json']:
            return

        # Ignore verdict files
        if '.verdict.' in file_path.name:
            return

        # Prevent duplicate processing
        if str(file_path) in self.processing:
            return

        self.processing.add(str(file_path))

        try:
            dataset_name = self.extract_dataset_name(file_path)

            print(f"\n{'='*70}")
            print(f"🔔 NEW FILE DETECTED")
            print(f"{'='*70}")
            print(f"File: {file_path.name}")
            print(f"Dataset: {dataset_name}")
            print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*70}")

            # Check if contract exists
            if self.contract_exists(dataset_name):
                print(f"✅ Contract found: {self.agent.contract_store.path_for(dataset_name)}")
                print(f"   Running validation...")

                # Run validation
                verdict = self.validate_file(file_path, dataset_name)

                # Handle result
                self.handle_verdict(file_path, verdict)

            else:
                print(f"⚠️  No contract found for dataset: {dataset_name}")
                print(f"   This appears to be a NEW dataset")

                # Check if proposal already exists
                if self.proposal_exists(dataset_name):
                    # Additional files while waiting for HITL approval still go to pending.
                    dest = self.pending_dir / file_path.name
                    shutil.move(str(file_path), str(dest))
                    print(f"   📁 Moved to pending approval: {dest}")
                    print(f"   ℹ️  Contract proposal already exists, waiting for human approval")
                else:
                    # Trigger LangGraph HITL workflow (durable interrupt/resume).
                    try:
                        workflow_result = self.hitl_workflow.start_missing_contract(
                            dataset_name=dataset_name,
                            file_path=str(file_path),
                        )
                        status = workflow_result.get("status")
                        if status == "paused_hitl":
                            pending_path = workflow_result.get("state", {}).get("pending_file_path")
                            if pending_path:
                                print(f"   📁 Moved to pending approval: {pending_path}")
                            print(f"\n   🙋 HUMAN ACTION REQUIRED:")
                            print(f"   1. Open UI: http://localhost:5173")
                            print(f"   2. Review proposed contract for '{dataset_name}'")
                            print(f"   3. Edit if needed and approve")
                            print(f"   4. File will be validated automatically after approval")
                        else:
                            print(f"   ⚠️ Workflow finished with status: {status}")
                    except Exception as workflow_err:
                        print(f"   ❌ HITL workflow start failed: {workflow_err}")
                        print("   Falling back to legacy proposal generation path.")
                        dest = self.pending_dir / file_path.name
                        if file_path.exists():
                            shutil.move(str(file_path), str(dest))
                            print(f"   📁 Moved to pending approval: {dest}")
                        source_for_generation = dest if dest.exists() else file_path
                        success = self.generate_contract_proposal(dataset_name, source_for_generation)
                        if success:
                            print(f"\n   🙋 HUMAN ACTION REQUIRED:")
                            print(f"   1. Open UI: http://localhost:5173")
                            print(f"   2. Review proposed contract for '{dataset_name}'")
                            print(f"   3. Edit if needed and approve")
                            print(f"   4. File will be validated automatically after approval")
                        else:
                            print(f"   ❌ Could not generate proposal - manual contract creation needed")

            print(f"{'='*70}\n")

        except Exception as e:
            print(f"❌ Error processing {file_path}: {str(e)}")
            import traceback
            traceback.print_exc()

        finally:
            self.processing.discard(str(file_path))


def watch_directory(watch_path: str = "data/landing"):
    """
    Start watching directory for new files.

    Args:
        watch_path: Directory to watch for new files
    """
    # Ensure watch directory exists
    Path(watch_path).mkdir(parents=True, exist_ok=True)

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║                   FILE WATCHER - EVENT DRIVEN DRE                ║
╚══════════════════════════════════════════════════════════════════╝

📁 Watching: {watch_path}
🎯 Mode: Human-in-the-Loop Contract Approval

Flow:
  1. Drop file → Watcher detects
  2. Contract exists? → Auto-validate
  3. New dataset? → Generate proposal → Wait for approval
  4. BLOCKED files → Move to data/quarantine/
  5. PASSED files → Ready for downstream

Press Ctrl+C to stop
──────────────────────────────────────────────────────────────────
""")

    # Initialize agent and handler
    agent = MonitorAgent()
    event_handler = DataLandingHandler(agent)

    # Start observer
    observer = Observer()
    observer.schedule(event_handler, watch_path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping file watcher...")
        observer.stop()

    observer.join()
    print("✅ File watcher stopped")


if __name__ == "__main__":
    watch_directory()
