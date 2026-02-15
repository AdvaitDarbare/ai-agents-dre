"""
Schema Remediator Tool - The Self-Healing Component (Hardened)

This tool uses an LLM to propose fixes for broken Data Contracts.
It takes a failed contract, the error details (e.g. "Missing Column"), and generates a corrected YAML.

Safety Features (Post-Audit):
1. YAML Validation: Ensures LLM output is parseable YAML before returning.
2. Semantic Guardrails: Prevents the LLM from removing existing columns.
3. Backup on Apply: Creates .backup files before any overwrite.
"""

import os
import json
import yaml
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
from agno.agent import Agent
from agno.models.openai import OpenAIChat
from src.tools.contract_generator import DataContractGenerator, ContractGenerationResult
from src.tools.contract_diff import merge_contracts


class SchemaRemediator:
    """
    Intelligent Agent that fixes YAML schemas.
    Includes safety guardrails identified in the architecture audit.
    """
    
    def __init__(self):
        """
        Initialize the remediator with an LLM.
        """
        self.contract_generator = DataContractGenerator()
        self.last_generation_report: Dict[str, Any] = {}
        self.agent = Agent(
            model=OpenAIChat(id=os.getenv("OPENAI_MODEL_NAME", "gpt-4o")),
            description="You are a Senior Data Engineer specializing in Data Contracts (YAML).",
            instructions=[
                "You will be given a current YAML schema and a list of validation errors.",
                "Your task is to generate a CORRECTED YAML schema that resolves the errors.",
                "If there are new columns, add them to the 'columns' list with appropriate data types.",
                "If there are type mismatches, update the 'data_type' to match the actual data found.",
                "NEVER remove existing columns. Only add or modify types.",
                "Preserve all existing descriptions, table metadata, quality checks, and isPrimaryKey flags.",
                "Output ONLY the raw YAML string. Do not use Markdown code blocks (```yaml).",
                "Do not include any conversational text."
            ],
            markdown=False  # We want raw text
        )

    def generate_initial_contract_with_report(
        self, data_path: str, dataset_name: str = ""
    ) -> ContractGenerationResult:
        """
        Generate an initial contract with structured metadata.
        Prefers datacontract-cli and falls back to deterministic inference.
        """
        print(f"🕵️ Generating contract from source: {data_path}")
        result = self.contract_generator.generate_from_source(
            data_path=data_path, dataset_name=dataset_name or Path(data_path).stem
        )
        self.last_generation_report = result.to_dict()
        if result.engine == "datacontract-cli":
            print("✅ Contract generation used datacontract-cli")
        else:
            print("⚠️ Contract generation used deterministic fallback")
        return result

    def generate_initial_contract(self, data_path: str, dataset_name: str = "") -> str:
        """
        Backwards-compatible contract generation entry point.
        Returns only YAML content.
        """
        result = self.generate_initial_contract_with_report(data_path, dataset_name)
        return result.yaml_content

    def propose_schema_update(self, current_yaml: str, error_details: str, impact_context: str = "") -> str:
        """
        Generate a fixed YAML schema with safety validation.
        
        Args:
            current_yaml: The content of the current broken schema.
            error_details: A string describing the schema errors (diff).
            impact_context: Metadata about downstream impact for context.
            
        Returns:
            The corrected YAML string, guaranteed to be valid YAML.
        """
        prompt = f"""
        ### Current Broken Schema
        {current_yaml}
        
        ### Validation Errors / Schema Evolution
        {error_details}

        ### Downstream Impact Context
        {impact_context}
        
        ### Task
        Fix the schema to accommodate these changes. Return the fully valid YAML.
        Use the Impact Context to improve the 'description' or 'info' sections if relevant.
        """
        
        try:
            response = self.agent.run(prompt)
            content = response.content.strip()
            
            # Clean up potential markdown formatting from LLM
            if content.startswith("```yaml"):
                content = content[7:]
            elif content.startswith("```"):
                content = content[3:]
                
            if content.endswith("```"):
                content = content[:-3]
                
            content = content.strip()
            
            # --------------------------------------------------
            # SAFETY GATE 1: Validate YAML is parseable
            # --------------------------------------------------
            if not self._validate_yaml(content):
                print("❌ SAFETY: LLM output is not valid YAML. Returning original.")
                return current_yaml
            
            # --------------------------------------------------
            # SAFETY GATE 2: Semantic validation (no columns removed)
            # --------------------------------------------------
            if not self._validate_no_columns_removed(current_yaml, content):
                print("❌ SAFETY: LLM tried to remove columns. Returning original.")
                return current_yaml
            
            return content
            
        except Exception as e:
            return f"# Error generating fix: {str(e)}\n{current_yaml}"

    def propose_schema_update_hybrid(
        self,
        current_yaml: str,
        data_path: str,
        error_details: str,
        impact_context: str = "",
        enable_llm: bool = True,
    ) -> Dict[str, Any]:
        """
        Hybrid remediation:
        1) Generate observed schema from data (CLI/fallback).
        2) Deterministically merge into current schema.
        3) Optionally enrich via LLM (non-destructive).
        """
        generation = self.generate_initial_contract_with_report(data_path)
        observed_yaml = generation.yaml_content

        merged_yaml, merge_summary = merge_contracts(current_yaml, observed_yaml)

        llm_yaml = None
        if enable_llm:
            llm_yaml = self.propose_schema_update(merged_yaml, error_details, impact_context)

        return {
            "deterministic_yaml": merged_yaml,
            "llm_yaml": llm_yaml,
            "observed_yaml": observed_yaml,
            "merge_summary": merge_summary,
            "generation": generation.to_dict(),
        }

    def _validate_yaml(self, content: str) -> bool:
        """
        SAFETY GATE 1: Ensure the content is valid, parseable YAML.
        """
        try:
            parsed = yaml.safe_load(content)
            if parsed is None or not isinstance(parsed, dict):
                return False
            # Must have 'columns' key to be a valid schema
            if "columns" not in parsed:
                return False
            return True
        except yaml.YAMLError as e:
            print(f"⚠️ YAML Parse Error: {e}")
            return False

    def _validate_no_columns_removed(self, original_yaml: str, proposed_yaml: str) -> bool:
        """
        SAFETY GATE 2: Ensure the LLM didn't remove any existing columns.
        Only additions and type modifications are allowed.
        """
        try:
            original = yaml.safe_load(original_yaml)
            proposed = yaml.safe_load(proposed_yaml)
            
            if not original or not proposed:
                return True  # Can't validate, allow it
            
            original_cols = {c.get("name") for c in original.get("columns", [])}
            proposed_cols = {c.get("name") for c in proposed.get("columns", [])}
            
            removed = original_cols - proposed_cols
            if removed:
                print(f"⚠️ BLOCKED: LLM tried to remove columns: {removed}")
                return False
            
            return True
            
        except Exception:
            return True  # If we can't parse, allow it (Gate 1 would catch it)

    @staticmethod
    def create_backup(file_path: str) -> str:
        """
        Create a timestamped backup of a file before overwriting.
        
        Args:
            file_path: Path to the file to back up.
            
        Returns:
            Path to the backup file.
        """
        path = Path(file_path)
        if not path.exists():
            return ""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = path.parent / f"{path.stem}.backup_{timestamp}{path.suffix}"
        shutil.copy2(path, backup_path)
        print(f"📦 Backup created: {backup_path}")
        return str(backup_path)

    def apply_fix(self, contract_path: str, proposed_yaml: str) -> str:
        """
        Apply the proposed fix to the contract file with safety backup.
        
        Args:
            contract_path: Path to the contract YAML.
            proposed_yaml: The new YAML content to write.
            
        Returns:
            The path to the created backup file.
        """
        path = Path(contract_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Create standard backup (original file)
        backup_path = self.create_backup(contract_path)
        
        # 2. Save NEW version to Governance History
        # We explicitly save this version so we can roll back TO it later.
        history_dir = Path("config/history")
        history_dir.mkdir(parents=True, exist_ok=True)
        
        version_filename = f"{path.stem}_v{timestamp}{path.suffix}"
        version_path = history_dir / version_filename
        
        with open(version_path, "w") as f:
            f.write(proposed_yaml)
            
        print(f"📜 Governance Version Saved: {version_path}")
        
        # 3. Apply the fix
        with open(contract_path, "w") as f:
            f.write(proposed_yaml)
            
        return str(version_path) # Return version path for logging

    def generate_contract_from_metadata(self, metadata: Dict[str, Any], dataset_name: str) -> str:
        """
        Generate a full ODCS YAML contract from profiling metadata.
        """
        # Mocking for testing if no key
        if not os.getenv("OPENAI_API_KEY"):
            print("⚠️ No OPENAI_API_KEY found. Using deterministic fallback.")
            return self._generate_deterministic_contract(metadata, dataset_name)

        prompt = f"""
You are a Data Architect. Generate a production-grade ODCS Data Contract (YAML) for dataset '{dataset_name}'.

INPUT METADATA (Statistics & Samples):
{json.dumps(metadata, indent=2)}

INSTRUCTIONS:
1. **Schema Definition**:
   - Map inferred types to standard SQL types (varchar, integer, decimal, timestamp, boolean).
   - Set `nullable` based on the profile (if null_count > 0, nullable: true).

2. **Advanced Semantics**:
   - **PII**: Tag columns like email, ssn, name, phone as `pii: true`.
   - **Business Keys**: Tag highly unique ID columns as `business_key: true` and `primary_key: true`.
   - **Criticality**:
     - HIGH (critical: true): IDs, timestamps, financial amounts, status codes.
     - LOW (critical: false): Descriptions, logs, optional metadata.

3. **Quality Rules**:
   - Add `min_value` / `max_value` for numeric columns where appropriate (use profile min/max with some buffer).
   - If `possible_values` exists (low cardinality), add `allowed_values` list.

4. **Output Format**:
   - STRICT ODCS YAML format.
   - NO Markdown code blocks. Raw YAML only.
"""
        response = self.agent.run(prompt)
        
        # Clean up potential markdown code blocks if LLM ignores instructions
        clean_yaml = response.content.replace("```yaml", "").replace("```", "").strip()
        
        return clean_yaml

    def _generate_deterministic_contract(self, metadata: Dict[str, Any], dataset_name: str) -> str:
        """Deterministic fallback for contract generation."""
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.1.0",
            "dataset": dataset_name,
            "columns": [],
            "quality": {"custom_checks": []}
        }
        
        for col in metadata.get("columns", []):
            dtype = col["inferred_type"]
            if "int" in dtype:
                sql_type = "integer"
            elif "float" in dtype:
                sql_type = "double"
            elif "bool" in dtype:
                sql_type = "boolean"
            elif "datetime" in dtype:
                sql_type = "timestamp"
            else:
                sql_type = "varchar"

            col_def = {
                "name": col["name"],
                "data_type": sql_type,
                "nullable": col["nullable"],
                "description": f"Automatically inferred for {col['name']}"
            }
            
            # PII Heuristic
            name_lower = col["name"].lower()
            if any(p in name_lower for p in ["email", "phone", "ssn", "password", "address"]):
                col_def["pii"] = True
                
            # Business Key Heuristic
            if col["unique_values"] == metadata["total_rows"] and not col["nullable"]:
                col_def["business_key"] = True
                col_def["primary_key"] = True
                col_def["critical"] = True
            
            # Criticality Heuristic
            if any(k in name_lower for k in ["id", "amount", "status", "timestamp", "date"]):
                col_def["critical"] = True
            else:
                col_def["critical"] = False
            
            # Range Constraints (Soft)
            if sql_type in ["integer", "double", "float"] and "min_value" in col:
                 # We don't add them to 'quality' yet, just sticking to column props for now
                 # or add strict min/max? User asked for Soft vs Hard.
                 # Let's put strict min/max in column def (which implies Hard in some systems)
                 # or use custom checks. Typically column min/max is hard.
                 col_def["min_value"] = col.get("min_value")
                 col_def["max_value"] = col.get("max_value")

            contract["columns"].append(col_def)
            
        return yaml.dump(contract, sort_keys=False)
