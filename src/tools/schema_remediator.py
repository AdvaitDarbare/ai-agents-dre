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
import yaml
import shutil
from pathlib import Path
from datetime import datetime
from agno.agent import Agent
from agno.models.openai import OpenAIChat


class SchemaRemediator:
    """
    Intelligent Agent that fixes YAML schemas.
    Includes safety guardrails identified in the architecture audit.
    """
    
    def __init__(self):
        """
        Initialize the remediator with an LLM.
        """
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

    def propose_schema_update(self, current_yaml: str, error_details: str) -> str:
        """
        Generate a fixed YAML schema with safety validation.
        
        Args:
            current_yaml: The content of the current broken schema.
            error_details: A string describing the schema errors (diff).
            
        Returns:
            The corrected YAML string, guaranteed to be valid YAML.
        """
        prompt = f"""
        ### Current Broken Schema
        {current_yaml}
        
        ### Validation Errors / Schema Evolution
        {error_details}
        
        ### Task
        Fix the schema to accommodate these changes. Return the fully valid YAML.
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
