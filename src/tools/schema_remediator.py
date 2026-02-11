"""
Schema Remediator Tool - The Self-Healing Component

This tool uses an LLM to propose fixes for broken Data Contracts.
It takes a failed contract, the error details (e.g. "Missing Column"), and generates a corrected YAML.
"""

import os
from agno.agent import Agent
from agno.models.openai import OpenAIChat

class SchemaRemediator:
    """
    Intelligent Agent that fixes YAML schemas.
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
                "Preserve all existing descriptions and table metadata.",
                "Output ONLY the raw YAML string. Do not use Markdown code blocks (```yaml).",
                "Do not include any conversational text."
            ],
            markdown=False # We want raw text
        )

    def propose_schema_update(self, current_yaml: str, error_details: str) -> str:
        """
        Generate a fixed YAML schema.
        
        Args:
            current_yaml: The content of the current broken schema.
            error_details: A string describing the schema errors (diff).
            
        Returns:
            The corrected YAML string.
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
                
            return content.strip()
            
        except Exception as e:
            return f"# Error generating fix: {str(e)}\n{current_yaml}"
