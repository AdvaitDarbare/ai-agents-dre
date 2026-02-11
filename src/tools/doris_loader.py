"""
Doris Loader Tool - The Actuator

This tool loads validated data into Apache Doris using the Stream Load API.
It is the final step in the data pipeline, executed only if the Monitor Agent returns "PASSED".

Key Features:
1. Efficient Stream Load: Uses HTTP PUT to stream data directly to FE.
2. Pandas Integration: Accepts DataFrame directly.
3. Mock Mode: Allows testing without a live Doris instance.
"""

import os
import uuid
import json
import base64
import requests
import pandas as pd
from typing import Dict, Any, Optional

class DorisLoader:
    """
    The Actuator - Loads data into Apache Doris.
    """
    
    def __init__(self):
        """
        Initialize the Doris Loader with configuration from environment variables.
        """
        self.host = os.getenv("DORIS_FE_HOST", "127.0.0.1")
        self.port = os.getenv("DORIS_FE_HTTP_PORT", "8030")
        self.user = os.getenv("DORIS_USER", "root")
        self.password = os.getenv("DORIS_PASSWORD", "")
        self.db = os.getenv("DORIS_DB", "test_db")
        self.mock_mode = os.getenv("DORIS_MOCK_MODE", "False").lower() == "true"

    def load_data(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Load a Pandas DataFrame into a Doris table using Stream Load.
        
        Args:
            df: Pandas DataFrame containing the data.
            table_name: Target table name.
            
        Returns:
            Dictionary with load status.
        """
        if self.mock_mode:
            print(f"🔧 [Mock Mode] Simulating load of {len(df)} rows into '{self.db}.{table_name}'...")
            return {
                "Status": "Success",
                "Message": "Mock load successful",
                "NumberTotalRows": len(df),
                "NumberLoadedRows": len(df),
                "LoadUrl": "http://mock-doris/api/_stream_load"
            }
            
        # Prepare Data
        # Convert to CSV string without header and index
        csv_data = df.to_csv(index=False, header=False)
        
        # Prepare Request
        load_url = f"http://{self.host}:{self.port}/api/{self.db}/{table_name}/_stream_load"
        label = f"label_{uuid.uuid4()}"
        
        headers = {
            "Expect": "100-continue",
            "label": label,
            "column_separator": ",",
            "format": "csv"
            # Add other headers like 'columns' if mapping is needed
        }
        
        auth = (self.user, self.password)
        
        print(f"🚀 Loading {len(df)} rows into '{self.db}.{table_name}' via Stream Load...")
        
        try:
            response = requests.put(
                load_url,
                data=csv_data,
                headers=headers,
                auth=auth
            )
            
            # Check HTTP Status
            response.raise_for_status()
            
            # Parse Doris Response
            resp_dict = response.json()
            
            if resp_dict.get("Status") != "Success":
                error_msg = f"Doris Load Failed: {resp_dict.get('Message')}"
                error_url = resp_dict.get('ErrorURL')
                if error_url:
                    error_msg += f" (Check: {error_url})"
                raise Exception(error_msg)
                
            print(f"✅ Load Successful! Label: {resp_dict.get('Label')}")
            return resp_dict
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"HTTP Connection Failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Load Error: {str(e)}")

if __name__ == "__main__":
    # Test the Loader (Mock Mode recommended for development)
    os.environ["DORIS_MOCK_MODE"] = "True"
    
    loader = DorisLoader()
    
    # Create dummy data
    df_test = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [95.5, 88.0, 91.2]
    })
    
    try:
        result = loader.load_data(df_test, "students")
        print("Load Result:", json.dumps(result, indent=2))
    except Exception as e:
        print("Load Failed:", str(e))
