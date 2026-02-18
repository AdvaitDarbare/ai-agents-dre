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
import requests
import pandas as pd
from typing import Dict, Any
from urllib.parse import urlparse, urlunparse

try:
    import pymysql  # type: ignore
except Exception:  # pragma: no cover
    pymysql = None

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
        self.query_port = int(os.getenv("DORIS_FE_QUERY_PORT", "9030"))
        self.auto_create_table = os.getenv("DORIS_AUTO_CREATE_TABLE", "0").strip() in {"1", "true", "True"}
        self.redirect_host = os.getenv("DORIS_STREAM_LOAD_REDIRECT_HOST", "").strip()
        self.redirect_port = os.getenv("DORIS_STREAM_LOAD_REDIRECT_PORT", "").strip()

    @staticmethod
    def _is_private_ipv4(hostname: str) -> bool:
        if not hostname:
            return False
        return (
            hostname.startswith("10.")
            or hostname.startswith("192.168.")
            or hostname.startswith("172.")
        )

    def _resolve_redirect_url(self, location: str) -> str:
        parsed = urlparse(location)
        if not parsed.hostname:
            return location

        target_host = parsed.hostname
        target_port = parsed.port

        if self.redirect_host:
            target_host = self.redirect_host
            if self.redirect_port:
                target_port = int(self.redirect_port)
        elif self.host in {"127.0.0.1", "localhost"} and self._is_private_ipv4(parsed.hostname):
            # Local dockerized Doris often redirects to private bridge IPs that are not host-routable.
            target_host = self.host

        if target_port is None:
            return location

        rewritten = parsed._replace(netloc=f"{target_host}:{target_port}")
        return urlunparse(rewritten)

    @staticmethod
    def _map_dtype(dtype: Any) -> str:
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):
            return "DOUBLE"
        if pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        # Doris rejects STRING type for key columns; VARCHAR works for ids and dimensions.
        return "VARCHAR(255)"

    @staticmethod
    def _safe_identifier(name: str) -> str:
        return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in str(name))

    def _ensure_table(self, df: pd.DataFrame, table_name: str) -> None:
        if pymysql is None:
            raise Exception(
                "DORIS_AUTO_CREATE_TABLE requires PyMySQL. Install with `pip install pymysql`."
            )
        if df is None or len(df.columns) == 0:
            raise Exception("Cannot auto-create Doris table from empty dataframe.")

        safe_table = self._safe_identifier(table_name)
        safe_db = self._safe_identifier(self.db)
        column_defs = []
        for col in df.columns:
            safe_col = self._safe_identifier(str(col))
            doris_type = self._map_dtype(df[col].dtype)
            column_defs.append(f"`{safe_col}` {doris_type} NULL")

        preferred_key = None
        for candidate in df.columns:
            lowered = str(candidate).lower()
            if lowered == "id" or lowered.endswith("_id"):
                preferred_key = str(candidate)
                break
        if preferred_key is None:
            preferred_key = str(df.columns[0])
        safe_key = self._safe_identifier(preferred_key)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{safe_db}`.`{safe_table}` (
            {", ".join(column_defs)}
        )
        DUPLICATE KEY(`{safe_key}`)
        DISTRIBUTED BY HASH(`{safe_key}`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        conn = pymysql.connect(
            host=self.host,
            port=self.query_port,
            user=self.user,
            password=self.password,
            database="information_schema",
            connect_timeout=5,
            autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS `{safe_db}`")
                cur.execute(create_sql)
        finally:
            conn.close()

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
                "success": True,
                "Status": "Success",
                "Message": "Mock load successful",
                "NumberTotalRows": len(df),
                "NumberLoadedRows": len(df),
                "LoadUrl": "http://mock-doris/api/_stream_load"
            }

        safe_table = self._safe_identifier(table_name)
        if self.auto_create_table:
            self._ensure_table(df=df, table_name=safe_table)
            
        # Prepare Data
        # Convert to CSV string without header and index
        csv_data = df.to_csv(index=False, header=False)
        
        # Prepare Request
        load_url = f"http://{self.host}:{self.port}/api/{self.db}/{safe_table}/_stream_load"
        label = f"label_{uuid.uuid4()}"
        safe_columns = [self._safe_identifier(str(col)) for col in df.columns]
        
        headers = {
            "Expect": "100-continue",
            "label": label,
            "column_separator": ",",
            "format": "csv",
            "columns": ",".join(safe_columns),
            "strict_mode": os.getenv("DORIS_STREAM_LOAD_STRICT_MODE", "false"),
            # Add other headers like 'columns' if mapping is needed
        }
        
        auth = (self.user, self.password)
        
        print(f"🚀 Loading {len(df)} rows into '{self.db}.{table_name}' via Stream Load...")
        
        try:
            response = requests.put(
                load_url,
                data=csv_data,
                headers=headers,
                auth=auth,
                allow_redirects=False,
                timeout=float(os.getenv("DORIS_STREAM_LOAD_TIMEOUT_SECONDS", "30")),
            )

            if response.status_code in {301, 302, 307, 308}:
                location = response.headers.get("Location") or response.headers.get("location")
                if not location:
                    raise Exception("Doris returned redirect without Location header.")
                redirect_url = self._resolve_redirect_url(location)
                response = requests.put(
                    redirect_url,
                    data=csv_data,
                    headers=headers,
                    auth=auth,
                    allow_redirects=False,
                    timeout=float(os.getenv("DORIS_STREAM_LOAD_TIMEOUT_SECONDS", "30")),
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
                
            resp_dict["success"] = True
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
