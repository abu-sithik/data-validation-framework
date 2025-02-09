import ibis
from .base import DatabaseConnector
from ..config.settings import DuckDBSettings
import pandas as pd
from typing import Optional, Dict
from ..core.exceptions import DatabaseError

class DuckDBConnector(DatabaseConnector):
    def __init__(self, settings: DuckDBSettings):
        self.settings = settings
        self.connection = None
    
    def connect(self):
        try:
            self.connection = ibis.duckdb.connect(database=self.settings.file_path)
            return self.connection
        except Exception as e:
            raise DatabaseError(f"Failed to connect to DuckDB: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        try:
            if params:
                query = query.format(**params)
            result = self.connection.sql(query).execute()
            return pd.DataFrame(result)
        except Exception as e:
            raise DatabaseError(f"Failed to execute DuckDB query: {str(e)}")
    
    def close(self):
        if self.connection:
            self.connection.close()
