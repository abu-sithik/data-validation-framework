import ibis
from .base import DatabaseConnector
from ..config.settings import DatabaseSettings
import pandas as pd
from typing import Optional, Dict
from ..core.exceptions import DatabaseError

class PostgresConnector(DatabaseConnector):
    def __init__(self, settings: DatabaseSettings):
        self.settings = settings
        self.connection = None
    
    def connect(self):
        try:
            self.connection = ibis.postgres.connect(
                host=self.settings.host,
                port=self.settings.port,
                user=self.settings.user,
                password=self.settings.password,
                database=self.settings.database
            )
            self.connection.raw_sql('SET TRANSACTION READ ONLY;')
            return self.connection
        except Exception as e:
            raise DatabaseError(f"Failed to connect to PostgreSQL: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        try:
            if params:
                query = query.format(**params)
            result = self.connection.raw_sql(query).fetchall()
            columns = [desc[0] for desc in self.connection.raw_sql(query).description]
            return pd.DataFrame(result, columns=columns)
        except Exception as e:
            raise DatabaseError(f"Failed to execute PostgreSQL query: {str(e)}")
    
    def close(self):
        if self.connection:
            self.connection.close()
