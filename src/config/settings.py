from pydantic import BaseSettings, Field
from typing import Optional

class DatabaseSettings(BaseSettings):
    host: str = Field(..., env="DB_HOST")
    port: int = Field(5432, env="DB_PORT")
    database: str = Field(..., env="DB_NAME")
    user: str = Field(..., env="DB_USER")
    password: str = Field(..., env="DB_PASSWORD")
    
    class Config:
        env_prefix = 'DB_'

class DuckDBSettings(BaseSettings):
    file_path: str = Field(..., env="DUCKDB_FILE_PATH")
    download_path: str = Field(..., env="DUCKDB_DOWNLOAD_PATH")
    
    class Config:
        env_prefix = 'DUCKDB_'

class ValidationSettings(BaseSettings):
    batch_size: int = Field(25000, env="VALIDATION_BATCH_SIZE")
    tolerance: float = Field(1e-6, env="VALIDATION_TOLERANCE")
    results_file: str = Field("validation_results.csv", env="VALIDATION_RESULTS_FILE")
    
    class Config:
        env_prefix = 'VALIDATION_'