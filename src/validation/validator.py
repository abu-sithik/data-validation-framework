from typing import Dict, Any, List
import pandas as pd
from ..database.base import DatabaseConnector
from .strategies import ValidationStrategy

class DataValidator:
    def __init__(
        self,
        source_db: DatabaseConnector,
        target_db: DatabaseConnector,
        validation_strategy: ValidationStrategy,
        batch_size: int = 25000
    ):
        self.source_db = source_db
        self.target_db = target_db
        self.validation_strategy = validation_strategy
        self.batch_size = batch_size
    
    def validate_query(self, source_query: str, target_query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        try:
            source_data = self.source_db.execute_query(source_query, params)
            target_data = self.target_db.execute_query(target_query, params)
            
            # Align columns
            common_columns = source_data.columns.intersection(target_data.columns)
            source_data = source_data[common_columns]
            target_data = target_data[common_columns]
            
            # Validate data
            result = self.validation_strategy.validate(source_data, target_data)
            
            return {
                'status': result['status'],
                'details': result['differences'] if result['status'] == 'fail' else {},
                'source_rows': len(source_data),
                'target_rows': len(target_data)
            }
        except Exception as e:
            raise ValidationError(f"Validation failed: {str(e)}")