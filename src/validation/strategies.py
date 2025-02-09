from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any

class ValidationStrategy(ABC):
    @abstractmethod
    def validate(self, expected_data: pd.DataFrame, actual_data: pd.DataFrame) -> Dict[str, Any]:
        pass

class NumericValidation(ValidationStrategy):
    def __init__(self, tolerance: float):
        self.tolerance = tolerance
    
    def validate(self, expected_data: pd.DataFrame, actual_data: pd.DataFrame) -> Dict[str, Any]:
        differences = {}
        
        for column in expected_data.columns:
            if pd.api.types.is_numeric_dtype(expected_data[column]):
                diff = (expected_data[column] - actual_data[column]).abs()
                mask = diff > self.tolerance
                if mask.any():
                    differences[column] = {
                        'expected': expected_data.loc[mask, column].to_dict(),
                        'actual': actual_data.loc[mask, column].to_dict()
                    }
        
        return {
            'status': 'fail' if differences else 'pass',
            'differences': differences
        }

class CategoricalValidation(ValidationStrategy):
    def validate(self, expected_data: pd.DataFrame, actual_data: pd.DataFrame) -> Dict[str, Any]:
        differences = {}
        
        for column in expected_data.columns:
            if pd.api.types.is_object_dtype(expected_data[column]):
                mask = expected_data[column] != actual_data[column]
                if mask.any():
                    differences[column] = {
                        'expected': expected_data.loc[mask, column].to_dict(),
                        'actual': actual_data.loc[mask, column].to_dict()
                    }
        
        return {
            'status': 'fail' if differences else 'pass',
            'differences': differences
        }

# src/validation/strategies.py
# ... (previous ValidationStrategy, NumericValidation, and CategoricalValidation classes remain the same)

class DateTimeValidation(ValidationStrategy):
    """Validates datetime fields with optional timezone handling."""
    def __init__(self, timezone_aware: bool = True, allow_time_difference_seconds: int = 0):
        self.timezone_aware = timezone_aware
        self.allowed_difference = pd.Timedelta(seconds=allow_time_difference_seconds)
    
    def validate(self, expected_data: pd.DataFrame, actual_data: pd.DataFrame) -> Dict[str, Any]:
        differences = {}
        
        for column in expected_data.columns:
            if pd.api.types.is_datetime64_any_dtype(expected_data[column]):
                # Convert to timezone-naive if needed
                if not self.timezone_aware:
                    expected_col = pd.DatetimeIndex(expected_data[column]).tz_localize(None)
                    actual_col = pd.DatetimeIndex(actual_data[column]).tz_localize(None)
                else:
                    expected_col = expected_data[column]
                    actual_col = actual_data[column]
                
                # Compare with allowed time difference
                time_diff = abs(expected_col - actual_col)
                mask = time_diff > self.allowed_difference
                
                if mask.any():
                    differences[column] = {
                        'expected': expected_data.loc[mask, column].dt.strftime('%Y-%m-%d %H:%M:%S%z').to_dict(),
                        'actual': actual_data.loc[mask, column].dt.strftime('%Y-%m-%d %H:%M:%S%z').to_dict(),
                        'difference_seconds': time_diff[mask].total_seconds().to_dict()
                    }
        
        return {
            'status': 'fail' if differences else 'pass',
            'differences': differences
        }

class NullValidation(ValidationStrategy):
    """Validates null/missing value patterns."""
    def __init__(self, treat_empty_as_null: bool = True):
        self.treat_empty_as_null = treat_empty_as_null
    
    def validate(self, expected_data: pd.DataFrame, actual_data: pd.DataFrame) -> Dict[str, Any]:
        differences = {}
        
        for column in expected_data.columns:
            expected_nulls = expected_data[column].isna()
            if self.treat_empty_as_null:
                expected_nulls |= (expected_data[column] == '') | (expected_data[column] == 'null/empty')
            
            actual_nulls = actual_data[column].isna()
            if self.treat_empty_as_null:
                actual_nulls |= (actual_data[column] == '') | (actual_data[column] == 'null/empty')
            
            mask = expected_nulls != actual_nulls
            if mask.any():
                differences[column] = {
                    'mismatched_rows': mask.sum(),
                    'expected_null_count': expected_nulls.sum(),
                    'actual_null_count': actual_nulls.sum(),
                    'rows_with_differences': mask[mask].index.tolist()
                }
        
        return {
            'status': 'fail' if differences else 'pass',
            'differences': differences
        }

class DistributionValidation(ValidationStrategy):
    """Validates statistical distribution of numeric columns."""
    def __init__(self, threshold_pct: float = 5.0):
        self.threshold_pct = threshold_pct
    
    def validate(self, expected_data: pd.DataFrame, actual_data: pd.DataFrame) -> Dict[str, Any]:
        differences = {}
        
        for column in expected_data.columns:
            if pd.api.types.is_numeric_dtype(expected_data[column]):
                expected_stats = {
                    'mean': expected_data[column].mean(),
                    'median': expected_data[column].median(),
                    'std': expected_data[column].std(),
                    'min': expected_data[column].min(),
                    'max': expected_data[column].max()
                }
                
                actual_stats = {
                    'mean': actual_data[column].mean(),
                    'median': actual_data[column].median(),
                    'std': actual_data[column].std(),
                    'min': actual_data[column].min(),
                    'max': actual_data[column].max()
                }
                
                # Calculate percentage differences
                stat_differences = {}
                for stat, expected_value in expected_stats.items():
                    actual_value = actual_stats[stat]
                    if expected_value != 0:
                        pct_diff = abs(expected_value - actual_value) / expected_value * 100
                        if pct_diff > self.threshold_pct:
                            stat_differences[stat] = {
                                'expected': expected_value,
                                'actual': actual_value,
                                'difference_pct': pct_diff
                            }
                
                if stat_differences:
                    differences[column] = stat_differences
        
        return {
            'status': 'fail' if differences else 'pass',
            'differences': differences
        }

class PatternValidation(ValidationStrategy):
    """Validates text patterns using regular expressions."""
    def __init__(self, patterns: Dict[str, str]):
        self.patterns = {k: re.compile(v) for k, v in patterns.items()}
    
    def validate(self, expected_data: pd.DataFrame, actual_data: pd.DataFrame) -> Dict[str, Any]:
        differences = {}
        
        for column in expected_data.columns:
            if pd.api.types.is_string_dtype(expected_data[column]):
                column_pattern = self.patterns.get(column)
                if column_pattern:
                    expected_matches = expected_data[column].str.match(column_pattern).fillna(False)
                    actual_matches = actual_data[column].str.match(column_pattern).fillna(False)
                    
                    mask = expected_matches != actual_matches
                    if mask.any():
                        differences[column] = {
                            'mismatched_rows': mask.sum(),
                            'invalid_values': actual_data.loc[~actual_matches, column].to_dict()
                        }
        
        return {
            'status': 'fail' if differences else 'pass',
            'differences': differences
        }

class CompositeValidation(ValidationStrategy):
    """Combines multiple validation strategies."""
    def __init__(self, strategies: List[ValidationStrategy]):
        self.strategies = strategies
    
    def validate(self, expected_data: pd.DataFrame, actual_data: pd.DataFrame) -> Dict[str, Any]:
        all_differences = {}
        overall_status = 'pass'
        
        for strategy in self.strategies:
            result = strategy.validate(expected_data, actual_data)
            if result['status'] == 'fail':
                overall_status = 'fail'
                strategy_name = strategy.__class__.__name__
                all_differences[strategy_name] = result['differences']
        
        return {
            'status': overall_status,
            'differences': all_differences
        }