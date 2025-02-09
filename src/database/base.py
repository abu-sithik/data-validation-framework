from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Any, Dict, List

class DatabaseConnector(ABC):
    @abstractmethod
    def connect(self) -> Any:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close database connection."""
        pass