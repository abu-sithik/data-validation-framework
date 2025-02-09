from abc import ABC, abstractmethod
from typing import List, Dict, Any
import csv
import json
import pandas as pd

class ValidationResult:
    def __init__(self, metric: str, status: str, details: Dict[str, Any]):
        self.metric = metric
        self.status = status
        self.details = details

class ResultHandler(ABC):
    @abstractmethod
    def handle_result(self, result: ValidationResult) -> None:
        pass

class CSVResultHandler(ResultHandler):
    def __init__(self, filename: str):
        self.filename = filename
    
    def handle_result(self, results: List[ValidationResult]) -> None:
        with open(self.filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['metric', 'status', 'details'])
            writer.writeheader()
            for result in results:
                writer.writerow({
                    'metric': result.metric,
                    'status': result.status,
                    'details': json.dumps(result.details)
                })

class JSONResultHandler(ResultHandler):
    def __init__(self, filename: str):
        self.filename = filename
    
    def handle_result(self, results: List[ValidationResult]) -> None:
        with open(self.filename, 'w') as f:
            json.dump([vars(r) for r in results], f, indent=2)