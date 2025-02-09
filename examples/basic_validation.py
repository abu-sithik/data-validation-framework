# examples/basic_validation.py
import os
from dotenv import load_dotenv
from src.config.settings import DatabaseSettings, DuckDBSettings, ValidationSettings
from src.database.postgres import PostgresConnector
from src.database.duckdb import DuckDBConnector
from src.validation.validator import DataValidator
from src.validation.strategies import NumericValidation
from src.reporting.handlers import CSVResultHandler, ValidationResult

def main():
    # Load environment variables
    load_dotenv()
    
    # Initialize settings
    db_settings = DatabaseSettings()
    duckdb_settings = DuckDBSettings()
    validation_settings = ValidationSettings()
    
    # Initialize database connections
    postgres_db = PostgresConnector(db_settings)
    duckdb_db = DuckDBConnector(duckdb_settings)
    
    try:
        # Connect to databases
        postgres_db.connect()
        duckdb_db.connect()
        
        # Create validator with numeric strategy
        validator = DataValidator(
            source_db=postgres_db,
            target_db=duckdb_db,
            validation_strategy=NumericValidation(tolerance=validation_settings.tolerance),
            batch_size=validation_settings.batch_size
        )
        
        # Example queries
        source_query = """
            SELECT customer_id, total_amount, items_count
            FROM orders
            WHERE created_at >= '{start_date}'
        """
        
        target_query = """
            SELECT customer_id, total_amount, items_count
            FROM orders
            WHERE created_at >= '{start_date}'
        """
        
        # Run validation
        params = {'start_date': '2024-01-01'}
        result = validator.validate_query(source_query, target_query, params)
        
        # Handle results
        handler = CSVResultHandler(validation_settings.results_file)
        handler.handle_result([
            ValidationResult(
                metric="orders_validation",
                status=result['status'],
                details=result['details']
            )
        ])
        
        print(f"Validation completed. Status: {result['status']}")
        print(f"Results written to: {validation_settings.results_file}")
        
    finally:
        # Clean up connections
        postgres_db.close()
        duckdb_db.close()

if __name__ == "__main__":
    main()