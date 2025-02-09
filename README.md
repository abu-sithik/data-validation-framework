# Data Validation Framework

A flexible Python framework for validating data consistency between different database systems. This framework provides comprehensive validation strategies and is particularly useful for comparing data between source and target databases.

## Features

- Multiple validation strategies:
  - Numeric validation with configurable tolerance
  - Categorical data validation
  - DateTime validation with timezone support
  - Null/missing value pattern validation
  - Statistical distribution validation
  - Pattern matching validation using regex
  - Composite validation combining multiple strategies
- Support for PostgreSQL and DuckDB
- Batch processing for large datasets
- Configurable validation rules
- Multiple output formats (CSV, JSON)
- Environment-based configuration
- Comprehensive error handling

## Prerequisites

- Python 3.10+
- PostgreSQL
- DuckDB
- Virtual environment (recommended)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/data-validation-framework.git
cd data-validation-framework
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```


2. Update the environment variables in `.env`:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password

DUCKDB_FILE_PATH=/path/to/database.db
DUCKDB_DOWNLOAD_PATH=/path/to/downloads

VALIDATION_BATCH_SIZE=25000
VALIDATION_TOLERANCE=0.000001
```

## Usage

### Basic Validation

```python
from src.validation.validator import DataValidator
from src.validation.strategies import NumericValidation

# Initialize validator
validator = DataValidator(
    source_db=postgres_db,
    target_db=duckdb_db,
    validation_strategy=NumericValidation(tolerance=0.001)
)

# Run validation
result = validator.validate_query(
    source_query="SELECT * FROM source_table",
    target_query="SELECT * FROM target_table"
)
```

### Using Multiple Validation Strategies

```python
from src.validation.strategies import (
    CompositeValidation,
    NumericValidation,
    DateTimeValidation,
    NullValidation
)

# Create composite validator
validator = DataValidator(
    source_db=postgres_db,
    target_db=duckdb_db,
    validation_strategy=CompositeValidation([
        NumericValidation(tolerance=0.001),
        DateTimeValidation(timezone_aware=True),
        NullValidation(treat_empty_as_null=True)
    ])
)
```

### Pattern Validation

```python
from src.validation.strategies import PatternValidation

patterns = {
    'email': r'^[\w\.-]+@[\w\.-]+\.\w+$',
    'phone': r'^\+?1?\d{9,15}$'
}

validator = DataValidator(
    source_db=postgres_db,
    target_db=duckdb_db,
    validation_strategy=PatternValidation(patterns)
)
```

## Project Structure

```
data-validation-framework/
├── src/
│   ├── config/
│   │   └── settings.py
│   ├── database/
│   │   ├── base.py
│   │   ├── postgres.py
│   │   └── duckdb.py
│   ├── validation/
│   │   ├── strategies.py
│   │   └── validator.py
│   └── reporting/
│       └── handlers.py
├── examples/
│   └── basic_validation.py
├── tests/
├── .env.example
├── requirements.txt
└── README.md
```