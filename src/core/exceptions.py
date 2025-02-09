class ValidationError(Exception):
    """Base exception for validation errors."""
    pass

class DatabaseError(Exception):
    """Base exception for database errors."""
    pass

class ConfigurationError(Exception):
    """Base exception for configuration errors."""
    pass