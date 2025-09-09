"""
CSV Schema Validator
====================

Automated CSV schema validation framework for preventing data regressions 
in scraper outputs and data processing pipelines.

This package provides:
- Comprehensive schema definitions for CSV files
- Robust validation engine with detailed error reporting  
- CLI tools for manual validation
- Integration helpers for CI/CD pipelines
- Zero external dependencies (Python standard library only)
"""

from .schemas import LISTING_SCHEMA, SELLER_SCHEMA, DataType, SchemaField
from .test_runner import SchemaTestRunner
from .validators import CSVSchemaValidator, ValidationIssue, ValidationResult

__all__ = [
    # Schema definitions
    "DataType",
    "SchemaField", 
    "LISTING_SCHEMA",
    "SELLER_SCHEMA",
    
    # Validation classes
    "ValidationIssue",
    "ValidationResult", 
    "CSVSchemaValidator",
    "SchemaTestRunner",
]

__version__ = "1.0.0"
