"""
CSV Schema Validation Framework (Lightweight Version)
======================================================

Comprehensive validation framework for CSV files using only standard library.
This version doesn't require pandas and works with just the csv module.
"""

import csv
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from .schemas import DataType, SchemaField


@dataclass
class ValidationIssue:
    """Represents a single validation issue."""

    field_name: str
    issue_type: str
    severity: str  # "error", "warning", "info"
    description: str
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    row_number: Optional[int] = None
    suggestion: Optional[str] = None


@dataclass
class ValidationResult:
    """Results of CSV schema validation."""

    file_path: str
    schema_name: str
    is_valid: bool
    total_rows: int
    total_columns: int
    issues: List[ValidationIssue]
    missing_columns: List[str]
    extra_columns: List[str]
    validation_timestamp: datetime
    execution_time_ms: float

    @property
    def error_count(self) -> int:
        """Number of error-level issues."""
        return len([issue for issue in self.issues if issue.severity == "error"])

    @property
    def warning_count(self) -> int:
        """Number of warning-level issues."""
        return len([issue for issue in self.issues if issue.severity == "warning"])

    @property
    def info_count(self) -> int:
        """Number of info-level issues."""
        return len([issue for issue in self.issues if issue.severity == "info"])


class CSVSchemaValidator:
    """Main validator class for CSV schema validation using standard library."""

    def __init__(self, schema: List[SchemaField]):
        """Initialize validator with schema definition."""
        self.schema = schema
        self.schema_fields = {field.name: field for field in schema}
        self.required_columns = {field.name for field in schema if field.required}
        self.schema_name = "unknown"  # Will be set by test runner

    def validate_file(self, file_path: Union[str, Path]) -> ValidationResult:
        """Validate a CSV file against the schema."""
        start_time = datetime.now()
        file_path = Path(file_path)

        if not file_path.exists():
            return ValidationResult(
                file_path=str(file_path),
                schema_name=self.schema_name,
                is_valid=False,
                total_rows=0,
                total_columns=0,
                issues=[
                    ValidationIssue(
                        field_name="file",
                        issue_type="file_not_found",
                        severity="error",
                        description=f"File not found: {file_path}",
                    )
                ],
                missing_columns=[],
                extra_columns=[],
                validation_timestamp=start_time,
                execution_time_ms=0.0,
            )

        try:
            # Load CSV file using standard csv module
            csv_data = []
            csv_columns = []

            with open(file_path, "r", encoding="utf-8", newline="") as csvfile:
                # Auto-detect delimiter
                sample = csvfile.read(1024)
                csvfile.seek(0)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter

                reader = csv.DictReader(csvfile, delimiter=delimiter)
                csv_columns = reader.fieldnames or []
                csv_data = list(reader)

            issues = []

            # Check column structure
            missing_columns = self._check_missing_columns(csv_columns)
            extra_columns = self._check_extra_columns(csv_columns)

            # Add column structure issues
            for col in missing_columns:
                issues.append(
                    ValidationIssue(
                        field_name=col,
                        issue_type="missing_column",
                        severity="error",
                        description=f"Required column '{col}' is missing from CSV",
                        suggestion=f"Add column '{col}' to the CSV file",
                    )
                )

            for col in extra_columns:
                issues.append(
                    ValidationIssue(
                        field_name=col,
                        issue_type="extra_column",
                        severity="warning",
                        description=f"Unexpected column '{col}' found in CSV",
                        suggestion=f"Remove column '{col}' or update schema",
                    )
                )

            # Validate data content
            issues.extend(self._validate_data_content(csv_data))

            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds() * 1000

            return ValidationResult(
                file_path=str(file_path),
                schema_name=self.schema_name,
                is_valid=len([i for i in issues if i.severity == "error"]) == 0,
                total_rows=len(csv_data),
                total_columns=len(csv_columns),
                issues=issues,
                missing_columns=missing_columns,
                extra_columns=extra_columns,
                validation_timestamp=start_time,
                execution_time_ms=execution_time,
            )

        except Exception as e:
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds() * 1000

            return ValidationResult(
                file_path=str(file_path),
                schema_name=self.schema_name,
                is_valid=False,
                total_rows=0,
                total_columns=0,
                issues=[
                    ValidationIssue(
                        field_name="file",
                        issue_type="parse_error",
                        severity="error",
                        description=f"Error parsing CSV file: {str(e)}",
                    )
                ],
                missing_columns=[],
                extra_columns=[],
                validation_timestamp=start_time,
                execution_time_ms=execution_time,
            )

    def _check_missing_columns(self, csv_columns: List[str]) -> List[str]:
        """Check for missing required columns."""
        csv_column_set = set(csv_columns)
        return [col for col in self.required_columns if col not in csv_column_set]

    def _check_extra_columns(self, csv_columns: List[str]) -> List[str]:
        """Check for unexpected columns not in schema."""
        schema_column_set = set(self.schema_fields.keys())
        return [col for col in csv_columns if col not in schema_column_set]

    def _validate_data_content(
        self, csv_data: List[Dict[str, Any]]
    ) -> List[ValidationIssue]:
        """Validate the actual data content against schema rules."""
        issues = []

        # Get all column names that exist in both schema and data
        data_columns = set(csv_data[0].keys()) if csv_data else set()
        common_columns = data_columns.intersection(set(self.schema_fields.keys()))

        for column_name in common_columns:
            field = self.schema_fields[column_name]
            column_data = [row.get(column_name, "") for row in csv_data]
            issues.extend(self._validate_column_data(column_name, column_data, field))

        return issues

    def _validate_column_data(
        self, column_name: str, column_data: List[str], field: SchemaField
    ) -> List[ValidationIssue]:
        """Validate a single column's data against its field definition."""
        issues = []

        for row_idx, value in enumerate(column_data):
            # Check for required but missing values
            if field.required and not field.nullable:
                if (
                    not value
                    or str(value).strip() == ""
                    or str(value).strip().lower() == "null"
                ):
                    issues.append(
                        ValidationIssue(
                            field_name=column_name,
                            issue_type="required_field_missing",
                            severity="error",
                            description=f"Required field '{column_name}' is missing or null",
                            row_number=row_idx
                            + 2,  # +2 because csv index starts at 0 and has header
                            actual_value=str(value),
                            suggestion=f"Provide a valid {field.data_type.value} value",
                        )
                    )
                    continue

            # Validate data types for non-null values
            str_value = str(value).strip()
            if str_value == "" or str_value.lower() == "null":
                continue  # Skip null/empty values

            validation_issues = self._validate_single_value(
                column_name, str_value, field, row_idx + 2
            )
            issues.extend(validation_issues)

        return issues

    def _validate_single_value(
        self, column_name: str, value: str, field: SchemaField, row_number: int
    ) -> List[ValidationIssue]:
        """Validate a single value against field definition."""
        issues = []

        if value == "" or value.lower() == "null":
            return issues  # Handle null/empty values separately

        # Type-specific validation
        if field.data_type == DataType.UUID:
            if not self._is_valid_uuid(value):
                issues.append(
                    ValidationIssue(
                        field_name=column_name,
                        issue_type="invalid_uuid",
                        severity="error",
                        description=f"Invalid UUID format in field '{column_name}'",
                        actual_value=value,
                        row_number=row_number,
                        suggestion="Provide a valid UUID (e.g., '123e4567-e89b-12d3-a456-426614174000')",
                    )
                )

        elif field.data_type == DataType.EMAIL:
            if value.lower() != "null" and not self._is_valid_email(value):
                issues.append(
                    ValidationIssue(
                        field_name=column_name,
                        issue_type="invalid_email",
                        severity="error",
                        description=f"Invalid email format in field '{column_name}'",
                        actual_value=value,
                        row_number=row_number,
                        suggestion="Provide a valid email address (e.g., 'user@example.com')",
                    )
                )

        elif field.data_type == DataType.URL:
            if value.lower() != "null" and not self._is_valid_url(value):
                issues.append(
                    ValidationIssue(
                        field_name=column_name,
                        issue_type="invalid_url",
                        severity="error",
                        description=f"Invalid URL format in field '{column_name}'",
                        actual_value=value,
                        row_number=row_number,
                        suggestion="Provide a valid URL (e.g., 'https://example.com')",
                    )
                )

        elif field.data_type == DataType.DATETIME:
            if not self._is_valid_datetime(value, field.format_pattern):
                issues.append(
                    ValidationIssue(
                        field_name=column_name,
                        issue_type="invalid_datetime",
                        severity="error",
                        description=f"Invalid datetime format in field '{column_name}'",
                        actual_value=value,
                        row_number=row_number,
                        expected_value=field.format_pattern,
                        suggestion="Use format: YYYY-MM-DD HH:MM:SS.mmm",
                    )
                )

        elif field.data_type == DataType.JSON_ARRAY:
            if not self._is_valid_json_array(value):
                issues.append(
                    ValidationIssue(
                        field_name=column_name,
                        issue_type="invalid_json_array",
                        severity="error",
                        description=f"Invalid JSON array format in field '{column_name}'",
                        actual_value=value,
                        row_number=row_number,
                        suggestion="Provide valid JSON array (e.g., '[\"item1\", \"item2\"]' or '[]')",
                    )
                )

        elif field.data_type == DataType.JSON_OBJECT:
            if not self._is_valid_json_object(value):
                issues.append(
                    ValidationIssue(
                        field_name=column_name,
                        issue_type="invalid_json_object",
                        severity="error",
                        description=f"Invalid JSON object format in field '{column_name}'",
                        actual_value=value,
                        row_number=row_number,
                        suggestion="Provide valid JSON object (e.g., '{\"key\": \"value\"}' or '{}')",
                    )
                )

        elif field.data_type == DataType.INTEGER:
            if not self._is_valid_integer(value):
                issues.append(
                    ValidationIssue(
                        field_name=column_name,
                        issue_type="invalid_integer",
                        severity="error",
                        description=f"Invalid integer format in field '{column_name}'",
                        actual_value=value,
                        row_number=row_number,
                        suggestion="Provide a valid integer (e.g., '123', '0', '-456')",
                    )
                )

        elif field.data_type == DataType.FLOAT:
            if not self._is_valid_float(value):
                issues.append(
                    ValidationIssue(
                        field_name=column_name,
                        issue_type="invalid_float",
                        severity="error",
                        description=f"Invalid float format in field '{column_name}'",
                        actual_value=value,
                        row_number=row_number,
                        suggestion="Provide a valid float (e.g., '12.34', '0.0', '-456.78')",
                    )
                )

        elif field.data_type == DataType.BOOLEAN:
            if not self._is_valid_boolean(value):
                issues.append(
                    ValidationIssue(
                        field_name=column_name,
                        issue_type="invalid_boolean",
                        severity="error",
                        description=f"Invalid boolean format in field '{column_name}'",
                        actual_value=value,
                        row_number=row_number,
                        suggestion="Use 'true' or 'false' (lowercase)",
                    )
                )

        return issues

    # Validation helper methods
    def _is_valid_uuid(self, value: str) -> bool:
        """Check if value is a valid UUID."""
        try:
            uuid.UUID(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_valid_email(self, value: str) -> bool:
        """Check if value is a valid email address."""
        email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        return bool(email_pattern.match(value))

    def _is_valid_url(self, value: str) -> bool:
        """Check if value is a valid URL."""
        try:
            result = urlparse(value)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

    def _is_valid_datetime(self, value: str, pattern: Optional[str] = None) -> bool:
        """Check if value is a valid datetime."""
        if pattern:
            try:
                return bool(re.match(pattern, value))
            except re.error:
                pass

        # Try common datetime formats
        common_formats = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in common_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue

        return False

    def _is_valid_json_array(self, value: str) -> bool:
        """Check if value is a valid JSON array."""
        try:
            parsed = json.loads(value)
            return isinstance(parsed, list)
        except (json.JSONDecodeError, TypeError):
            return False

    def _is_valid_json_object(self, value: str) -> bool:
        """Check if value is a valid JSON object."""
        try:
            parsed = json.loads(value)
            return isinstance(parsed, dict)
        except (json.JSONDecodeError, TypeError):
            return False

    def _is_valid_integer(self, value: str) -> bool:
        """Check if value is a valid integer."""
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_valid_float(self, value: str) -> bool:
        """Check if value is a valid float."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_valid_boolean(self, value: str) -> bool:
        """Check if value is a valid boolean."""
        return value.lower() in ["true", "false"]


class BatchValidator:
    """Batch validation for multiple CSV files."""

    def __init__(self):
        """Initialize batch validator."""
        pass

    def validate_directory(
        self,
        directory_path: Union[str, Path],
        schema_mapping: Dict[str, List[SchemaField]],
        file_patterns: Optional[Dict[str, str]] = None,
    ) -> Dict[str, ValidationResult]:
        """
        Validate all CSV files in a directory using appropriate schemas.

        Args:
            directory_path: Directory containing CSV files
            schema_mapping: Dict mapping schema names to schema definitions
            file_patterns: Optional dict mapping schema names to file patterns

        Returns:
            Dict mapping file paths to validation results
        """
        directory_path = Path(directory_path)
        results = {}

        if file_patterns is None:
            file_patterns = {
                "seller": "*Seller_rows*.csv",
                "listing": "*Listing_rows*.csv",
            }

        for schema_name, pattern in file_patterns.items():
            if schema_name not in schema_mapping:
                continue

            schema = schema_mapping[schema_name]
            validator = CSVSchemaValidator(schema)
            validator.schema_name = schema_name

            matching_files = list(directory_path.glob(pattern))

            for file_path in matching_files:
                result = validator.validate_file(file_path)
                results[str(file_path)] = result

        return results

    def validate_files(
        self, file_schema_pairs: List[Tuple[Union[str, Path], List[SchemaField]]]
    ) -> Dict[str, ValidationResult]:
        """
        Validate specific files with their respective schemas.

        Args:
            file_schema_pairs: List of (file_path, schema) pairs

        Returns:
            Dict mapping file paths to validation results
        """
        results = {}

        for file_path, schema in file_schema_pairs:
            validator = CSVSchemaValidator(schema)
            result = validator.validate_file(file_path)
            results[str(file_path)] = result

        return results
