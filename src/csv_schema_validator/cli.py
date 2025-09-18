#!/usr/bin/env python3
"""
Simple CLI for testing the schema validation framework
"""

import argparse
import sys
from pathlib import Path

from .schemas import LISTING_SCHEMA, SELLER_SCHEMA
from .validators import CSVSchemaValidator


def main():
    """Simple CLI entry point for testing."""
    parser = argparse.ArgumentParser(
        description="Simple CSV Schema Validation Tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "command", choices=["validate", "info"], help="Command to execute"
    )

    parser.add_argument(
        "--schema-type",
        choices=["seller", "listing"],
        required=True,
        help="Schema to use for validation",
    )

    parser.add_argument(
        "--file", type=Path, help="CSV file to validate (for validate command)"
    )

    args = parser.parse_args()

    # Get schema
    schema = SELLER_SCHEMA if args.schema_type == "seller" else LISTING_SCHEMA
    schema_name = args.schema_type.title()

    if args.command == "info":
        print(f"📋 {schema_name} Schema Information")
        print("=" * 50)
        print(f"Total Fields: {len(schema)}")
        print(f"Required Fields: {sum(1 for field in schema if field.required)}")
        print(f"Optional Fields: {sum(1 for field in schema if not field.required)}")
        print("\nFields:")

        for i, field in enumerate(schema, 1):
            required_marker = "✓" if field.required else "○"
            nullable_marker = "(nullable)" if field.nullable else ""
            print(
                f"  {i:2d}. {required_marker} {field.name:<30} [{field.data_type.value}] {nullable_marker}"
            )

        return 0

    elif args.command == "validate":
        if not args.file:
            print("❌ Error: --file is required for validate command", file=sys.stderr)
            return 1

        if not args.file.exists():
            print(f"❌ Error: File not found: {args.file}", file=sys.stderr)
            return 1

        print(f"🔍 Validating {args.file} against {schema_name} schema...")

        validator = CSVSchemaValidator(schema)
        validator.schema_name = args.schema_type

        result = validator.validate_file(args.file)

        print(f"\n📊 Validation Results for {result.file_path}")
        print("=" * 60)
        print(f"Status: {'✅ PASSED' if result.is_valid else '❌ FAILED'}")
        print(f"Total Rows: {result.total_rows}")
        print(f"Total Columns: {result.total_columns}")
        print(f"Execution Time: {result.execution_time_ms:.2f}ms")

        if result.issues:
            print(f"\n🚨 Issues Found ({len(result.issues)} total):")
            print(f"  - Errors: {result.error_count}")
            print(f"  - Warnings: {result.warning_count}")
            print(f"  - Info: {result.info_count}")

            # Group issues by severity
            errors = [issue for issue in result.issues if issue.severity == "error"]
            warnings = [issue for issue in result.issues if issue.severity == "warning"]

            if errors:
                print("\n❌ ERRORS:")
                for error in errors[:5]:  # Show first 5 errors
                    row_info = f" (row {error.row_number})" if error.row_number else ""
                    print(f"  • {error.field_name}: {error.description}{row_info}")
                if len(errors) > 5:
                    print(f"  ... and {len(errors) - 5} more errors")

            if warnings:
                print("\n⚠️  WARNINGS:")
                for warning in warnings[:3]:  # Show first 3 warnings
                    print(f"  • {warning.field_name}: {warning.description}")
                if len(warnings) > 3:
                    print(f"  ... and {len(warnings) - 3} more warnings")

        if result.missing_columns:
            print(f"\n🔍 Missing Required Columns ({len(result.missing_columns)}):")
            for col in result.missing_columns:
                print(f"  - {col}")

        if result.extra_columns:
            print(f"\n➕ Extra Columns Found ({len(result.extra_columns)}):")
            for col in result.extra_columns:
                print(f"  - {col}")

        if result.is_valid:
            print("\n🎉 Validation completed successfully!")
            return 0
        else:
            print("\n💥 Validation failed - please fix the issues above")
            return 1


if __name__ == "__main__":
    sys.exit(main())
