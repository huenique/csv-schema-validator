"""
Automated Schema Test Runner
============================

Automated test runner for continuous schema validation across all scrapers.
Includes test discovery, execution, reporting, and CI/CD integration.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .schemas import LISTING_SCHEMA, SELLER_SCHEMA, SchemaField
from .validators import BatchValidator, ValidationResult


def get_schema_by_name(schema_name: str) -> Optional[List[SchemaField]]:
    """Get schema by name."""
    schemas = {
        "seller": SELLER_SCHEMA,
        "listing": LISTING_SCHEMA,
    }
    return schemas.get(schema_name.lower())


class SchemaTestRunner:
    """Main test runner for schema validation tests."""

    def __init__(self, workspace_root: Union[str, Path]):
        """Initialize test runner with workspace root directory."""
        self.workspace_root = Path(workspace_root)
        self.batch_validator = BatchValidator()
        self.test_results: Dict[str, Any] = {}

    def discover_csv_files(
        self, patterns: Optional[Dict[str, str]] = None
    ) -> Dict[str, List[Path]]:
        """
        Discover CSV files across all scraper directories.

        Args:
            patterns: Optional dict mapping schema types to file patterns

        Returns:
            Dict mapping schema types to lists of discovered files
        """
        if patterns is None:
            patterns = {
                "seller": "**/Seller_rows*.csv",
                "listing": "**/Listing_rows*.csv",
            }

        discovered_files: dict[str, List[Path]] = {}

        for schema_type, pattern in patterns.items():
            files = list(self.workspace_root.glob(pattern))
            # Filter out files in test directories that might be temporary
            files = [
                f
                for f in files
                if "test_results" not in str(f) and "__pycache__" not in str(f)
            ]
            discovered_files[schema_type] = files

        return discovered_files

    def run_full_validation(self, include_warnings: bool = True) -> Dict[str, Any]:
        """
        Run complete validation across all discovered CSV files.

        Args:
            include_warnings: Whether to include warnings in failure criteria

        Returns:
            Comprehensive test results dictionary
        """
        start_time = datetime.now()
        print("🔍 Starting Schema Validation Tests")
        print("=" * 60)

        # Discover files
        print("📂 Discovering CSV files...")
        discovered_files = self.discover_csv_files()

        total_files = sum(len(files) for files in discovered_files.values())
        print(
            f"Found {total_files} CSV files across {len(discovered_files)} schema types"
        )

        # Run validation for each schema type
        all_results: Dict[str, ValidationResult] = {}
        schema_mapping = {
            "seller": SELLER_SCHEMA,
            "listing": LISTING_SCHEMA,
        }

        for schema_type, files in discovered_files.items():
            if not files:
                continue

            print(f"\n📋 Validating {schema_type} files ({len(files)} files)...")
            schema = schema_mapping.get(schema_type, [])

            if not schema:
                print(f"❌ No schema definition found for type: {schema_type}")
                continue

            # Create file-schema pairs for batch validation
            file_schema_pairs = [(file_path, schema) for file_path in files]
            results = self.batch_validator.validate_files(file_schema_pairs)  # type: ignore

            all_results.update(results)

            # Print summary for this schema type
            passed = sum(1 for r in results.values() if r.is_valid)
            failed = len(results) - passed

            print(f"   ✅ Passed: {passed}")
            print(f"   ❌ Failed: {failed}")

        # Generate comprehensive results
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        test_summary = self._generate_test_summary(
            all_results, execution_time, include_warnings
        )

        # Print final summary
        self._print_final_summary(test_summary)

        return test_summary

    def run_directory_validation(
        self, directory: Union[str, Path]
    ) -> Dict[str, ValidationResult]:
        """
        Run validation for all CSV files in a specific directory.

        Args:
            directory: Directory path to validate

        Returns:
            Dict mapping file paths to validation results
        """
        directory = Path(directory)

        if not directory.exists():
            print(f"❌ Directory not found: {directory}")
            return {}

        print(f"🔍 Validating CSV files in: {directory}")

        schema_mapping = {
            "seller": SELLER_SCHEMA,
            "listing": LISTING_SCHEMA,
        }

        results = self.batch_validator.validate_directory(directory, schema_mapping)

        print(f"📊 Validated {len(results)} files")

        return results

    def run_single_file_validation(
        self, file_path: Union[str, Path], schema_type: str
    ) -> ValidationResult:
        """
        Run validation for a single CSV file.

        Args:
            file_path: Path to CSV file
            schema_type: Type of schema ('seller' or 'listing')

        Returns:
            Validation result for the file
        """
        file_path = Path(file_path)
        schema = get_schema_by_name(schema_type)

        if schema is None:
            raise ValueError(f"Unknown schema type: {schema_type}")

        from .validators import CSVSchemaValidator

        validator = CSVSchemaValidator(schema)
        validator.schema_name = schema_type

        return validator.validate_file(file_path)

    def run_regression_test(
        self, baseline_results_file: Optional[Union[str, Path]] = None
    ) -> Dict[str, Any]:
        """
        Run regression tests by comparing current results with baseline.

        Args:
            baseline_results_file: Path to baseline results JSON file

        Returns:
            Regression test results
        """
        current_results = self.run_full_validation(include_warnings=False)

        if baseline_results_file:
            baseline_results = self._load_baseline_results(baseline_results_file)
            regression_analysis = self._analyze_regression(
                baseline_results, current_results
            )
            current_results["regression_analysis"] = regression_analysis

        return current_results

    def save_results_as_baseline(
        self, results: Dict[str, Any], output_file: Union[str, Path]
    ) -> None:
        """
        Save test results as a baseline for future regression testing.

        Args:
            results: Test results to save
            output_file: Path to save baseline results
        """
        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Prepare results for JSON serialization
        serializable_results = self._make_serializable(results)

        with open(output_file, "w") as f:
            json.dump(serializable_results, f, indent=2, default=str)

        print(f"💾 Baseline results saved to: {output_file}")

    def generate_detailed_report(
        self,
        results: Dict[str, Any],
        output_file: Union[str, Path],
        format_type: str = "html",
    ) -> None:
        """
        Generate detailed validation report.

        Args:
            results: Test results to report on
            output_file: Path to save report
            format_type: Report format ('html', 'markdown', or 'json')
        """
        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if format_type == "html":
            report_content = self._generate_html_report(results)
        elif format_type == "markdown":
            report_content = self._generate_markdown_report(results)
        elif format_type == "json":
            report_content = json.dumps(
                self._make_serializable(results), indent=2, default=str
            )
        else:
            raise ValueError(f"Unsupported report format: {format_type}")

        with open(output_file, "w") as f:
            f.write(report_content)

        print(f"📄 Detailed report saved to: {output_file}")

    def _generate_test_summary(
        self,
        results: Dict[str, ValidationResult],
        execution_time: float,
        include_warnings: bool = True,
    ) -> Dict[str, Any]:
        """Generate comprehensive test summary."""
        total_files = len(results)

        # Count results by status
        passed_files: List[str] = []
        failed_files: List[str] = []

        total_errors = 0
        total_warnings = 0
        total_info = 0

        for file_path, result in results.items():
            total_errors += result.error_count
            total_warnings += result.warning_count
            total_info += result.info_count

            if result.is_valid and (not include_warnings or result.warning_count == 0):
                passed_files.append(file_path)
            else:
                failed_files.append(file_path)

        # Analyze issues by type
        issue_types: Dict[str, int] = {}
        missing_columns_summary: Dict[str, int] = {}
        extra_columns_summary: Dict[str, int] = {}

        for result in results.values():
            for issue in result.issues:
                issue_type = issue.issue_type
                issue_types[issue_type] = issue_types.get(issue_type, 0) + 1

            for col in result.missing_columns:
                missing_columns_summary[col] = missing_columns_summary.get(col, 0) + 1

            for col in result.extra_columns:
                extra_columns_summary[col] = extra_columns_summary.get(col, 0) + 1

        return {
            "summary": {
                "total_files": total_files,
                "passed_files": len(passed_files),
                "failed_files": len(failed_files),
                "execution_time_seconds": execution_time,
                "success_rate": (len(passed_files) / total_files * 100)
                if total_files > 0
                else 0,
            },
            "issue_counts": {
                "total_errors": total_errors,
                "total_warnings": total_warnings,
                "total_info": total_info,
            },
            "issue_analysis": {
                "issue_types": issue_types,
                "missing_columns": missing_columns_summary,
                "extra_columns": extra_columns_summary,
            },
            "detailed_results": results,
            "passed_files": passed_files,
            "failed_files": failed_files,
            "timestamp": datetime.now().isoformat(),
        }

    def _print_final_summary(self, test_summary: Dict[str, Any]) -> None:
        """Print final test summary to console."""
        summary = test_summary["summary"]
        issues = test_summary["issue_counts"]

        print("\n" + "=" * 60)
        print("📊 FINAL TEST RESULTS")
        print("=" * 60)

        print(f"Total files validated: {summary['total_files']}")
        print(f"✅ Passed: {summary['passed_files']}")
        print(f"❌ Failed: {summary['failed_files']}")
        print(f"📈 Success rate: {summary['success_rate']:.1f}%")
        print(f"⏱️  Execution time: {summary['execution_time_seconds']:.2f}s")

        print(f"\n🚨 Issue Summary:")
        print(f"   Errors: {issues['total_errors']}")
        print(f"   Warnings: {issues['total_warnings']}")
        print(f"   Info: {issues['total_info']}")

        if test_summary["failed_files"]:
            print(f"\n❌ Failed files:")
            for file_path in test_summary["failed_files"][:5]:  # Show first 5
                result = test_summary["detailed_results"][file_path]
                print(
                    f"   • {Path(file_path).name} ({result.error_count} errors, {result.warning_count} warnings)"
                )

            if len(test_summary["failed_files"]) > 5:
                print(f"   ... and {len(test_summary['failed_files']) - 5} more")

        # Final status
        if summary["failed_files"] == 0:
            print(f"\n🎉 ALL TESTS PASSED!")
        else:
            print(f"\n⚠️  {summary['failed_files']} FILES FAILED VALIDATION")

        print("=" * 60)

    def _load_baseline_results(self, baseline_file: Union[str, Path]) -> Dict[str, Any]:
        """Load baseline results from file."""
        baseline_file = Path(baseline_file)

        if not baseline_file.exists():
            print(f"⚠️  Baseline file not found: {baseline_file}")
            return {}

        try:
            with open(baseline_file, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Error loading baseline file: {e}")
            return {}

    def _analyze_regression(
        self, baseline: Dict[str, Any], current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze regression between baseline and current results."""
        if not baseline:
            return {
                "status": "no_baseline",
                "message": "No baseline available for comparison",
            }

        baseline_summary = baseline.get("summary", {})
        current_summary = current.get("summary", {})

        # Compare key metrics
        regression_analysis: Dict[str, Any] = {
            "files_comparison": {
                "baseline_total": baseline_summary.get("total_files", 0),
                "current_total": current_summary.get("total_files", 0),
                "baseline_passed": baseline_summary.get("passed_files", 0),
                "current_passed": current_summary.get("passed_files", 0),
            },
            "success_rate_change": current_summary.get("success_rate", 0)
            - baseline_summary.get("success_rate", 0),
            "new_failures": [],
            "resolved_failures": [],
            "status": "unknown",
        }

        baseline_failed = set(baseline.get("failed_files", []))
        current_failed = set(current.get("failed_files", []))

        regression_analysis["new_failures"] = list(current_failed - baseline_failed)
        regression_analysis["resolved_failures"] = list(
            baseline_failed - current_failed
        )

        # Determine regression status
        if regression_analysis["new_failures"]:
            regression_analysis["status"] = "regression"
        elif regression_analysis["resolved_failures"]:
            regression_analysis["status"] = "improvement"
        else:
            regression_analysis["status"] = "stable"

        return regression_analysis

    def _make_serializable(self, obj: Any) -> Any:
        """Make object JSON serializable."""
        if isinstance(obj, ValidationResult):
            return {
                "file_path": obj.file_path,
                "schema_name": obj.schema_name,
                "is_valid": obj.is_valid,
                "total_rows": obj.total_rows,
                "total_columns": obj.total_columns,
                "error_count": obj.error_count,
                "warning_count": obj.warning_count,
                "info_count": obj.info_count,
                "missing_columns": obj.missing_columns,
                "extra_columns": obj.extra_columns,
                "validation_timestamp": obj.validation_timestamp.isoformat(),
                "execution_time_ms": obj.execution_time_ms,
                "issues": [self._make_serializable(issue) for issue in obj.issues],
            }
        elif hasattr(obj, "__dict__"):
            return {k: self._make_serializable(v) for k, v in obj.__dict__.items()}  # type: ignore
        elif isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}  # type: ignore
        elif isinstance(obj, (list, tuple)):
            return [self._make_serializable(item) for item in obj]  # type: ignore
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj

    def _generate_html_report(self, results: Dict[str, Any]) -> str:
        """Generate HTML report."""
        summary = results["summary"]
        issues = results["issue_counts"]

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Schema Validation Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .header {{ background: #f4f4f4; padding: 20px; border-radius: 5px; }}
                .summary {{ margin: 20px 0; }}
                .passed {{ color: #28a745; }}
                .failed {{ color: #dc3545; }}
                .warning {{ color: #ffc107; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Schema Validation Report</h1>
                <p>Generated on: {results.get("timestamp", "Unknown")}</p>
            </div>
            
            <div class="summary">
                <h2>Summary</h2>
                <ul>
                    <li>Total files: {summary["total_files"]}</li>
                    <li class="passed">✅ Passed: {summary["passed_files"]}</li>
                    <li class="failed">❌ Failed: {summary["failed_files"]}</li>
                    <li>Success rate: {summary["success_rate"]:.1f}%</li>
                    <li>Execution time: {summary["execution_time_seconds"]:.2f}s</li>
                </ul>
                
                <h3>Issues</h3>
                <ul>
                    <li class="failed">Errors: {issues["total_errors"]}</li>
                    <li class="warning">Warnings: {issues["total_warnings"]}</li>
                    <li>Info: {issues["total_info"]}</li>
                </ul>
            </div>
        </body>
        </html>
        """

        return html

    def _generate_markdown_report(self, results: Dict[str, Any]) -> str:
        """Generate Markdown report."""
        summary = results["summary"]
        issues = results["issue_counts"]

        markdown = f"""# 📊 Schema Validation Report

Generated on: {results.get("timestamp", "Unknown")}

## Summary

- **Total files**: {summary["total_files"]}
- **✅ Passed**: {summary["passed_files"]}
- **❌ Failed**: {summary["failed_files"]}
- **Success rate**: {summary["success_rate"]:.1f}%
- **Execution time**: {summary["execution_time_seconds"]:.2f}s

## Issues

- **🚨 Errors**: {issues["total_errors"]}
- **⚠️ Warnings**: {issues["total_warnings"]}
- **ℹ️ Info**: {issues["total_info"]}

"""

        if results.get("failed_files"):
            markdown += "\n## Failed Files\n\n"
            for file_path in results["failed_files"]:
                result = results["detailed_results"][file_path]
                markdown += f"- `{Path(file_path).name}` ({result.error_count} errors, {result.warning_count} warnings)\n"

        return markdown
