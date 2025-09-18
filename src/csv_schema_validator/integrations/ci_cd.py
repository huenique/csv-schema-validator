"""
CI/CD Integration Helpers
=========================

Helper functions for integrating CSV validation into CI/CD pipelines.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from ..validators import ValidationResult


class GitHubActionsReporter:
    """GitHub Actions specific reporter for validation results."""

    def __init__(self) -> None:
        """Initialize GitHub Actions reporter."""
        self.is_github_actions = os.getenv("GITHUB_ACTIONS") == "true"

    def report_results(self, results: Dict[str, ValidationResult]) -> None:
        """
        Report validation results in GitHub Actions format.

        Args:
            results: Dictionary mapping file paths to validation results
        """
        total_files = len(results)
        passed_files = sum(1 for r in results.values() if r.is_valid)
        failed_files = total_files - passed_files

        # Create summary
        if self.is_github_actions:
            print(
                f"::notice::CSV Validation Summary: {passed_files}/{total_files} files passed"
            )

        print(f"📊 CSV Validation Summary")
        print(f"  Total Files: {total_files}")
        print(f"  ✅ Passed: {passed_files}")
        print(f"  ❌ Failed: {failed_files}")

        # Report individual file results
        for file_path, result in results.items():
            if result.is_valid:
                if self.is_github_actions:
                    print(
                        f"::notice file={file_path}::✅ Validation passed ({result.total_rows} rows)"
                    )
                print(f"✅ {file_path}: PASSED ({result.total_rows} rows)")
            else:
                if self.is_github_actions:
                    print(
                        f"::error file={file_path}::❌ Validation failed - {result.error_count} errors, {result.warning_count} warnings"
                    )

                print(f"❌ {file_path}: FAILED")
                print(
                    f"  Errors: {result.error_count}, Warnings: {result.warning_count}"
                )

                # Show key issues
                for issue in result.issues[:3]:
                    if self.is_github_actions and issue.severity == "error":
                        row_info = (
                            f" at line {issue.row_number}" if issue.row_number else ""
                        )
                        print(
                            f"::error file={file_path}{row_info}::{issue.field_name}: {issue.description}"
                        )
                    print(f"    - {issue.field_name}: {issue.description}")

        # Exit with error if any validation failed
        if failed_files > 0:
            if self.is_github_actions:
                print(f"::error::CSV validation failed for {failed_files} files")
            sys.exit(1)


def generate_ci_report(
    results: Dict[str, ValidationResult], output_path: Path | None = None
) -> Dict[str, Any]:
    """
    Generate CI-friendly validation report.

    Args:
        results: Validation results
        output_path: Optional path to save JSON report

    Returns:
        Report data dictionary
    """
    total_files = len(results)
    passed_files = sum(1 for r in results.values() if r.is_valid)
    failed_files = total_files - passed_files
    total_errors = sum(r.error_count for r in results.values())
    total_warnings = sum(r.warning_count for r in results.values())

    report: Dict[str, Any] = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_files": total_files,
            "passed_files": passed_files,
            "failed_files": failed_files,
            "success_rate": (passed_files / total_files * 100)
            if total_files > 0
            else 0,
            "total_errors": total_errors,
            "total_warnings": total_warnings,
        },
        "files": [],
    }

    for file_path, result in results.items():
        file_report: Dict[str, Any] = {
            "file_path": file_path,
            "schema_type": result.schema_name,
            "status": "passed" if result.is_valid else "failed",
            "total_rows": result.total_rows,
            "total_columns": result.total_columns,
            "error_count": result.error_count,
            "warning_count": result.warning_count,
            "execution_time_ms": result.execution_time_ms,
            "issues": [],
        }

        # Add issues details
        for issue in result.issues:
            issue_data: Dict[str, Any] = {
                "field_name": issue.field_name,
                "issue_type": issue.issue_type,
                "severity": issue.severity,
                "description": issue.description,
                "row_number": issue.row_number,
                "actual_value": issue.actual_value,
                "expected_value": issue.expected_value,
                "suggestion": issue.suggestion,
            }
            file_report["issues"].append(issue_data)

        report["files"].append(file_report)

    # Save to file if requested
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2))
        print(f"📄 CI report saved to: {output_path}")

    return report


def create_github_workflow(output_path: Path | None = None) -> str:
    """
    Create GitHub Actions workflow for CSV validation.

    Args:
        output_path: Optional path to save workflow file

    Returns:
        Workflow YAML content
    """
    workflow_content = """name: CSV Schema Validation

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]
  schedule:
    # Run validation daily at 2 AM UTC
    - cron: '0 2 * * *'

jobs:
  validate-csv:
    name: Validate CSV Outputs
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
      
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.8'
        
    - name: Install CSV Schema Validator
      run: |
        pip install git+https://github.com/huenique/csv-schema-validator.git
        
    - name: Validate Seller CSV files
      run: |
        find . -name "*Seller_rows*.csv" -print0 | while IFS= read -r -d '' file; do
          echo "Validating Seller CSV: $file"
          csv-schema-validator validate --schema-type seller --file "$file"
        done
        
    - name: Validate Listing CSV files  
      run: |
        find . -name "*Listing_rows*.csv" -print0 | while IFS= read -r -d '' file; do
          echo "Validating Listing CSV: $file"
          csv-schema-validator validate --schema-type listing --file "$file"
        done
        
    - name: Generate validation report
      if: always()
      run: |
        python -c "
        from ..integrations.ci_cd import generate_ci_report
        from ..test_runner import SchemaTestRunner
        import sys
        
        try:
            runner = SchemaTestRunner('.')
            results = runner.discover_and_validate_files('.')
            report = generate_ci_report(results, Path('validation-report.json'))
            print('Validation report generated successfully')
        except Exception as e:
            print(f'Error generating report: {e}')
            sys.exit(0)  # Don't fail the job for report generation
        "
        
    - name: Upload validation report
      if: always()
      uses: actions/upload-artifact@v4
      with:
        name: csv-validation-report
        path: validation-report.json
        retention-days: 30
        
    - name: Comment PR with results
      if: github.event_name == 'pull_request' && always()
      uses: actions/github-script@v7
      with:
        script: |
          const fs = require('fs');
          
          try {
            const reportData = fs.readFileSync('validation-report.json', 'utf8');
            const report = JSON.parse(reportData);
            
            const summary = report.summary;
            const passed = summary.passed_files;
            const failed = summary.failed_files;
            const total = summary.total_files;
            
            const comment = `## 📊 CSV Schema Validation Results
            
            - **Total Files**: ${total}
            - **✅ Passed**: ${passed}
            - **❌ Failed**: ${failed}
            - **Success Rate**: ${summary.success_rate.toFixed(1)}%
            
            ${failed > 0 ? '⚠️ Some CSV files failed validation. Please review the detailed report.' : '🎉 All CSV files passed validation!'}
            `;
            
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: comment
            });
          } catch (error) {
            console.log('Error posting comment:', error);
          }
"""

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(workflow_content)
        print(f"📄 GitHub workflow created: {output_path}")

    return workflow_content


def setup_ci_integration(project_directory: Path) -> None:
    """
    Set up complete CI/CD integration for a project.

    Args:
        project_directory: Path to project root
    """
    print(f"🔧 Setting up CI/CD integration for: {project_directory.name}")

    # Create .github/workflows directory
    workflows_dir = project_directory / ".github" / "workflows"
    workflows_dir.mkdir(parents=True, exist_ok=True)

    # Create workflow file
    workflow_path = workflows_dir / "csv-validation.yml"
    create_github_workflow(workflow_path)

    # Create local validation script
    validation_script = project_directory / "validate_csv.py"
    script_content = '''#!/usr/bin/env python3
"""
Local CSV validation script for development.
"""

from pathlib import Path
from ..integrations.ci_cd import GitHubActionsReporter, generate_ci_report
from ..test_runner import SchemaTestRunner


def main() -> None:
    """Run local CSV validation."""
    project_root = Path(__file__).parent
    runner = SchemaTestRunner(str(project_root))
    
    # Discover and validate CSV files
    results = runner.discover_and_validate_files(str(project_root))
    
    # Generate report
    report = generate_ci_report(results, project_root / "validation-report.json")
    
    # Use GitHub Actions reporter for consistent output
    reporter = GitHubActionsReporter()
    reporter.report_results(results)


if __name__ == "__main__":
    main()
'''

    validation_script.write_text(script_content)
    validation_script.chmod(0o755)

    print("✅ CI/CD integration setup complete!")
    print("📋 Created files:")
    print(f"  - {workflow_path}")
    print(f"  - {validation_script}")
    print()
    print("🚀 Next steps:")
    print("1. Commit and push the workflow file")
    print("2. CSV validation will run on every push/PR")
    print("3. Use ./validate_csv.py for local testing")
