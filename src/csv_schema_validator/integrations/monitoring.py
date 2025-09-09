"""
Monitoring Integration
=====================

Real-time monitoring and alerting for CSV validation results.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import schedule

from ..test_runner import SchemaTestRunner
from ..validators import ValidationResult


class ValidationMonitor:
    """Real-time validation monitoring system."""
    
    def __init__(self, base_directory: str = "./scrapers", 
                 monitoring_interval: int = 60):
        """
        Initialize validation monitor.
        
        Args:
            base_directory: Directory to monitor for CSV files
            monitoring_interval: Check interval in minutes
        """
        self.base_directory = Path(base_directory)
        self.monitoring_interval = monitoring_interval
        self.runner = SchemaTestRunner()
        self.logger = logging.getLogger(__name__)
        
    def start_monitoring(self):
        """Start continuous monitoring of CSV files."""
        schedule.every(self.monitoring_interval).minutes.do(self._run_validation_check)
        
        self.logger.info(f"Started CSV validation monitoring every {self.monitoring_interval} minutes")
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    def _run_validation_check(self):
        """Run validation check and send alerts if needed."""
        try:
            results = self.runner.discover_and_validate(str(self.base_directory))
            
            for file_path, result in results.items():
                if not result.is_valid:
                    self._send_alert(file_path, result)
                    self._log_validation_failure(result)
                else:
                    self._log_validation_success(result)
                    
        except Exception as e:
            self.logger.error(f"Error during validation check: {e}")
    
    def _send_alert(self, file_path: str, result: ValidationResult):
        """Send alert for validation failure."""
        alert_data = {
            "timestamp": datetime.now().isoformat(),
            "file_path": file_path,
            "schema_type": result.schema_name,
            "error_count": result.error_count,
            "warning_count": result.warning_count,
            "issues": [
                {
                    "field": issue.field_name,
                    "type": issue.issue_type,
                    "severity": issue.severity,
                    "description": issue.description,
                    "row": issue.row_number
                }
                for issue in result.issues[:5]  # First 5 issues
            ]
        }
        
        self.logger.warning(f"CSV validation failed: {json.dumps(alert_data, indent=2)}")
        
        # Send to external monitoring systems
        send_to_prometheus({"file_path": result})
        send_to_datadog({"file_path": result})
    
    def _log_validation_success(self, result: ValidationResult):
        """Log successful validation."""
        self.logger.info(f"CSV validation passed: {result.file_path} "
                        f"({result.total_rows} rows, {result.total_columns} columns)")
    
    def _log_validation_failure(self, result: ValidationResult):
        """Log validation failure."""
        self.logger.error(f"CSV validation failed: {result.file_path} "
                         f"- {result.error_count} errors, {result.warning_count} warnings")


def send_to_prometheus(validation_results: Dict[str, ValidationResult]):
    """Send validation metrics to Prometheus."""
    try:
        import requests
        
        metrics = []
        
        for file_path, result in validation_results.items():
            metrics.append({
                'metric': 'csv_validation_status',
                'value': 1 if result.is_valid else 0,
                'labels': {
                    'file_path': file_path,
                    'schema_type': result.schema_name,
                    'error_count': str(result.error_count)
                },
                'timestamp': datetime.now().isoformat()
            })
            
            metrics.append({
                'metric': 'csv_validation_errors',
                'value': result.error_count,
                'labels': {
                    'file_path': file_path,
                    'schema_type': result.schema_name
                },
                'timestamp': datetime.now().isoformat()
            })
        
        # Send to Prometheus pushgateway (if available)
        prometheus_url = "http://pushgateway:9091/metrics"
        response = requests.post(prometheus_url, json=metrics, timeout=10)
        
        if response.status_code == 200:
            logging.info("Successfully sent metrics to Prometheus")
        else:
            logging.warning(f"Failed to send metrics to Prometheus: {response.status_code}")
            
    except ImportError:
        logging.warning("requests not available - skipping Prometheus integration")
    except Exception as e:
        logging.error(f"Error sending metrics to Prometheus: {e}")


def send_to_datadog(validation_results: Dict[str, ValidationResult]):
    """Send validation metrics to DataDog."""
    try:
        # Mock DataDog integration - replace with actual DataDog client
        for file_path, result in validation_results.items():
            # statsd.gauge('csv.validation.status', 
            #             1 if result.is_valid else 0,
            #             tags=[f'file:{file_path}', f'schema:{result.schema_name}'])
            
            # statsd.gauge('csv.validation.errors', 
            #             result.error_count,
            #             tags=[f'file:{file_path}', f'schema:{result.schema_name}'])
            
            logging.info(f"DataDog metrics: csv.validation.status={1 if result.is_valid else 0}, "
                        f"csv.validation.errors={result.error_count} "
                        f"[file:{file_path}, schema:{result.schema_name}]")
                        
    except Exception as e:
        logging.error(f"Error sending metrics to DataDog: {e}")


def create_monitoring_dashboard():
    """Create a simple monitoring dashboard."""
    dashboard_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>CSV Validation Dashboard</title>
        <meta http-equiv="refresh" content="60">
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .header { color: #333; border-bottom: 2px solid #007cba; }
            .status-ok { color: green; }
            .status-error { color: red; }
            .metric { margin: 10px 0; padding: 10px; background: #f5f5f5; border-radius: 5px; }
        </style>
    </head>
    <body>
        <h1 class="header">CSV Schema Validation Dashboard</h1>
        
        <div id="metrics">
            <!-- Metrics will be populated here -->
        </div>
        
        <script>
            // Auto-refresh dashboard data
            function updateMetrics() {
                fetch('/api/validation-metrics')
                    .then(response => response.json())
                    .then(data => {
                        document.getElementById('metrics').innerHTML = renderMetrics(data);
                    });
            }
            
            function renderMetrics(data) {
                return data.map(metric => `
                    <div class="metric">
                        <strong>${metric.file_path}</strong>: 
                        <span class="${metric.status === 'valid' ? 'status-ok' : 'status-error'}">
                            ${metric.status === 'valid' ? '✅ Valid' : '❌ Invalid'}
                        </span>
                        (${metric.error_count} errors)
                    </div>
                `).join('');
            }
            
            // Update every 60 seconds
            setInterval(updateMetrics, 60000);
            updateMetrics(); // Initial load
        </script>
    </body>
    </html>
    """
    
    return dashboard_html
