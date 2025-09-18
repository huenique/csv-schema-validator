"""
Monitoring CSV Schema Definitions
===============================

This module defines the expected schemas for seller monitoring CSV output files.
These schemas are separate from the main seller/listing schemas and track
monitoring-specific data without modifying existing CSV formats.
"""

from typing import Any, Dict, List

from .schemas import DataType, SchemaField

# Seller Monitoring CSV Schema Definition
SELLER_MONITORING_SCHEMA = [
    SchemaField(
        name="seller_uuid",
        data_type=DataType.UUID,
        required=True,
        nullable=True,
        description="Links to existing seller_uuid in Seller_rows.csv",
        examples=["abc123-def456-789..."],
    ),
    SchemaField(
        name="marketplace",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        default_value="aliexpress",
        description="Marketplace identifier",
        examples=["aliexpress", "alibaba", "dhgate", "tokopedia", "google_shopping"],
    ),
    SchemaField(
        name="monitoring_status",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Current monitoring status",
        examples=["active", "inactive", "blocked", "unknown"],
    ),
    SchemaField(
        name="confidence_score",
        data_type=DataType.FLOAT,
        required=True,
        nullable=True,
        description="Confidence score for status determination (0.0-1.0)",
        examples=["0.75", "0.90", "0.25"],
    ),
    SchemaField(
        name="status_reason",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Reason for current status determination",
        examples=["store_active_with_products", "404_not_found", "blocked_captcha"],
    ),
    SchemaField(
        name="evidence_hash",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Hash of evidence data for change detection",
        examples=["abc123def456", "789xyz012"],
    ),
    SchemaField(
        name="last_checked_at",
        data_type=DataType.DATETIME,
        required=True,
        nullable=True,
        format_pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$",
        description="When this seller was last monitored",
        examples=["2025-09-17 10:30:00.123"],
    ),
    SchemaField(
        name="last_transition_at",
        data_type=DataType.DATETIME,
        required=True,
        nullable=True,
        format_pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$",
        description="When status last changed (null if never changed)",
        examples=["2025-09-15 14:20:00.456", "null"],
    ),
    SchemaField(
        name="check_count",
        data_type=DataType.INTEGER,
        required=True,
        nullable=True,
        description="Total number of monitoring checks performed",
        examples=["1", "15", "100"],
    ),
    SchemaField(
        name="created_at",
        data_type=DataType.DATETIME,
        required=True,
        nullable=True,
        format_pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$",
        description="When monitoring for this seller began",
        examples=["2025-09-10 08:00:00.000"],
    ),
]

# Seller Monitoring Events CSV Schema Definition
SELLER_MONITORING_EVENTS_SCHEMA = [
    SchemaField(
        name="event_uuid",
        data_type=DataType.UUID,
        required=True,
        nullable=True,
        description="Unique identifier for this event",
        examples=["def456-abc123-000..."],
    ),
    SchemaField(
        name="seller_uuid",
        data_type=DataType.UUID,
        required=True,
        nullable=True,
        description="Links to seller_uuid in Seller_rows.csv",
        examples=["abc123-def456-789..."],
    ),
    SchemaField(
        name="marketplace",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        default_value="aliexpress",
        description="Marketplace identifier",
        examples=["aliexpress", "alibaba", "dhgate", "tokopedia", "google_shopping"],
    ),
    SchemaField(
        name="from_status",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Previous monitoring status (null for first check)",
        examples=["active", "inactive", "blocked", "unknown", "null"],
    ),
    SchemaField(
        name="to_status",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="New monitoring status",
        examples=["active", "inactive", "blocked", "unknown"],
    ),
    SchemaField(
        name="from_confidence",
        data_type=DataType.FLOAT,
        required=True,
        nullable=True,
        description="Previous confidence score",
        examples=["0.80", "0.25", "null"],
    ),
    SchemaField(
        name="to_confidence",
        data_type=DataType.FLOAT,
        required=True,
        nullable=True,
        description="New confidence score",
        examples=["0.90", "0.10", "0.75"],
    ),
    SchemaField(
        name="transition_reason",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Why the status changed",
        examples=[
            "confidence_threshold_reached",
            "explicit_closed_marker_found",
            "blocked_detected",
        ],
    ),
    SchemaField(
        name="evidence_hash",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Hash of evidence for this check",
        examples=["abc123def456", "789xyz012"],
    ),
    SchemaField(
        name="created_at",
        data_type=DataType.DATETIME,
        required=True,
        nullable=True,
        format_pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$",
        description="When this event occurred",
        examples=["2025-09-17 10:30:00.123"],
    ),
]


def get_monitoring_schema_by_name(schema_name: str) -> List[SchemaField]:
    """Get monitoring schema definition by name."""
    schemas = {
        "seller_monitoring": SELLER_MONITORING_SCHEMA,
        "seller_monitoring_events": SELLER_MONITORING_EVENTS_SCHEMA,
    }
    return schemas.get(schema_name.lower(), [])


def get_monitoring_column_names(schema: List[SchemaField]) -> List[str]:
    """Extract column names from monitoring schema definition."""
    return [field.name for field in schema]


def get_monitoring_required_columns(schema: List[SchemaField]) -> List[str]:
    """Get list of required column names from monitoring schema."""
    return [field.name for field in schema if field.required and not field.nullable]


def get_monitoring_schema_info() -> Dict[str, Dict[str, Any]]:
    """Get comprehensive information about monitoring schemas."""
    return {
        "seller_monitoring": {
            "total_columns": len(SELLER_MONITORING_SCHEMA),
            "required_columns": len(
                get_monitoring_required_columns(SELLER_MONITORING_SCHEMA)
            ),
            "column_names": get_monitoring_column_names(SELLER_MONITORING_SCHEMA),
            "data_types": {
                field.name: field.data_type.value for field in SELLER_MONITORING_SCHEMA
            },
        },
        "seller_monitoring_events": {
            "total_columns": len(SELLER_MONITORING_EVENTS_SCHEMA),
            "required_columns": len(
                get_monitoring_required_columns(SELLER_MONITORING_EVENTS_SCHEMA)
            ),
            "column_names": get_monitoring_column_names(
                SELLER_MONITORING_EVENTS_SCHEMA
            ),
            "data_types": {
                field.name: field.data_type.value
                for field in SELLER_MONITORING_EVENTS_SCHEMA
            },
        },
    }
