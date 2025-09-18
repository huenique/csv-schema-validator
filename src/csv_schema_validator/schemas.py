"""
CSV Schema Definitions
======================

This module defines the expected schemas for all CSV output files
across the tereo scraping ecosystem.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class DataType(Enum):
    """Enumeration of supported data types for validation."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    UUID = "uuid"
    DATETIME = "datetime"
    JSON_ARRAY = "json_array"
    JSON_OBJECT = "json_object"
    URL = "url"
    EMAIL = "email"


@dataclass
class SchemaField:
    """Defines a single CSV column field with validation rules."""

    name: str
    data_type: DataType
    required: bool = True
    nullable: bool = True
    default_value: Optional[Any] = None
    format_pattern: Optional[str] = None
    description: Optional[str] = None
    examples: Optional[List[str]] = None

    def __post_init__(self):
        """Post-initialization validation."""
        # For UUID and other auto-generated fields, we don't require a default value
        # since they are generated during data creation
        auto_generated_types = {DataType.UUID, DataType.DATETIME}

        if (
            not self.nullable
            and self.required
            and self.default_value is None
            and self.data_type not in auto_generated_types
        ):
            raise ValueError(
                f"Field '{self.name}' is required and non-nullable but has no default value"
            )


# Seller CSV Schema Definition
# Based on analysis of actual CSV files and code patterns across scrapers
SELLER_SCHEMA = [
    SchemaField(
        name="seller_uuid",
        data_type=DataType.UUID,
        required=True,
        nullable=False,
        description="Unique identifier for the seller",
        examples=["0340c9fb-bb41-4f5b-8dad-9dec16e2aec8"],
    ),
    SchemaField(
        name="seller_name",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        default_value="null",
        description="Name of the seller/store",
        examples=["Esquared Outlets", "Test Store Name"],
    ),
    SchemaField(
        name="profile_photo_url",
        data_type=DataType.URL,
        required=True,
        nullable=True,
        default_value="null",
        description="URL to seller's profile photo",
        examples=["https://example.com/photo.jpg", "null"],
    ),
    SchemaField(
        name="seller_profile_url",
        data_type=DataType.URL,
        required=True,
        nullable=True,
        description="URL to seller's profile/store page",
        examples=["https://aliexpress.com/store/123", "https://amazon.com/seller"],
    ),
    SchemaField(
        name="seller_rating",
        data_type=DataType.FLOAT,
        required=True,
        nullable=True,
        default_value="null",
        description="Seller's rating score",
        examples=["4.90", "null"],
    ),
    SchemaField(
        name="total_reviews",
        data_type=DataType.INTEGER,
        required=True,
        nullable=True,
        default_value="null",
        description="Total number of reviews for the seller",
        examples=["151", "null"],
    ),
    SchemaField(
        name="contact_methods",
        data_type=DataType.JSON_ARRAY,
        required=True,
        nullable=False,
        default_value="[]",
        description="Available contact methods as JSON array",
        examples=["[]", '["email", "phone"]'],
    ),
    SchemaField(
        name="email_address",
        data_type=DataType.EMAIL,
        required=True,
        nullable=True,
        default_value="null",
        description="Seller's email address",
        examples=["seller@example.com", "null"],
    ),
    SchemaField(
        name="phone_number",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        default_value="null",
        description="Seller's phone number",
        examples=["+1-234-567-8900", "null"],
    ),
    SchemaField(
        name="physical_address",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        default_value="null",
        description="Seller's physical address",
        examples=["10 Chiswick Dr, Jackson, NJ, 08527, US", "null"],
    ),
    SchemaField(
        name="verification_status",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="Unverified",
        description="Verification status of the seller",
        examples=["Verified", "Unverified", "Pending"],
    ),
    SchemaField(
        name="seller_status",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="New",
        description="Status of the seller",
        examples=["New", "Active", "Inactive"],
    ),
    SchemaField(
        name="enforcement_status",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="None",
        description="Enforcement status for the seller",
        examples=["None", "Warning", "Suspended"],
    ),
    SchemaField(
        name="map_compliance_status",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="Compliant",
        description="MAP compliance status",
        examples=["Compliant", "Non-Compliant", "NA"],
    ),
    SchemaField(
        name="associated_listings",
        data_type=DataType.INTEGER,
        required=True,
        nullable=False,
        default_value=0,
        description="Number of listings associated with this seller",
        examples=["0", "5", "100"],
    ),
    SchemaField(
        name="date_added",
        data_type=DataType.DATETIME,
        required=True,
        nullable=False,
        format_pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$",
        description="Date when seller was first added to database",
        examples=["2025-07-18 19:35:35.275"],
    ),
    SchemaField(
        name="last_updated",
        data_type=DataType.DATETIME,
        required=True,
        nullable=False,
        format_pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$",
        description="Date when seller record was last updated",
        examples=["2025-07-18 19:35:35.176"],
    ),
    SchemaField(
        name="blacklisted",
        data_type=DataType.BOOLEAN,
        required=True,
        nullable=False,
        default_value=False,
        description="Whether the seller is blacklisted",
        examples=["true", "false"],
    ),
    SchemaField(
        name="counterfeiter",
        data_type=DataType.BOOLEAN,
        required=True,
        nullable=False,
        default_value=False,
        description="Whether the seller is flagged as counterfeiter",
        examples=["true", "false"],
    ),
    SchemaField(
        name="priority_seller",
        data_type=DataType.BOOLEAN,
        required=True,
        nullable=False,
        default_value=False,
        description="Whether the seller is marked as priority",
        examples=["true", "false"],
    ),
    SchemaField(
        name="seller_note",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        default_value="",
        description="Additional notes about the seller",
        examples=["", "High volume seller", "null"],
    ),
    SchemaField(
        name="seller_id",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Platform-specific seller ID",
        examples=["A2LD697S8ZZ8Z2", "1104923653", "null"],
    ),
    SchemaField(
        name="admin_priority_seller",
        data_type=DataType.BOOLEAN,
        required=True,
        nullable=False,
        default_value=False,
        description="Whether seller is admin-flagged as priority",
        examples=["true", "false"],
    ),
    SchemaField(
        name="known_counterfeiter",
        data_type=DataType.BOOLEAN,
        required=True,
        nullable=False,
        default_value=False,
        description="Whether seller is known counterfeiter",
        examples=["true", "false"],
    ),
    SchemaField(
        name="seller_admin_stage",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="NA",
        description="Admin processing stage for seller",
        examples=["NA", "Review", "Approved"],
    ),
    SchemaField(
        name="seller_investigation",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="NotStarted",
        description="Investigation status for seller",
        examples=["NotStarted", "InProgress", "Completed"],
    ),
    SchemaField(
        name="seller_stage",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="NA",
        description="Current processing stage of seller",
        examples=["NA", "Processing", "Completed"],
    ),
    SchemaField(
        name="seller_state",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="Active",
        description="Current state of seller record",
        examples=["Active", "Inactive", "Pending"],
    ),
]

# Listing CSV Schema Definition
# Based on analysis of actual CSV files and code patterns across scrapers
LISTING_SCHEMA = [
    SchemaField(
        name="listing_uuid",
        data_type=DataType.UUID,
        required=True,
        nullable=False,
        description="Unique identifier for the listing",
        examples=["0113701d-4d11-4f5a-a596-3f8edd261645"],
    ),
    SchemaField(
        name="product_uuid",
        data_type=DataType.UUID,
        required=True,
        nullable=True,
        default_value="null",
        description="UUID of the associated product",
        examples=["a745c0f0-f5c2-411a-babe-b39f3d13bc76", "null"],
    ),
    SchemaField(
        name="product_title",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Title/name of the product",
        examples=["Arlo Ultra - 4K UHD Wire-Free Security Camera"],
    ),
    SchemaField(
        name="product_image_urls",
        data_type=DataType.JSON_ARRAY,
        required=True,
        nullable=True,
        default_value="[]",
        description="Array of product image URLs in JSON format",
        examples=[
            '["https://example.com/image1.jpg", "https://example.com/image2.jpg"]'
        ],
    ),
    SchemaField(
        name="sku",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Stock Keeping Unit identifier",
        examples=["B07YDZF4NZ", "null"],
    ),
    SchemaField(
        name="asin",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Amazon Standard Identification Number",
        examples=["B07YDZF4NZ", "null"],
    ),
    SchemaField(
        name="item_number",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Platform-specific item number",
        examples=["VMC5040B-100NAS", "null"],
    ),
    SchemaField(
        name="price",
        data_type=DataType.FLOAT,
        required=True,
        nullable=True,
        description="Current price of the product",
        examples=["199.99", "16.00"],
    ),
    SchemaField(
        name="price_history",
        data_type=DataType.JSON_ARRAY,
        required=True,
        nullable=True,
        default_value="[]",
        description="Price history as JSON array",
        examples=[
            '[{"date":"2025-07-18T19:34:58.756Z","price":199.99,"currency":"USD"}]'
        ],
    ),
    SchemaField(
        name="variation_attributes",
        data_type=DataType.JSON_ARRAY,
        required=True,
        nullable=True,
        default_value="null",
        description="Product variations as JSON array",
        examples=['[{"asin":"B07MCWFQ6C","title":"5 Piece Set White"}]', "null"],
    ),
    SchemaField(
        name="units_available",
        data_type=DataType.INTEGER,
        required=True,
        nullable=True,
        default_value="null",
        description="Number of units available for purchase",
        examples=["10", "null"],
    ),
    SchemaField(
        name="units_sold",
        data_type=DataType.INTEGER,
        required=True,
        nullable=True,
        default_value="null",
        description="Number of units sold",
        examples=["100", "null"],
    ),
    SchemaField(
        name="listing_status",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="New",
        description="Status of the listing",
        examples=["New", "Active", "Inactive"],
    ),
    SchemaField(
        name="seller_status",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="New",
        description="Status of the seller for this listing",
        examples=["New", "Active", "Inactive"],
    ),
    SchemaField(
        name="enforcement_status",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="None",
        description="Enforcement status for the listing",
        examples=["None", "Warning", "Removed"],
    ),
    SchemaField(
        name="map_compliance_status",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="NA",
        description="MAP compliance status",
        examples=["Compliant", "Non-Compliant", "NA"],
    ),
    SchemaField(
        name="amazon_buy_box_won",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="No",
        description="Whether listing won Amazon buy box",
        examples=["Yes", "No"],
    ),
    SchemaField(
        name="date_first_detected",
        data_type=DataType.DATETIME,
        required=True,
        nullable=False,
        format_pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$",
        description="Date when listing was first detected",
        examples=["2025-07-18 19:34:58.756"],
    ),
    SchemaField(
        name="last_checked",
        data_type=DataType.DATETIME,
        required=True,
        nullable=False,
        format_pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$",
        description="Date when listing was last checked",
        examples=["2025-07-18 19:39:36.161"],
    ),
    SchemaField(
        name="authenticity",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="Unverified",
        description="Authenticity status of the product",
        examples=["Verified", "Unverified", "Counterfeit"],
    ),
    SchemaField(
        name="listing_note",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        default_value="",
        description="Additional notes about the listing",
        examples=["", "High demand item", "null"],
    ),
    SchemaField(
        name="marketplaceMarketplace_uuid",
        data_type=DataType.UUID,
        required=True,
        nullable=True,
        description="UUID of the marketplace",
        examples=["181bb34f-3d44-4944-a525-7a3b91b8ed51", "null"],
    ),
    SchemaField(
        name="parent_asin",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Parent ASIN for product variations",
        examples=["B07ZS21X2X", "null"],
    ),
    SchemaField(
        name="currency",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="USD",
        description="Currency code for price",
        examples=["USD", "EUR", "GBP"],
    ),
    SchemaField(
        name="brand_uuid",
        data_type=DataType.UUID,
        required=True,
        nullable=True,
        description="UUID of the brand",
        examples=["29e092e4-692d-4ffa-b85b-b08d7519882b", "null"],
    ),
    SchemaField(
        name="enforce_admin_stage",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="NotSubmitted",
        description="Admin enforcement stage",
        examples=["NotSubmitted", "Submitted", "Processed"],
    ),
    SchemaField(
        name="listing_enforcement",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="NA",
        description="Enforcement actions taken on listing",
        examples=["NA", "Warning", "Removal"],
    ),
    SchemaField(
        name="listing_priority",
        data_type=DataType.BOOLEAN,
        required=True,
        nullable=False,
        default_value=False,
        description="Whether listing is marked as priority",
        examples=["true", "false"],
    ),
    SchemaField(
        name="listing_stage",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="NA",
        description="Current processing stage of listing",
        examples=["NA", "Processing", "Completed"],
    ),
    SchemaField(
        name="re_listed_status",
        data_type=DataType.BOOLEAN,
        required=True,
        nullable=False,
        default_value=False,
        description="Whether the listing was re-listed",
        examples=["true", "false"],
    ),
    SchemaField(
        name="listing_url",
        data_type=DataType.URL,
        required=True,
        nullable=True,
        description="URL to the product listing",
        examples=["https://aliexpress.com/item/123456789.html", "null"],
    ),
    SchemaField(
        name="listing_state",
        data_type=DataType.STRING,
        required=True,
        nullable=False,
        default_value="Active",
        description="Current state of the listing",
        examples=["Active", "Inactive", "Pending"],
    ),
    SchemaField(
        name="brand_name",
        data_type=DataType.STRING,
        required=True,
        nullable=True,
        description="Name of the product brand",
        examples=["Arlo", "Supergoop", "null"],
    ),
]


def get_schema_by_name(schema_name: str) -> List[SchemaField]:
    """Get schema definition by name."""
    schemas = {
        "seller": SELLER_SCHEMA,
        "listing": LISTING_SCHEMA,
    }

    if schema_name.lower() not in schemas:
        raise ValueError(
            f"Unknown schema: {schema_name}. Available: {list(schemas.keys())}"
        )

    return schemas[schema_name.lower()]


def get_column_names(schema: List[SchemaField]) -> List[str]:
    """Extract column names from schema definition."""
    return [field.name for field in schema]


def get_required_columns(schema: List[SchemaField]) -> List[str]:
    """Get list of required column names from schema."""
    return [field.name for field in schema if field.required]


def get_schema_info() -> Dict[str, Dict[str, Any]]:
    """Get comprehensive information about all schemas."""
    return {
        "seller": {
            "total_columns": len(SELLER_SCHEMA),
            "required_columns": len(get_required_columns(SELLER_SCHEMA)),
            "column_names": get_column_names(SELLER_SCHEMA),
            "data_types": {
                field.name: field.data_type.value for field in SELLER_SCHEMA
            },
        },
        "listing": {
            "total_columns": len(LISTING_SCHEMA),
            "required_columns": len(get_required_columns(LISTING_SCHEMA)),
            "column_names": get_column_names(LISTING_SCHEMA),
            "data_types": {
                field.name: field.data_type.value for field in LISTING_SCHEMA
            },
        },
    }
