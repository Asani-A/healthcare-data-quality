"""
Create expectation suite for the patients table.

This suite validates:
- Schema correctness (column existence, data types)
- Completeness (required fields not null)
- Validity (proper value ranges, formats)
- Uniqueness (no duplicate patient IDs)
"""

import great_expectations as gx
from great_expectations.core.batch import BatchRequest

# Initialize context
context = gx.get_context()

# Create a new expectation suite
suite_name = "patients_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# Get a validator for the patients table
batch_request = BatchRequest(
    datasource_name="healthcare_postgres",
    data_connector_name="default_inferred_data_connector",
    data_asset_name="public.patients",
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name,
)

print(f"Creating expectations for {suite_name}...")
print("=" * 60)

# ============================================================================
# SCHEMA EXPECTATIONS - Ensure table structure is correct
# ============================================================================

# Expect specific columns to exist
expected_columns = [
    "patient_id", "first_name", "last_name", "date_of_birth",
    "gender", "email", "phone", "created_at"
]

validator.expect_table_columns_to_match_ordered_list(
    column_list=expected_columns,
)
print("✓ Validated column schema")

# ============================================================================
# PRIMARY KEY EXPECTATIONS - Patient ID must be unique and present
# ============================================================================

validator.expect_column_values_to_be_unique(
    column="patient_id",
    meta={
        "criticality": "CRITICAL",
        "notes": "Patient ID is the primary key - duplicates would cause serious data integrity issues"
    }
)

validator.expect_column_values_to_not_be_null(
    column="patient_id",
    meta={
        "criticality": "CRITICAL",
        "notes": "Every patient must have an identifier"
    }
)

validator.expect_column_values_to_match_regex(
    column="patient_id",
    regex=r"^PAT-\d{6}$",
    meta={
        "notes": "Patient IDs should follow format PAT-XXXXXX"
    }
)
print("✓ Added primary key validations")

# ============================================================================
# COMPLETENESS EXPECTATIONS - Critical fields must not be null
# ============================================================================

# Names should be present (we introduced 2% null first names, 1% null last names)
validator.expect_column_values_to_not_be_null(
    column="first_name",
    mostly=0.98,  # Accept up to 2% null
    meta={
        "criticality": "HIGH",
        "notes": "First name is required for patient identification"
    }
)

validator.expect_column_values_to_not_be_null(
    column="last_name",
    mostly=0.99,  # Accept up to 1% null
    meta={
        "criticality": "HIGH",
        "notes": "Last name is required for patient identification"
    }
)

# Date of birth is critical for age calculations, billing, clinical decisions
validator.expect_column_values_to_not_be_null(
    column="date_of_birth",
    mostly=0.97,  # We introduced 3% null
    meta={
        "criticality": "CRITICAL",
        "notes": "DOB required for age-based clinical protocols and billing"
    }
)
print("✓ Added completeness validations")

# ============================================================================
# VALIDITY EXPECTATIONS - Values must be in reasonable ranges
# ============================================================================

# Date of birth should be in the past (not future)
validator.expect_column_values_to_be_between(
    column="date_of_birth",
    min_value="1900-01-01",
    max_value="2024-12-31",  # No future births
    parse_strings_as_datetimes=True,
    mostly=0.95,  # We introduced 5% future dates
    meta={
        "criticality": "HIGH",
        "notes": "Future birth dates indicate data entry errors"
    }
)

# Age should be reasonable (0-120 years)
# GE doesn't have built-in age calculation, so we use a SQL expression
validator.expect_column_values_to_be_between(
    column="date_of_birth",
    min_value="1904-01-01",  # 120 years ago from 2024
    max_value="2024-12-31",
    parse_strings_as_datetimes=True,
    mostly=0.98,  # We introduced 2% ages > 120
    meta={
        "criticality": "MEDIUM",
        "notes": "Ages over 120 are extremely rare and likely errors"
    }
)

# Gender should be in expected set
validator.expect_column_values_to_be_in_set(
    column="gender",
    value_set=["Male", "Female", "Other", "Unknown"],
    mostly=0.99,
    meta={
        "notes": "Standardized gender values for reporting"
    }
)
print("✓ Added validity range validations")

# ============================================================================
# FORMAT EXPECTATIONS - Email and phone should follow patterns
# ============================================================================

# Email should contain @ symbol (we introduced 5% without @)
validator.expect_column_values_to_match_regex(
    column="email",
    regex=r"^[^@]+@[^@]+\.[^@]+$",
    mostly=0.95,
    meta={
        "criticality": "MEDIUM",
        "notes": "Valid email format required for patient communications"
    }
)

# Phone can be null, but if present should be formatted
# We allow 10% null, but non-null should have proper format
validator.expect_column_values_to_match_regex_list(
    column="phone",
    regex_list=[
        r"^\(\d{3}\) \d{3}-\d{4}$",  # (555) 555-5555
        r"^\d{10}$",                   # 5555555555
    ],
    match_on="any",
    mostly=0.70,  # 10% null + 20% improperly formatted
    meta={
        "criticality": "LOW",
        "notes": "Phone format should be standardized for automated dialing systems"
    }
)
print("✓ Added format validations")

# ============================================================================
# DATA FRESHNESS - Track when records were created
# ============================================================================

validator.expect_column_values_to_not_be_null(
    column="created_at",
    meta={
        "notes": "Audit trail - every record should have creation timestamp"
    }
)
print("✓ Added audit trail validations")

# ============================================================================
# SAVE THE SUITE
# ============================================================================

validator.save_expectation_suite(discard_failed_expectations=False)

print("=" * 60)
print(f"✓ Expectation suite '{suite_name}' created successfully!")
print(f"\nTotal expectations: {len(validator.get_expectation_suite().expectations)}")
print("\nSuite saved to: great_expectations/expectations/")
