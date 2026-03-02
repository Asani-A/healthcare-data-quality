"""
Create expectation suites for all healthcare tables.

This consolidates suite creation for easier maintenance.
"""

import great_expectations as gx
from great_expectations.core.batch import BatchRequest

context = gx.get_context()

# ============================================================================
# ENCOUNTERS SUITE
# ============================================================================

def create_encounters_suite():
    suite_name = "encounters_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    
    batch_request = BatchRequest(
        datasource_name="healthcare_postgres",
        data_connector_name="default_inferred_data_connector",
        data_asset_name="public.encounters",
    )
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    
    print(f"\nCreating {suite_name}...")
    
    # Primary key
    validator.expect_column_values_to_be_unique(column="encounter_id")
    validator.expect_column_values_to_not_be_null(column="encounter_id")
    
    # Foreign key - patient must exist (we introduced 5% orphaned)
    validator.expect_column_values_to_not_be_null(column="patient_id", mostly=0.95)
    
    # Encounter type must be in valid set
    validator.expect_column_values_to_be_in_set(
        column="encounter_type",
        value_set=["Emergency", "Inpatient", "Outpatient", "Urgent Care", "Telemedicine"],
    )
    
    # Date validations - admission date required
    validator.expect_column_values_to_not_be_null(
        column="admission_date",
        mostly=0.98,  # We introduced 2% null
        meta={"criticality": "HIGH"}
    )
    
    # Discharge should be after admission (we introduced 10% logic errors)
    validator.expect_column_pair_values_a_to_be_greater_than_b(
        column_A="discharge_date",
        column_B="admission_date",
        or_equal=True,
        mostly=0.90,
        meta={"criticality": "CRITICAL", "notes": "Discharge cannot precede admission"}
    )
    
    # Cost validations
    validator.expect_column_values_to_be_between(
        column="total_cost",
        min_value=0,
        max_value=1000000,
        mostly=0.97,  # We introduced 3% negative
        meta={"notes": "Negative costs indicate billing system errors"}
    )
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✓ {suite_name} created ({len(validator.get_expectation_suite().expectations)} expectations)")

# ============================================================================
# DIAGNOSES SUITE
# ============================================================================

def create_diagnoses_suite():
    suite_name = "diagnoses_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    
    batch_request = BatchRequest(
        datasource_name="healthcare_postgres",
        data_connector_name="default_inferred_data_connector",
        data_asset_name="public.diagnoses",
    )
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    
    print(f"\nCreating {suite_name}...")
    
    # Primary key
    validator.expect_column_values_to_be_unique(column="diagnosis_id")
    validator.expect_column_values_to_not_be_null(column="diagnosis_id")
    
    # Foreign key - encounter must exist (we introduced 8% orphaned)
    validator.expect_column_values_to_not_be_null(column="encounter_id", mostly=0.92)
    
    # ICD-10 code is critical for billing (we introduced 15% null)
    validator.expect_column_values_to_not_be_null(
        column="icd10_code",
        mostly=0.85,
        meta={"criticality": "CRITICAL", "notes": "ICD-10 codes required for insurance billing"}
    )
    
    # ICD-10 format validation (simplified - real codes are complex)
    validator.expect_column_values_to_match_regex(
        column="icd10_code",
        regex=r"^[A-Z]\d{2}\.\d{1,2}$",
        mostly=0.85,
        meta={"notes": "ICD-10 codes follow specific format"}
    )
    
    # Severity must be in valid set
    validator.expect_column_values_to_be_in_set(
        column="severity",
        value_set=["Mild", "Moderate", "Severe", "Critical"],
    )
    
    # Diagnosis date should be reasonable
    validator.expect_column_values_to_be_between(
        column="diagnosis_date",
        min_value="2020-01-01",
        max_value="2024-12-31",
        parse_strings_as_datetimes=True,
        mostly=0.95,  # We introduced 5% future dates
    )
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✓ {suite_name} created ({len(validator.get_expectation_suite().expectations)} expectations)")

# ============================================================================
# MEDICATIONS SUITE
# ============================================================================

def create_medications_suite():
    suite_name = "medications_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    
    batch_request = BatchRequest(
        datasource_name="healthcare_postgres",
        data_connector_name="default_inferred_data_connector",
        data_asset_name="public.medications",
    )
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    
    print(f"\nCreating {suite_name}...")
    
    # Primary key
    validator.expect_column_values_to_be_unique(column="medication_id")
    validator.expect_column_values_to_not_be_null(column="medication_id")
    
    # Foreign key
    validator.expect_column_values_to_not_be_null(column="encounter_id")
    
    # Medication name is critical (we introduced 5% null)
    validator.expect_column_values_to_not_be_null(
        column="medication_name",
        mostly=0.95,
        meta={"criticality": "CRITICAL", "notes": "Medication name required for pharmacy"}
    )
    
    # Dosage should be present (we introduced 3% null)
    validator.expect_column_values_to_not_be_null(
        column="dosage",
        mostly=0.97,
        meta={"criticality": "HIGH", "notes": "Dosage required for patient safety"}
    )
    
    # End date should be after start date (we introduced 10% logic errors)
    validator.expect_column_pair_values_a_to_be_greater_than_b(
        column_A="end_date",
        column_B="start_date",
        or_equal=True,
        mostly=0.90,
        meta={"criticality": "HIGH", "notes": "Medication end date must be after start"}
    )
    
    # Frequency should be in expected set
    validator.expect_column_values_to_be_in_set(
        column="frequency",
        value_set=["Once daily", "Twice daily", "Three times daily", "As needed"],
        mostly=0.98,
    )
    
    # Prescribing physician should be present
    validator.expect_column_values_to_not_be_null(column="prescribing_physician")
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✓ {suite_name} created ({len(validator.get_expectation_suite().expectations)} expectations)")

# ============================================================================
# RUN ALL
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Creating Great Expectations suites for all tables")
    print("=" * 60)
    
    create_encounters_suite()
    create_diagnoses_suite()
    create_medications_suite()
    
    print("\n" + "=" * 60)
    print("✓ All expectation suites created successfully!")
    print("\nSuites available:")
    print("  - patients_suite")
    print("  - encounters_suite")
    print("  - diagnoses_suite")
    print("  - medications_suite")
