"""
Create a Great Expectations checkpoint for running validations.

A checkpoint defines:
- Which expectation suites to run
- What data sources to validate
- How to handle results (Data Docs, alerts, etc.)
"""

import great_expectations as gx

context = gx.get_context()

# Checkpoint configuration
checkpoint_config = {
    "name": "healthcare_quality_checkpoint",
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-healthcare-validation",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "healthcare_postgres",
                "data_connector_name": "default_inferred_data_connector",
                "data_asset_name": "public.patients",
            },
            "expectation_suite_name": "patients_suite",
        },
        {
            "batch_request": {
                "datasource_name": "healthcare_postgres",
                "data_connector_name": "default_inferred_data_connector",
                "data_asset_name": "public.encounters",
            },
            "expectation_suite_name": "encounters_suite",
        },
        {
            "batch_request": {
                "datasource_name": "healthcare_postgres",
                "data_connector_name": "default_inferred_data_connector",
                "data_asset_name": "public.diagnoses",
            },
            "expectation_suite_name": "diagnoses_suite",
        },
        {
            "batch_request": {
                "datasource_name": "healthcare_postgres",
                "data_connector_name": "default_inferred_data_connector",
                "data_asset_name": "public.medications",
            },
            "expectation_suite_name": "medications_suite",
        },
    ],
}

# Add checkpoint to context
context.add_or_update_checkpoint(**checkpoint_config)

print("=" * 60)
print("✓ Checkpoint 'healthcare_quality_checkpoint' created!")
print("=" * 60)
print("\nThis checkpoint will validate:")
print("  - patients table against patients_suite")
print("  - encounters table against encounters_suite")
print("  - diagnoses table against diagnoses_suite")
print("  - medications table against medications_suite")
print("\nCheckpoint saved to: great_expectations/checkpoints/")
