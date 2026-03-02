"""
Configure Great Expectations to connect to PostgreSQL database.

This creates a datasource that GE will use to run validations.
"""

import great_expectations as gx
from great_expectations.core.batch import BatchRequest
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize Great Expectations context
context = gx.get_context()

# Database connection parameters
datasource_config = {
    "name": "healthcare_postgres",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        ),
    },
    "data_connectors": {
        "default_runtime_data_connector": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        },
    },
}

# Test the connection and add datasource
try:
    context.add_datasource(**datasource_config)
    print("✓ Datasource 'healthcare_postgres' configured successfully")
    
    # List available tables to verify connection
    datasource = context.get_datasource("healthcare_postgres")
    print("\n Available tables:")
    for asset_name in datasource.get_available_data_asset_names():
        print(f"  - {asset_name}")
    
except Exception as e:
    print(f"Error configuring datasource: {e}")
