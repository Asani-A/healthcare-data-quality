"""View historical validation results."""

from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONNECTION_STRING = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

engine = create_engine(DB_CONNECTION_STRING)

print("=" * 60)
print("DATA QUALITY VALIDATION HISTORY")
print("=" * 60)

# Get summary by table
df = pd.read_sql(text("""
    SELECT 
        table_name,
        run_timestamp,
        validation_status,
        success_percent,
        failed_expectations
    FROM data_quality_results
    ORDER BY run_timestamp DESC
    LIMIT 20
"""), engine)

print("\nRecent Validation Runs:")
print(df.to_string(index=False))

# Get failure trends
print("\n" + "=" * 60)
print("FAILURE SUMMARY BY TABLE")
print("=" * 60)

trends = pd.read_sql(text("""
    SELECT 
        table_name,
        AVG(success_percent) as avg_success_rate,
        MAX(failed_expectations) as max_failures,
        COUNT(*) as validation_runs
    FROM data_quality_results
    GROUP BY table_name
    ORDER BY avg_success_rate ASC
"""), engine)

print("\n" + trends.to_string(index=False))
