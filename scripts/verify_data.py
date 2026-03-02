"""Quick verification of generated data."""

from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONNECTION_STRING = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

engine = create_engine(DB_CONNECTION_STRING)

# Quick counts
tables = ['patients', 'encounters', 'diagnoses', 'medications']
for table in tables:
    count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", engine).iloc[0, 0]
    print(f"{table}: {count} records")

# Sample some problematic data
print("\n--- Sample Issues ---")
print("\nPatients with future birth dates:")
print(pd.read_sql("""
    SELECT patient_id, first_name, last_name, date_of_birth 
    FROM patients 
    WHERE date_of_birth > CURRENT_DATE 
    LIMIT 5
""", engine))

print("\nEncounters with discharge before admission:")
print(pd.read_sql("""
    SELECT encounter_id, admission_date, discharge_date 
    FROM encounters 
    WHERE discharge_date < admission_date 
    LIMIT 5
""", engine))
