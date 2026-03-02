"""
Generate synthetic healthcare data with intentional quality issues.

This script creates realistic test data that demonstrates common data quality problems:
- Missing values in critical fields
- Invalid date ranges
- Orphaned records (referential integrity violations)
- Out-of-range values
- Inconsistent formatting
"""

import random
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
DB_CONNECTION_STRING = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Sample data for generation
FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 
               'Linda', 'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan']
LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
              'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez']
GENDERS = ['Male', 'Female', 'Other', 'Unknown']
ENCOUNTER_TYPES = ['Emergency', 'Inpatient', 'Outpatient', 'Urgent Care', 'Telemedicine']
ICD10_CODES = ['E11.9', 'I10', 'J44.9', 'M54.5', 'F32.9', 'N18.3', 'K21.9', 'G43.909']
SEVERITIES = ['Mild', 'Moderate', 'Severe', 'Critical']
MEDICATIONS = ['Metformin', 'Lisinopril', 'Atorvastatin', 'Amlodipine', 'Omeprazole',
               'Albuterol', 'Gabapentin', 'Sertraline', 'Ibuprofen', 'Levothyroxine']


def random_date(start_year=1940, end_year=2005):
    """Generate random date for birth dates."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def random_recent_date(days_back=365):
    """Generate recent date for encounters."""
    return datetime.now() - timedelta(days=random.randint(0, days_back))


def generate_patients(n=1000):
    """
    Generate patient records with intentional quality issues:
    - 2% missing first names
    - 1% missing last names  
    - 3% null date of birth
    - 5% future birth dates (invalid)
    - 2% ages over 120 (suspicious)
    """
    patients = []
    
    for i in range(n):
        patient_id = f"PAT-{i+1:06d}"
        
        # Intentional issues
        first_name = None if random.random() < 0.02 else random.choice(FIRST_NAMES)
        last_name = None if random.random() < 0.01 else random.choice(LAST_NAMES)
        
        # Generate DOB with issues
        if random.random() < 0.03:
            dob = None  # Missing DOB
        elif random.random() < 0.05:
            dob = datetime.now() + timedelta(days=random.randint(1, 365))  # Future date
        elif random.random() < 0.02:
            dob = datetime(1880, 1, 1) + timedelta(days=random.randint(0, 10000))  # Age > 120
        else:
            dob = random_date()
        
        gender = random.choice(GENDERS)
        
        # Email formatting issues (some invalid)
        if random.random() < 0.05:
            email = f"{first_name or 'unknown'}{last_name or 'user'}".lower()  # Missing @
        else:
            email = f"{first_name or 'unknown'}.{last_name or 'user'}@email.com".lower()
        
        # Phone with formatting inconsistencies
        if random.random() < 0.10:
            phone = None
        elif random.random() < 0.20:
            phone = f"{random.randint(1000000000, 9999999999)}"  # No formatting
        else:
            phone = f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"
        
        patients.append({
            'patient_id': patient_id,
            'first_name': first_name,
            'last_name': last_name,
            'date_of_birth': dob,
            'gender': gender,
            'email': email,
            'phone': phone
        })
    
    return pd.DataFrame(patients)


def generate_encounters(patients_df, avg_encounters_per_patient=2):
    """
    Generate encounter records with quality issues:
    - 5% missing patient_id (orphaned records)
    - 10% discharge before admission (logic error)
    - 3% negative costs
    - 2% null admission dates
    """
    encounters = []
    encounter_id = 1
    
    for _, patient in patients_df.iterrows():
        num_encounters = random.randint(0, avg_encounters_per_patient * 2)
        
        for _ in range(num_encounters):
            enc_id = f"ENC-{encounter_id:08d}"
            
            # Intentional orphaned records
            if random.random() < 0.05:
                patient_id = f"PAT-{random.randint(900000, 999999)}"  # Non-existent
            else:
                patient_id = patient['patient_id']
            
            encounter_type = random.choice(ENCOUNTER_TYPES)
            
            # Date logic errors
            if random.random() < 0.02:
                admission_date = None
            else:
                admission_date = random_recent_date(days_back=180)
            
            if admission_date and random.random() < 0.10:
                # Discharge before admission
                discharge_date = admission_date - timedelta(days=random.randint(1, 5))
            elif admission_date:
                discharge_date = admission_date + timedelta(days=random.randint(1, 14))
            else:
                discharge_date = random_recent_date(days_back=180)
            
            primary_diagnosis = random.choice(ICD10_CODES)
            
            # Cost issues
            if random.random() < 0.03:
                cost = -random.uniform(100, 5000)  # Negative cost
            elif random.random() < 0.05:
                cost = None
            else:
                cost = round(random.uniform(500, 50000), 2)
            
            encounters.append({
                'encounter_id': enc_id,
                'patient_id': patient_id,
                'encounter_type': encounter_type,
                'admission_date': admission_date,
                'discharge_date': discharge_date,
                'primary_diagnosis_code': primary_diagnosis,
                'total_cost': cost
            })
            
            encounter_id += 1
    
    return pd.DataFrame(encounters)


def generate_diagnoses(encounters_df):
    """
    Generate diagnosis records with quality issues:
    - 8% orphaned encounter references
    - 15% null ICD-10 codes
    - 5% future diagnosis dates
    """
    diagnoses = []
    diagnosis_id = 1
    
    for _, encounter in encounters_df.iterrows():
        # 70% of encounters have diagnoses
        if random.random() < 0.70:
            num_diagnoses = random.randint(1, 3)
            
            for _ in range(num_diagnoses):
                diag_id = f"DIAG-{diagnosis_id:08d}"
                
                # Orphaned records
                if random.random() < 0.08:
                    enc_id = f"ENC-{random.randint(90000000, 99999999)}"
                else:
                    enc_id = encounter['encounter_id']
                
                # Missing codes
                if random.random() < 0.15:
                    icd10 = None
                    description = "Unspecified condition"
                else:
                    icd10 = random.choice(ICD10_CODES)
                    description = f"Diagnosis for {icd10}"
                
                # Date issues
                if encounter['admission_date'] and random.random() < 0.05:
                    diag_date = encounter['admission_date'] + timedelta(days=random.randint(30, 90))
                elif encounter['admission_date']:
                    diag_date = encounter['admission_date']
                else:
                    diag_date = random_recent_date()
                
                severity = random.choice(SEVERITIES)
                
                diagnoses.append({
                    'diagnosis_id': diag_id,
                    'encounter_id': enc_id,
                    'icd10_code': icd10,
                    'diagnosis_description': description,
                    'diagnosis_date': diag_date,
                    'severity': severity
                })
                
                diagnosis_id += 1
    
    return pd.DataFrame(diagnoses)


def generate_medications(encounters_df):
    """
    Generate medication records with quality issues:
    - 10% end_date before start_date
    - 5% missing medication names
    - 3% null dosages
    """
    medications = []
    medication_id = 1
    
    for _, encounter in encounters_df.iterrows():
        # 60% of encounters have medications
        if random.random() < 0.60:
            num_meds = random.randint(1, 4)
            
            for _ in range(num_meds):
                med_id = f"MED-{medication_id:08d}"
                
                enc_id = encounter['encounter_id']
                
                # Missing medication names
                if random.random() < 0.05:
                    med_name = None
                else:
                    med_name = random.choice(MEDICATIONS)
                
                # Missing dosages
                dosage = None if random.random() < 0.03 else f"{random.choice([5, 10, 20, 50, 100])}mg"
                
                frequency = random.choice(['Once daily', 'Twice daily', 'Three times daily', 'As needed'])
                
                if encounter['admission_date']:
                    start_date = encounter['admission_date']
                else:
                    start_date = random_recent_date()
                
                # End date logic errors
                if random.random() < 0.10:
                    end_date = start_date - timedelta(days=random.randint(1, 30))
                else:
                    end_date = start_date + timedelta(days=random.randint(7, 90))
                
                physician = f"Dr. {random.choice(LAST_NAMES)}"
                
                medications.append({
                    'medication_id': med_id,
                    'encounter_id': enc_id,
                    'medication_name': med_name,
                    'dosage': dosage,
                    'frequency': frequency,
                    'start_date': start_date,
                    'end_date': end_date,
                    'prescribing_physician': physician
                })
                
                medication_id += 1
    
    return pd.DataFrame(medications)

def load_data_to_postgres(patients_df, encounters_df, diagnoses_df, medications_df):
    """Load all dataframes to PostgreSQL."""
    engine = create_engine(DB_CONNECTION_STRING)

    print("Loading data to PostgreSQL...")

    # Load in order due to foreign key constraints
    patients_df.to_sql('patients', engine, if_exists='replace', index=False)
    print(f"✓ Loaded {len(patients_df)} patients")

    encounters_df.to_sql('encounters', engine, if_exists='replace', index=False)
    print(f"✓ Loaded {len(encounters_df)} encounters")

    diagnoses_df.to_sql('diagnoses', engine, if_exists='replace', index=False)
    print(f"✓ Loaded {len(diagnoses_df)} diagnoses")

    medications_df.to_sql('medications', engine, if_exists='replace', index=False)
    print(f"✓ Loaded {len(medications_df)} medications")

    # Create data_quality_results table - FIXED with text()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS data_quality_results (
                validation_id SERIAL PRIMARY KEY,
                run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expectation_suite_name VARCHAR(200),
                table_name VARCHAR(100),
                total_expectations INTEGER,
                successful_expectations INTEGER,
                failed_expectations INTEGER,
                success_percent NUMERIC(5, 2),
                validation_status VARCHAR(20),
                failed_expectation_details JSONB
            )
        """))
        conn.commit()

    print("✓ Created data_quality_results table")
    print("\nData generation complete!")



if __name__ == "__main__":
    print("Generating synthetic healthcare data...")
    print("=" * 60)
    
    # Generate data
    patients = generate_patients(n=1000)
    encounters = generate_encounters(patients, avg_encounters_per_patient=2)
    diagnoses = generate_diagnoses(encounters)
    medications = generate_medications(encounters)
    
    print(f"\nGenerated:")
    print(f"  - {len(patients)} patients")
    print(f"  - {len(encounters)} encounters")
    print(f"  - {len(diagnoses)} diagnoses")
    print(f"  - {len(medications)} medications")
    print()
    
    # Load to database
    load_data_to_postgres(patients, encounters, diagnoses, medications)
    
    print("\n" + "=" * 60)
    print("Summary of intentional quality issues:")
    print("  Patients: ~2% missing names, ~3% null DOB, ~5% invalid dates")
    print("  Encounters: ~5% orphaned, ~10% date logic errors, ~3% negative costs")
    print("  Diagnoses: ~8% orphaned, ~15% null codes, ~5% future dates")
    print("  Medications: ~10% end before start, ~5% missing names")
    print("\nThese issues will be caught by Great Expectations validations!")
