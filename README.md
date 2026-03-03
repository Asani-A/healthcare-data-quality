# Healthcare Data Quality Monitoring Framework

A production-ready data quality monitoring system built with Python, PostgreSQL, and Great Expectations. This framework demonstrates automated validation of healthcare data with comprehensive quality checks, historical tracking, and interactive reporting.

---

## Table of Contents

- [Overview](#overview)
- [Problem Statement](#problem-statement)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data Quality Checks](#data-quality-checks)
- [Validation Results](#validation-results)
- [Future Enhancements](#future-enhancements)
- [Author](#author)

---

## Overview

This project implements a comprehensive data quality monitoring framework for healthcare data, addressing the critical need for data validation in systems where accuracy directly impacts patient care, billing compliance, and regulatory requirements.

The framework validates four core healthcare tables (patients, encounters, diagnoses, medications) against 40+ expectations covering completeness, validity, consistency, uniqueness, and referential integrity. All validation results are logged to a PostgreSQL database for trend analysis, and interactive HTML reports are generated automatically.

**Key Business Value:**
- Catches data quality issues before they reach production systems
- Provides historical tracking to identify degrading data quality over time
- Reduces manual QA effort through automated validation
- Generates stakeholder-ready reports for data governance
- Integrates seamlessly with orchestration tools (Airflow, cron)

---

## Problem Statement

Healthcare organizations face significant data quality challenges:

- **Clinical Risk**: Incorrect patient data can lead to misdiagnoses or improper treatment
- **Financial Impact**: Data errors cause billing rejections and revenue loss
- **Regulatory Compliance**: HIPAA, CMS, and FDA require accurate data for audits
- **Operational Efficiency**: Poor data quality increases manual reconciliation work

**Common Data Quality Issues in Healthcare:**
- Missing critical fields (patient identifiers, diagnosis codes)
- Invalid date ranges (discharge before admission, future birth dates)
- Referential integrity violations (orphaned records)
- Inconsistent formatting across systems
- Out-of-range values (negative costs, impossible ages)

This framework provides automated detection and tracking of these issues.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Orchestration Layer                       │
│              (Airflow / Cron / Manual Trigger)              │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              run_data_quality_checks.py                      │
│          (Main Pipeline Orchestration Script)                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│               Great Expectations Engine                      │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │  Patients    │  │  Encounters  │  │   Diagnoses     │  │
│  │    Suite     │  │    Suite     │  │     Suite       │  │
│  │ 15 checks    │  │  9 checks    │  │   10 checks     │  │
│  └──────────────┘  └──────────────┘  └─────────────────┘  │
│  ┌──────────────┐                                          │
│  │ Medications  │                                          │
│  │    Suite     │                                          │
│  │  8 checks    │                                          │
│  └──────────────┘                                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              PostgreSQL Database                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Healthcare Tables                                     │ │
│  │  - patients (1,000 records)                          │ │
│  │  - encounters (~2,000 records)                       │ │
│  │  - diagnoses (~2,700 records)                        │ │
│  │  - medications (~2,800 records)                      │ │
│  └───────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Quality Tracking                                      │ │
│  │  - data_quality_results (validation history)         │ │
│  └───────────────────────────────────────────────────────┘ │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Output Artifacts                          │
│  - HTML Data Docs (interactive dashboard)                   │
│  - Validation history (PostgreSQL logs)                     │
│  - Exit codes for scheduler integration                     │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Features

### Automated Data Validation
- 40+ expectation rules across 4 healthcare tables
- Validates completeness, validity, consistency, uniqueness, referential integrity
- Configurable quality thresholds using the `mostly` parameter
- Metadata tagging for criticality levels (CRITICAL, HIGH, MEDIUM, LOW)

### Historical Tracking
- All validation results logged to PostgreSQL with timestamps
- Enables trend analysis to detect degrading data quality
- Supports compliance auditing and SLA monitoring

### Interactive Reporting
- Auto-generated HTML Data Docs with visual dashboards
- Drill-down capability to see specific failing records
- Shareable reports for stakeholders and data governance teams

### Production-Ready Design
- Proper exit codes (0=pass, 1=fail, 2=error) for scheduler integration
- Comprehensive error handling and logging
- Environment variable configuration for security
- Version-controlled expectation suites

### Scheduler Integration
- Designed for Apache Airflow DAGs
- Compatible with cron jobs
- Can be triggered manually or via CI/CD pipelines

---

## Technology Stack

**Core Technologies:**
- Python 3.11
- PostgreSQL 15
- Great Expectations 0.18.12

**Key Libraries:**
- SQLAlchemy 2.0.27 - Database ORM and connection management
- Pandas 2.2.0 - Data manipulation and analysis
- psycopg2-binary 2.9.9 - PostgreSQL adapter
- python-dotenv 1.0.1 - Environment variable management

**Development Tools:**
- Git - Version control
- Virtual environments - Dependency isolation
- Docker (optional) - Containerized PostgreSQL

---

## Installation & Setup

### Prerequisites
- Python 3.11+
- PostgreSQL 15+
- Git

### Step 1: Clone the Repository

```bash
git clone https://github.com/Asani-A/healthcare-data-quality.git
cd healthcare-data-quality
```

### Step 2: Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Set Up PostgreSQL Database

**Option A: Local PostgreSQL**
```bash
createdb healthcare_quality_db
```

**Option B: Docker PostgreSQL**
```bash
docker run --name healthcare-postgres \
  -e POSTGRES_PASSWORD=yourpassword \
  -e POSTGRES_DB=healthcare_quality_db \
  -p 5432:5432 \
  -d postgres:15
```

### Step 5: Configure Environment Variables

Create a `.env` file in the project root:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=healthcare_quality_db
DB_USER=postgres
DB_PASSWORD=yourpassword
```

**Important:** Never commit the `.env` file to version control.

### Step 6: Generate Synthetic Healthcare Data

```bash
python scripts/generate_synthetic_data.py
```

This creates 4 tables with intentional quality issues for demonstration:
- 1,000 patient records
- Approximately 2,000 encounters
- Approximately 2,700 diagnoses
- Approximately 2,800 medications

### Step 7: Configure Great Expectations

```bash
python scripts/configure_datasource.py
```

### Step 8: Create Expectation Suites

```bash
python scripts/create_patient_expectations.py
python scripts/create_all_expectations.py
```

### Step 9: Create Validation Checkpoint

```bash
python scripts/create_checkpoint.py
```

---

## Usage

### Run Complete Validation Pipeline

```bash
python run_data_quality_checks.py
```

**Expected Output:**
```
======================================================================
HEALTHCARE DATA QUALITY MONITORING PIPELINE
======================================================================
Started at: 2024-03-03 14:30:00

Initializing Great Expectations context...
✓ Great Expectations context loaded
Connecting to PostgreSQL database...
✓ Database connection successful

----------------------------------------------------------------------
RUNNING VALIDATIONS
----------------------------------------------------------------------
Checkpoint: healthcare_quality_checkpoint
Validating tables: patients, encounters, diagnoses, medications

✓ Validations complete

----------------------------------------------------------------------
LOGGING RESULTS
----------------------------------------------------------------------
✓ Logged results for patients
✓ Logged results for encounters
✓ Logged results for diagnoses
✓ Logged results for medications

----------------------------------------------------------------------
GENERATING DATA DOCS
----------------------------------------------------------------------
✓ Data Docs generated
  Location: /path/to/gx/uncommitted/data_docs/local_site/index.html

======================================================================
VALIDATION SUMMARY
======================================================================

✗ PATIENTS_SUITE
  Expectations: 10/15 passed (66.7%)
  Failed checks:
    • first_name: values to not be null
    • date_of_birth: values to be between
    • email: values to match regex

✗ ENCOUNTERS_SUITE
  Expectations: 6/9 passed (66.7%)
  Failed checks:
    • discharge_date: pair values a to be greater than b
    • total_cost: values to be between

======================================================================
OVERALL STATUS: ✗ FAIL - Data quality issues detected
Duration: 3.45 seconds
======================================================================
```

### View Validation History

```bash
python scripts/view_validation_history.py
```

### Open Data Docs in Browser

```bash
python scripts/open_data_docs.py
```

Or manually navigate to: `gx/uncommitted/data_docs/local_site/index.html`

### Verify Data Generation

```bash
python scripts/verify_data.py
```

---

## Project Structure

```
healthcare-data-quality/
│
├── run_data_quality_checks.py    # Main orchestration script
├── requirements.txt                # Python dependencies
├── .env                           # Database credentials (not in git)
├── .gitignore                     # Git ignore rules
│
├── scripts/                       # Utility scripts
│   ├── configure_datasource.py    # GE datasource setup
│   ├── create_checkpoint.py       # Checkpoint configuration
│   ├── create_patient_expectations.py  # Patient suite creation
│   ├── create_all_expectations.py      # All other suites
│   ├── generate_synthetic_data.py      # Test data generation
│   ├── verify_data.py             # Data verification
│   ├── run_validation.py          # Validation runner (alternative)
│   ├── view_validation_history.py # Query validation logs
│   └── open_data_docs.py          # Open HTML reports
│
├── gx/                            # Great Expectations directory
│   ├── great_expectations.yml     # GE configuration
│   ├── expectations/              # Expectation suite definitions
│   │   ├── patients_suite.json
│   │   ├── encounters_suite.json
│   │   ├── diagnoses_suite.json
│   │   └── medications_suite.json
│   ├── checkpoints/               # Checkpoint configurations
│   │   └── healthcare_quality_checkpoint.yml
│   └── uncommitted/               # Local artifacts (not in git)
│       └── data_docs/             # Generated HTML reports
│
└── README.md                      # This file
```

---

## Data Quality Checks

### Patients Table (15 Expectations)

**Schema Validation:**
- Column existence and order verification
- Data type validation

**Primary Key Integrity:**
- Uniqueness of patient_id
- No null patient_id values
- Format validation (PAT-XXXXXX pattern)

**Completeness:**
- First name present (98% threshold)
- Last name present (99% threshold)
- Date of birth present (97% threshold)

**Validity:**
- Birth dates in valid range (1900-2024)
- Ages under 120 years (98% threshold)
- Gender in expected value set

**Format Validation:**
- Email format (contains @ symbol, 95% threshold)
- Phone number format validation

**Audit Trail:**
- Created timestamp present

### Encounters Table (9 Expectations)

**Primary Key:**
- Unique encounter_id
- No null encounter_id

**Foreign Key:**
- Valid patient_id reference (95% threshold - allows for orphaned record detection)

**Business Logic:**
- Encounter type in valid set
- Admission date present (98% threshold)
- Discharge date after admission date (90% threshold)

**Financial Validation:**
- Cost values non-negative (97% threshold)
- Cost within reasonable range (0 to 1,000,000)

### Diagnoses Table (10 Expectations)

**Primary Key:**
- Unique diagnosis_id
- No null diagnosis_id

**Foreign Key:**
- Valid encounter_id reference (92% threshold)

**Clinical Coding:**
- ICD-10 code present (85% threshold - critical for billing)
- ICD-10 format validation (A00.0 pattern)

**Data Quality:**
- Severity in valid set (Mild, Moderate, Severe, Critical)
- Diagnosis date within reasonable range (2020-2024)

### Medications Table (8 Expectations)

**Primary Key:**
- Unique medication_id
- No null medication_id

**Foreign Key:**
- Valid encounter_id reference

**Safety-Critical Fields:**
- Medication name present (95% threshold)
- Dosage present (97% threshold)

**Business Logic:**
- End date after start date (90% threshold)
- Frequency in expected value set

**Clinical Documentation:**
- Prescribing physician present

---

## Validation Results

The framework intentionally includes data quality issues to demonstrate detection capabilities:

**Detected Issues by Category:**

| Issue Type | Example | Count | Detection Rate |
|------------|---------|-------|----------------|
| Missing Critical Fields | Null first names | ~20 | 100% |
| Invalid Date Ranges | Discharge before admission | ~200 | 100% |
| Format Violations | Invalid email format | ~50 | 100% |
| Referential Integrity | Orphaned encounter records | ~100 | 100% |
| Out-of-Range Values | Negative costs | ~60 | 100% |
| Logic Violations | Future birth dates | ~50 | 100% |

**Overall Metrics:**
- Total Expectations: 42
- Average Success Rate: 66-70% (intentional failures for demonstration)
- Validation Execution Time: 3-5 seconds
- Tables Validated: 4
- Total Records Validated: ~8,500

---

## Future Enhancements

### Short Term
- Add email/Slack alerting for critical validation failures
- Implement severity-based routing (CRITICAL failures stop pipeline, MEDIUM failures log only)
- Create Grafana dashboard for real-time quality monitoring
- Add data profiling to auto-generate expectations
- Implement data repair scripts for common issues

### Medium Term
- Expand to additional healthcare tables (labs, vitals, claims)
- Add data lineage tracking
- Implement anomaly detection using statistical methods
- Create API endpoint for on-demand validation
- Add support for streaming data validation

### Long Term
- Machine learning-based expectation generation
- Integration with data catalog (DataHub, Amundsen)
- Multi-environment support (dev, staging, prod)
- Distributed validation for large datasets using Spark
- Custom Great Expectations plugins for healthcare-specific validations

---

## About

This project was built as a portfolio demonstration of production-ready data engineering practices, specifically focused on data quality monitoring in healthcare systems.

**Skills Demonstrated:**
- Data quality framework design and implementation
- Python development with production best practices
- SQL database design and query optimization
- Great Expectations framework expertise
- Healthcare data domain knowledge
- CI/CD and scheduler integration patterns
- Technical documentation

**Author:** Abdul-Jaleel Asani    
**Email:** abduljaleelasani@gmail.com  
**Portfolio:** github.com/Asani-A

---

## License

This project is available under the MIT License. See LICENSE file for details.

---

## Acknowledgments

- Great Expectations open-source community
- Healthcare data quality research and case studies
- Data engineering best practices from industry leaders

---

**Note:** This project uses synthetic data generated for demonstration purposes. No real patient data or PHI (Protected Health Information) is included.
