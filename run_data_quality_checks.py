#!/usr/bin/env python3
"""
Healthcare Data Quality Monitoring - Main Orchestration Script

This script orchestrates the complete data quality validation pipeline:
1. Connects to PostgreSQL database
2. Runs all Great Expectations validation suites
3. Logs results to tracking table
4. Generates Data Docs reports
5. Returns appropriate exit codes for scheduler integration

Usage:
    python run_data_quality_checks.py
    
Exit Codes:
    0 - All validations passed
    1 - One or more validations failed (data quality issues detected)
    2 - Script error (configuration, connection, etc.)

This script is designed to be triggered by:
- Apache Airflow DAGs
- Cron jobs
- CI/CD pipelines
- Manual execution
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent))

import great_expectations as gx
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import json
import traceback

# Load environment variables
load_dotenv()

# Configuration
DB_CONNECTION_STRING = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

CHECKPOINT_NAME = "healthcare_quality_checkpoint"


class DataQualityPipeline:
    """Main orchestration class for data quality validation."""
    
    def __init__(self):
        """Initialize the pipeline."""
        self.context = None
        self.db_engine = None
        self.validation_results = None
        self.start_time = datetime.now()
        
    def setup(self):
        """Set up connections and context."""
        try:
            print("=" * 70)
            print("HEALTHCARE DATA QUALITY MONITORING PIPELINE")
            print("=" * 70)
            print(f"Started at: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
            # Initialize Great Expectations context
            print("Initializing Great Expectations context...")
            self.context = gx.get_context()
            print("✓ Great Expectations context loaded")
            
            # Set up database connection
            print("Connecting to PostgreSQL database...")
            self.db_engine = create_engine(DB_CONNECTION_STRING)
            
            # Test connection
            with self.db_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✓ Database connection successful")
            
            return True
            
        except Exception as e:
            print(f"✗ Setup failed: {e}")
            traceback.print_exc()
            return False
    
    def run_validations(self):
        """Execute all validation suites via checkpoint."""
        try:
            print("\n" + "-" * 70)
            print("RUNNING VALIDATIONS")
            print("-" * 70)
            print(f"Checkpoint: {CHECKPOINT_NAME}")
            print("Validating tables: patients, encounters, diagnoses, medications")
            print()
            
            # Run checkpoint
            self.validation_results = self.context.run_checkpoint(
                checkpoint_name=CHECKPOINT_NAME
            )
            
            print("✓ Validations complete")
            return True
            
        except Exception as e:
            print(f"✗ Validation execution failed: {e}")
            traceback.print_exc()
            return False
    
    def log_results(self):
        """Log validation results to database."""
        try:
            print("\n" + "-" * 70)
            print("LOGGING RESULTS")
            print("-" * 70)
            
            for result in self.validation_results['run_results'].values():
                suite_name = result['validation_result']['meta']['expectation_suite_name']
                table_name = suite_name.replace('_suite', '')
                
                # Extract statistics
                stats = result['validation_result']['statistics']
                total = stats['evaluated_expectations']
                successful = stats['successful_expectations']
                failed = stats['unsuccessful_expectations']
                success_pct = (successful / total * 100) if total > 0 else 0
                status = 'PASS' if failed == 0 else 'FAIL'
                
                # Collect failed expectation details
                failed_expectations = []
                for exp_result in result['validation_result']['results']:
                    if not exp_result['success']:
                        failed_expectations.append({
                            'expectation_type': exp_result['expectation_config']['expectation_type'],
                            'column': exp_result['expectation_config']['kwargs'].get('column', 'N/A'),
                            'observed_value': str(exp_result.get('result', {}))[:500],  # Truncate
                        })
                
                # Insert into tracking table
                with self.db_engine.connect() as conn:
                    conn.execute(text("""
                        INSERT INTO data_quality_results (
                            expectation_suite_name,
                            table_name,
                            total_expectations,
                            successful_expectations,
                            failed_expectations,
                            success_percent,
                            validation_status,
                            failed_expectation_details
                        ) VALUES (
                            :suite_name,
                            :table_name,
                            :total,
                            :successful,
                            :failed,
                            :success_pct,
                            :status,
                            :failed_details
                        )
                    """), {
                        'suite_name': suite_name,
                        'table_name': table_name,
                        'total': total,
                        'successful': successful,
                        'failed': failed,
                        'success_pct': round(success_pct, 2),
                        'status': status,
                        'failed_details': json.dumps(failed_expectations)
                    })
                    conn.commit()
                
                print(f"✓ Logged results for {table_name}")
            
            return True
            
        except Exception as e:
            print(f"✗ Failed to log results: {e}")
            traceback.print_exc()
            return False
    
    def generate_data_docs(self):
        """Build Data Docs HTML reports."""
        try:
            print("\n" + "-" * 70)
            print("GENERATING DATA DOCS")
            print("-" * 70)
            
            self.context.build_data_docs()
            
            # Find the data docs path (handles both gx/ and great_expectations/)
            possible_paths = [
                "gx/uncommitted/data_docs/local_site/index.html",
                "great_expectations/uncommitted/data_docs/local_site/index.html"
            ]
            
            docs_path = None
            for path in possible_paths:
                if Path(path).exists():
                    docs_path = Path(path).resolve()
                    break
            
            if docs_path:
                print(f"✓ Data Docs generated")
                print(f"  Location: {docs_path}")
                print(f"  URL: file://{docs_path}")
            else:
                print("✓ Data Docs generated (location unknown)")
            
            return True
            
        except Exception as e:
            print(f"✗ Failed to generate Data Docs: {e}")
            traceback.print_exc()
            return False
    
    def print_summary(self):
        """Print validation summary."""
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        
        overall_success = self.validation_results['success']
        
        for result in self.validation_results['run_results'].values():
            suite_name = result['validation_result']['meta']['expectation_suite_name']
            stats = result['validation_result']['statistics']
            
            total = stats['evaluated_expectations']
            successful = stats['successful_expectations']
            failed = stats['unsuccessful_expectations']
            success_pct = (successful / total * 100) if total > 0 else 0
            
            status_symbol = "✓" if failed == 0 else "✗"
            
            print(f"\n{status_symbol} {suite_name.upper()}")
            print(f"  Expectations: {successful}/{total} passed ({success_pct:.1f}%)")
            
            if failed > 0:
                print(f"  Failed checks:")
                for exp_result in result['validation_result']['results']:
                    if not exp_result['success']:
                        exp_type = exp_result['expectation_config']['expectation_type']
                        column = exp_result['expectation_config']['kwargs'].get('column', 'N/A')
                        # Shorten expectation type for readability
                        exp_type_short = exp_type.replace('expect_column_', '').replace('_', ' ')
                        print(f"    • {column}: {exp_type_short}")
        
        print("\n" + "=" * 70)
        
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        if overall_success:
            print("OVERALL STATUS: ✓ PASS - All data quality checks passed")
            print(f"Duration: {duration:.2f} seconds")
            print("=" * 70)
            return 0  # Success exit code
        else:
            print("OVERALL STATUS: ✗ FAIL - Data quality issues detected")
            print(f"Duration: {duration:.2f} seconds")
            print("=" * 70)
            print("\nAction Required:")
            print("  1. Review failed expectations in Data Docs")
            print("  2. Investigate root causes in source data")
            print("  3. Fix data quality issues or adjust thresholds")
            return 1  # Failure exit code
    
    def cleanup(self):
        """Clean up resources."""
        if self.db_engine:
            self.db_engine.dispose()


def main():
    """Main entry point for the data quality pipeline."""
    pipeline = DataQualityPipeline()
    
    try:
        # Setup
        if not pipeline.setup():
            print("\n✗ Pipeline setup failed")
            return 2
        
        # Run validations
        if not pipeline.run_validations():
            print("\n✗ Validation execution failed")
            return 2
        
        # Log results
        if not pipeline.log_results():
            print("\n✗ Failed to log results (validation completed but logging failed)")
            # Continue anyway - validations ran successfully
        
        # Generate reports
        if not pipeline.generate_data_docs():
            print("\n✗ Failed to generate Data Docs (validation completed)")
            # Continue anyway
        
        # Print summary and return appropriate exit code
        exit_code = pipeline.print_summary()
        
        return exit_code
        
    except KeyboardInterrupt:
        print("\n\n✗ Pipeline interrupted by user")
        return 2
        
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        traceback.print_exc()
        return 2
        
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
