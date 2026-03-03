"""
Run data quality validations and generate Data Docs.

This script:
1. Executes all validation checks via the checkpoint
2. Generates HTML Data Docs reports
3. Prints a summary of results
4. Logs results to the data_quality_results table
"""

import great_expectations as gx
from datetime import datetime
from sqlalchemy import create_engine, text
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Database connection for logging results
DB_CONNECTION_STRING = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def log_validation_results(validation_results):
    """Log validation results to PostgreSQL for tracking over time."""
    engine = create_engine(DB_CONNECTION_STRING)
    
    for result in validation_results['run_results'].values():
        suite_name = result['validation_result']['meta']['expectation_suite_name']
        
        # Extract table name from suite name (e.g., "patients_suite" -> "patients")
        table_name = suite_name.replace('_suite', '')
        
        # Get statistics
        stats = result['validation_result']['statistics']
        total = stats['evaluated_expectations']
        successful = stats['successful_expectations']
        failed = stats['unsuccessful_expectations']
        success_pct = (successful / total * 100) if total > 0 else 0
        
        # Determine status
        status = 'PASS' if failed == 0 else 'FAIL'
        
        # Get failed expectation details
        failed_expectations = []
        for exp_result in result['validation_result']['results']:
            if not exp_result['success']:
                failed_expectations.append({
                    'expectation_type': exp_result['expectation_config']['expectation_type'],
                    'column': exp_result['expectation_config']['kwargs'].get('column', 'N/A'),
                    'observed_value': str(exp_result.get('result', {})),
                })
        
        # Insert into database
        with engine.connect() as conn:
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
        
        print(f"  ✓ Logged results for {table_name}")


def print_validation_summary(validation_results):
    """Print a readable summary of validation results."""
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    
    overall_success = validation_results['success']
    
    for result in validation_results['run_results'].values():
        suite_name = result['validation_result']['meta']['expectation_suite_name']
        stats = result['validation_result']['statistics']
        
        total = stats['evaluated_expectations']
        successful = stats['successful_expectations']
        failed = stats['unsuccessful_expectations']
        success_pct = (successful / total * 100) if total > 0 else 0
        
        status_symbol = "✓" if failed == 0 else "✗"
        
        print(f"\n{status_symbol} {suite_name}")
        print(f"  Total Expectations: {total}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Success Rate: {success_pct:.1f}%")
        
        # Show failed expectations
        if failed > 0:
            print(f"\n  Failed Expectations:")
            for exp_result in result['validation_result']['results']:
                if not exp_result['success']:
                    exp_type = exp_result['expectation_config']['expectation_type']
                    column = exp_result['expectation_config']['kwargs'].get('column', 'N/A')
                    print(f"    - {exp_type} [{column}]")
    
    print("\n" + "=" * 60)
    print(f"OVERALL STATUS: {'PASS ✓' if overall_success else 'FAIL ✗'}")
    print("=" * 60)


if __name__ == "__main__":
    print("Starting data quality validation...")
    print("=" * 60)
    
    # Initialize GE context
    context = gx.get_context()
    
    # Run the checkpoint
    print("\nRunning checkpoint: healthcare_quality_checkpoint")
    print("This may take a moment...\n")
    
    checkpoint_result = context.run_checkpoint(
        checkpoint_name="healthcare_quality_checkpoint"
    )
    
    # Print summary
    print_validation_summary(checkpoint_result)
    
    # Log results to database
    print("\nLogging results to data_quality_results table...")
    log_validation_results(checkpoint_result)
    
    # Build Data Docs
    print("\nBuilding Data Docs...")
    context.build_data_docs()
    
    print("\n" + "=" * 60)
    print("✓ Validation complete!")
    print("=" * 60)
    print("\nData Docs available at:")
    print("  great_expectations/uncommitted/data_docs/local_site/index.html")
    print("\nOpen in browser:")
    print("  file://$(pwd)/great_expectations/uncommitted/data_docs/local_site/index.html")
