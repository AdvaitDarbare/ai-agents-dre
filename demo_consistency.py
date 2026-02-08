"""
Demo: Consistency (Logic) Checks

This script demonstrates the new SQL-based consistency checks that validate
business logic rules against transaction data.

Consistency Rules Defined in config/expectations/transactions.yaml:
1. No Future Transactions: timestamp <= now()
2. High Value Flag Check: amount < 5000 OR (amount >= 5000 AND status = 'completed')
"""

from src.tools.data_profiler import DataProfiler
from pathlib import Path


def demo_consistency_checks():
    """Demonstrate consistency checks on various test files."""
    
    print("\n" + "=" * 80)
    print("🔍 CONSISTENCY CHECKS DEMO - Business Logic Validation")
    print("=" * 80)
    
    profiler = DataProfiler(contracts_path='config/expectations')
    
    # Test 1: Perfect Data
    print("\n📊 Test 1: Perfect Transactions (All Rules Pass)")
    print("-" * 80)
    errors = profiler.analyze('data/landing/transactions_perfect.csv', 'transactions')
    
    if not errors:
        print("✅ All data quality checks passed!")
    else:
        for error in errors:
            print(error)
    
    # Test 2: Future Timestamp Violation
    print("\n\n📊 Test 2: Future Timestamp Violation")
    print("-" * 80)
    print("⚠️  Data contains a transaction with a future timestamp")
    print("   Rule: 'No Future Transactions' → timestamp <= now()")
    print()
    
    errors = profiler.analyze('data/landing/transactions_future.csv', 'transactions')
    
    consistency_errors = [e for e in errors if "CONSISTENCY" in e]
    for error in consistency_errors:
        print(error)
    
    # Summary
    print("\n" + "=" * 80)
    print("📋 SUMMARY - Consistency Check Capabilities")
    print("=" * 80)
    print("""
The Consistency tool validates business logic rules using custom SQL conditions:

✓ Temporal Rules:
  - No future dates: timestamp <= now()
  - Date ranges: start_date < end_date
  - Age calculations: (current_date - birth_date) >= 18 years

✓ Cross-Column Rules:
  - High value transactions must be completed
  - Refunds cannot exceed original amount
  - Discount percentage must be between 0-100%

✓ Custom Business Logic:
  - Any SQL WHERE clause can be validated
  - Counts rows that violate the rule
  - Reports clear error messages with row counts

This straightforward approach ensures data meets business requirements
before loading into Apache Doris.
""")
    
    print("=" * 80)
    print("✅ Demo Complete!")
    print("=" * 80)


if __name__ == '__main__':
    demo_consistency_checks()
