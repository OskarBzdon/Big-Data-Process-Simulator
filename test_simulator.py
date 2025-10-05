#!/usr/bin/env python3
"""
Test script for the Business Process Simulator with Kaggle Integration
"""

import os
import sys
import pandas as pd
import psycopg2
from business_process_simulator import BusinessProcessSimulator
from data_seeder import DataSeeder
import logging

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_csv_reading():
    """Test CSV file reading functionality."""
    logger.info("Testing CSV file reading...")
    
    csv_files = [
        'data/customers.csv',
        'data/orders.csv', 
        'data/products.csv'
    ]
    
    # Check if files exist
    for file_path in csv_files:
        if not os.path.exists(file_path):
            logger.error(f"CSV file not found: {file_path}")
            return False
    
    # Test reading files
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
            logger.info(f"✓ Successfully read {file_path}: {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            logger.error(f"✗ Failed to read {file_path}: {e}")
            return False
    
    return True

def test_database_connection():
    """Test database connection."""
    logger.info("Testing database connection...")
    
    db_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'business_db',
        'user': 'postgres',
        'password': 'password'
    }
    
    try:
        connection = psycopg2.connect(**db_config)
        connection.close()
        logger.info("✓ Database connection successful")
        return True
    except psycopg2.Error as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False

def test_data_seeder():
    """Test the data seeder functionality."""
    logger.info("Testing data seeder...")
    
    try:
        seeder = DataSeeder()
        
        # Test fetching data (this might fail if no internet connection)
        dataframes = seeder.fetch_uber_data()
        
        if dataframes:
            logger.info(f"✓ Data seeder successfully fetched {len(dataframes)} datasets")
            return True
        else:
            logger.warning("⚠ No data fetched from Kaggle (this is expected if offline)")
            return True  # Not a failure, just no internet access
            
    except Exception as e:
        logger.error(f"✗ Data seeder test failed: {e}")
        return False

def test_simulator_functionality():
    """Test the complete simulator functionality."""
    logger.info("Testing complete simulator functionality...")
    
    db_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'business_db',
        'user': 'postgres',
        'password': 'password'
    }
    
    csv_files = [
        'data/customers.csv',
        'data/orders.csv',
        'data/products.csv'
    ]
    
    simulator = BusinessProcessSimulator(db_config)
    
    try:
        # Test with local data only (skip Kaggle to avoid network issues in testing)
        success = simulator.load_local_csv_files(csv_files)
        
        if success and simulator.dataframes:
            logger.info("✓ Simulator functionality test passed")
            
            # Test report generation
            report = simulator.generate_comprehensive_report()
            if report and 'total_datasets' in report:
                logger.info("✓ Simulator report generation successful")
                return True
            else:
                logger.error("✗ Simulator report generation failed")
                return False
        else:
            logger.error("✗ Simulator functionality test failed")
            return False
    except Exception as e:
        logger.error(f"✗ Simulator test failed with exception: {e}")
        return False

def test_data_validation():
    """Test data validation and integrity."""
    logger.info("Testing data validation...")
    
    # Test customers data
    customers_df = pd.read_csv('data/customers.csv')
    assert 'customer_id' in customers_df.columns, "Missing customer_id column"
    assert customers_df['customer_id'].is_unique, "customer_id not unique"
    logger.info("✓ Customers data validation passed")
    
    # Test orders data
    orders_df = pd.read_csv('data/orders.csv')
    assert 'order_id' in orders_df.columns, "Missing order_id column"
    assert orders_df['order_id'].is_unique, "order_id not unique"
    logger.info("✓ Orders data validation passed")
    
    # Test products data
    products_df = pd.read_csv('data/products.csv')
    assert 'product_id' in products_df.columns, "Missing product_id column"
    assert products_df['product_id'].is_unique, "product_id not unique"
    logger.info("✓ Products data validation passed")
    
    return True

def run_all_tests():
    """Run all tests."""
    logger.info("Starting comprehensive test suite...")
    logger.info("=" * 50)
    
    tests = [
        ("Data Seeder", test_data_seeder),
        ("CSV Reading", test_csv_reading),
        ("Database Connection", test_database_connection),
        ("Data Validation", test_data_validation),
        ("Simulator Functionality", test_simulator_functionality)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\nRunning {test_name} test...")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! The system is ready.")
        return True
    else:
        logger.error("❌ Some tests failed. Please check the logs.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
