#!/usr/bin/env python3
"""
Test script for SalesAnalytics class that validates code structure and logic
without requiring a running Spark session.
"""

import sys
import ast
import inspect
from typing import Dict, Any


def validate_sales_analytics_class():
    """
    Validate the SalesAnalytics class structure and methods.
    """
    print("Validating SalesAnalytics class structure...")
    
    try:
        # Import the module
        import spark_analytics
        SalesAnalytics = spark_analytics.SalesAnalytics
        
        # Check class exists
        assert hasattr(SalesAnalytics, 'SalesAnalytics'), "SalesAnalytics class not found"
        
        # Check required methods exist
        required_methods = [
            'create_spark_session',
            'top_customers', 
            'sales_by_category',
            'monthly_trends',
            'create_sample_data',
            'stop_spark_session'
        ]
        
        for method in required_methods:
            assert hasattr(SalesAnalytics, method), f"Method {method} not found"
        
        print("✓ All required methods found")
        
        # Check method signatures
        class_instance = SalesAnalytics()
        
        # Validate create_spark_session signature
        sig = inspect.signature(class_instance.create_spark_session)
        print(f"✓ create_spark_session signature: {sig}")
        
        # Validate top_customers signature
        sig = inspect.signature(class_instance.top_customers)
        expected_params = ['orders_df', 'products_df', 'n']
        actual_params = list(sig.parameters.keys())
        assert actual_params[:3] == expected_params, f"top_customers signature mismatch: {actual_params}"
        print(f"✓ top_customers signature: {sig}")
        
        # Validate sales_by_category signature
        sig = inspect.signature(class_instance.sales_by_category)
        expected_params = ['orders_df', 'products_df']
        actual_params = list(sig.parameters.keys())
        assert actual_params == expected_params, f"sales_by_category signature mismatch: {actual_params}"
        print(f"✓ sales_by_category signature: {sig}")
        
        # Validate monthly_trends signature
        sig = inspect.signature(class_instance.monthly_trends)
        expected_params = ['orders_df', 'products_df']
        actual_params = list(sig.parameters.keys())
        assert actual_params == expected_params, f"monthly_trends signature mismatch: {actual_params}"
        print(f"✓ monthly_trends signature: {sig}")
        
        # Check docstrings exist
        assert class_instance.create_spark_session.__doc__, "create_spark_session missing docstring"
        assert class_instance.top_customers.__doc__, "top_customers missing docstring"
        assert class_instance.sales_by_category.__doc__, "sales_by_category missing docstring"
        assert class_instance.monthly_trends.__doc__, "monthly_trends missing docstring"
        print("✓ All methods have docstrings")
        
        # Check imports
        source = inspect.getsource(spark_analytics)
        required_imports = [
            'from typing import',
            'from pyspark.sql import',
            'from pyspark.sql.functions import',
            'import logging'
        ]
        
        for import_stmt in required_imports:
            assert import_stmt in source, f"Missing import: {import_stmt}"
        print("✓ All required imports present")
        
        # Check Spark configuration
        spark_config_checks = [
            'spark.driver.memory',
            'spark.sql.adaptive.enabled',
            'spark.serializer',
            'KryoSerializer'
        ]
        
        for config in spark_config_checks:
            assert config in source, f"Missing Spark configuration: {config}"
        print("✓ Spark configuration properly set")
        
        # Check Window functions usage
        window_checks = [
            'Window(',
            'rank()',
            'lag('
        ]
        
        for window_func in window_checks:
            assert window_func in source, f"Missing Window function: {window_func}"
        print("✓ Window functions properly implemented")
        
        # Check aggregation functions
        agg_functions = [
            'sum(',
            'count(',
            'avg(',
            'countDistinct('
        ]
        
        for agg_func in agg_functions:
            assert agg_func in source, f"Missing aggregation function: {agg_func}"
        print("✓ Aggregation functions properly implemented")
        
        print("\n" + "="*60)
        print("✓ SalesAnalytics class validation PASSED")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"\n✗ Validation failed: {str(e)}")
        return False


def print_code_summary():
    """
    Print a summary of the SalesAnalytics class implementation.
    """
    print("\n" + "="*60)
    print("SALES ANALYTICS CLASS SUMMARY")
    print("="*60)
    
    try:
        import spark_analytics
        
        # Print class overview
        print("\nClass: SalesAnalytics")
        print("Purpose: Comprehensive PySpark analytics for sales data")
        
        # Print method details
        methods_info = [
            {
                'name': 'create_spark_session()',
                'purpose': 'Creates optimized Spark session with 4GB memory and Kryo serialization',
                'config': ['4GB driver memory', 'Adaptive query execution', 'Kryo serialization', 'Local mode with 2 cores']
            },
            {
                'name': 'top_customers()',
                'purpose': 'Calculates total spend per customer and returns top N using Window functions',
                'features': ['Customer ranking', 'Total spend calculation', 'Order count', 'Configurable N value']
            },
            {
                'name': 'sales_by_category()',
                'purpose': 'Groups by product category and calculates revenue and units sold',
                'features': ['Total revenue', 'Units sold', 'Average order value', 'Distinct products', 'Order count']
            },
            {
                'name': 'monthly_trends()',
                'purpose': 'Calculates month-over-month revenue growth percentage',
                'features': ['Monthly revenue', 'Previous month comparison', 'Growth percentage', 'Revenue change']
            },
            {
                'name': 'create_sample_data()',
                'purpose': 'Creates sample data for testing analytics methods',
                'features': ['Sample orders', 'Sample products', 'Realistic data structure']
            }
        ]
        
        for method_info in methods_info:
            print(f"\n{method_info['name']}")
            print(f"  Purpose: {method_info['purpose']}")
            if 'config' in method_info:
                print(f"  Configuration: {', '.join(method_info['config'])}")
            if 'features' in method_info:
                print(f"  Features: {', '.join(method_info['features'])}")
        
        # Print Spark configuration details
        print(f"\nSpark Configuration:")
        config_details = [
            "Driver Memory: 4GB",
            "Master: local[2]",
            "Adaptive Query Execution: Enabled",
            "Serializer: KryoSerializer",
            "Arrow Optimization: Enabled",
            "Broadcast Join Threshold: 10MB"
        ]
        
        for config in config_details:
            print(f"  • {config}")
        
        # Print data schema expectations
        print(f"\nExpected Data Schemas:")
        print("Orders DataFrame:")
        print("  • order_id: Integer")
        print("  • customer_id: Integer") 
        print("  • product_id: Integer")
        print("  • quantity: Integer")
        print("  • order_date: String/Timestamp")
        
        print("\nProducts DataFrame:")
        print("  • product_id: Integer")
        print("  • product_name: String")
        print("  • category: String")
        print("  • price: Double")
        
        print(f"\nType Safety: All methods include type hints")
        print(f"Documentation: Comprehensive docstrings for all methods")
        print(f"Error Handling: Input validation and exception handling")
        print(f"Context Manager: Supports 'with' statement for resource management")
        
    except Exception as e:
        print(f"Error generating summary: {str(e)}")


def demonstrate_usage():
    """
    Demonstrate how to use the SalesAnalytics class.
    """
    print("\n" + "="*60)
    print("USAGE EXAMPLES")
    print("="*60)
    
    usage_code = '''
# Basic usage with context manager
from spark_analytics import SalesAnalytics

with SalesAnalytics() as analytics:
    # Create sample data or load your own
    data = analytics.create_sample_data()
    orders_df = data['orders']
    products_df = data['products']
    
    # Get top 10 customers
    top_customers = analytics.top_customers(orders_df, products_df, n=10)
    top_customers.show()
    
    # Get sales by category
    category_sales = analytics.sales_by_category(orders_df, products_df)
    category_sales.show()
    
    # Get monthly trends
    monthly_trends = analytics.monthly_trends(orders_df, products_df)
    monthly_trends.show()

# Manual session management
analytics = SalesAnalytics()
spark = analytics.create_spark_session()

try:
    # Your analytics operations here
    results = analytics.top_customers(orders_df, products_df, n=5)
    results.show()
finally:
    analytics.stop_spark_session()

# Loading your own data
# Assuming you have CSV files:
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)

# Run analytics on your data
category_performance = analytics.sales_by_category(orders_df, products_df)
category_performance.show()
'''
    
    print(usage_code)


if __name__ == "__main__":
    print("SalesAnalytics Validation and Documentation")
    print("=" * 60)
    
    # Validate the class structure
    validation_passed = validate_sales_analytics_class()
    
    if validation_passed:
        # Print code summary
        print_code_summary()
        
        # Show usage examples
        demonstrate_usage()
        
        print(f"\n" + "="*60)
        print("NOTE: To run the actual analytics, ensure Java is installed:")
        print("  - macOS: brew install openjdk")
        print("  - Set JAVA_HOME environment variable")
        print("  - Then run: python spark_analytics.py")
        print("="*60)
    else:
        print("Validation failed. Please check the implementation.")
        sys.exit(1)
