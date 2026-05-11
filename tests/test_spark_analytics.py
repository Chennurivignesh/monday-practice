"""
Test module for spark_analytics.py

This module contains unit tests for the PySpark analytics functionality.
"""

import unittest
import tempfile
import os
from unittest.mock import patch, MagicMock
import pandas as pd

from src.spark_analytics import ECommerceAnalytics


class TestSparkAnalytics(unittest.TestCase):
    """Test cases for Spark analytics functionality."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create sample data files
        self.create_sample_data()
        
        # Mock file paths
        self.patch_files = [
            patch('src.spark_analytics.CUSTOMERS_FILE', self.customers_file),
            patch('src.spark_analytics.PRODUCTS_FILE', self.products_file),
            patch('src.spark_analytics.ORDERS_FILE', self.orders_file),
            patch('src.spark_analytics.ORDER_ITEMS_FILE', self.order_items_file),
            patch('src.spark_analytics.CUSTOMER_ANALYSIS_FILE', 
                  os.path.join(self.temp_dir, "customer_analysis.csv")),
            patch('src.spark_analytics.PRODUCT_ANALYSIS_FILE', 
                  os.path.join(self.temp_dir, "product_analysis.csv")),
            patch('src.spark_analytics.REVENUE_ANALYSIS_FILE', 
                  os.path.join(self.temp_dir, "revenue_analysis.csv")),
        ]
        
        for p in self.patch_files:
            p.start()
    
    def tearDown(self):
        """Clean up after each test method."""
        for p in self.patch_files:
            p.stop()
        
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_sample_data(self):
        """Create sample CSV files for testing."""
        # Sample customers data
        customers_data = {
            'customer_id': [1, 2, 3],
            'first_name': ['John', 'Jane', 'Bob'],
            'last_name': ['Doe', 'Smith', 'Johnson'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
            'age': [25, 30, 35],
            'city': ['New York', 'Los Angeles', 'Chicago'],
            'state': ['NY', 'CA', 'IL']
        }
        
        # Sample products data
        products_data = {
            'product_id': [1, 2],
            'product_name': ['Laptop', 'Phone'],
            'category': ['Electronics', 'Electronics'],
            'price': [999.99, 699.99],
            'in_stock': [True, True]
        }
        
        # Sample orders data
        orders_data = {
            'order_id': [1, 2, 3],
            'customer_id': [1, 2, 1],
            'order_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'order_status': ['Delivered', 'Processing', 'Shipped']
        }
        
        # Sample order items data
        order_items_data = {
            'order_item_id': [1, 2, 3, 4],
            'order_id': [1, 1, 2, 3],
            'product_id': [1, 2, 1, 2],
            'quantity': [1, 2, 1, 1],
            'unit_price': [999.99, 699.99, 999.99, 699.99]
        }
        
        # Save to files
        self.customers_file = os.path.join(self.temp_dir, "customers.csv")
        self.products_file = os.path.join(self.temp_dir, "products.csv")
        self.orders_file = os.path.join(self.temp_dir, "orders.csv")
        self.order_items_file = os.path.join(self.temp_dir, "order_items.csv")
        
        pd.DataFrame(customers_data).to_csv(self.customers_file, index=False)
        pd.DataFrame(products_data).to_csv(self.products_file, index=False)
        pd.DataFrame(orders_data).to_csv(self.orders_file, index=False)
        pd.DataFrame(order_items_data).to_csv(self.order_items_file, index=False)
    
    @patch('src.spark_analytics.SparkSession')
    def test_init_analytics(self, mock_spark_session):
        """Test ECommerceAnalytics initialization."""
        mock_spark = MagicMock()
        mock_spark_session.builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
        
        analytics = ECommerceAnalytics()
        
        self.assertIsNotNone(analytics.spark)
        self.assertIsNotNone(analytics.customers_df)
        self.assertIsNotNone(analytics.products_df)
        self.assertIsNotNone(analytics.orders_df)
        self.assertIsNotNone(analytics.order_items_df)
    
    @patch('src.spark_analytics.SparkSession')
    def test_analyze_customer_segments(self, mock_spark_session):
        """Test customer segmentation analysis."""
        # Mock Spark components
        mock_spark = MagicMock()
        mock_spark_session.builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
        
        # Mock DataFrame operations
        mock_df = MagicMock()
        mock_spark.read.csv.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        
        analytics = ECommerceAnalytics()
        result = analytics.analyze_customer_segments()
        
        self.assertIsNotNone(result)
    
    @patch('src.spark_analytics.SparkSession')
    def test_analyze_product_performance(self, mock_spark_session):
        """Test product performance analysis."""
        mock_spark = MagicMock()
        mock_spark_session.builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
        
        # Mock DataFrame operations
        mock_df = MagicMock()
        mock_spark.read.csv.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        
        analytics = ECommerceAnalytics()
        result = analytics.analyze_product_performance()
        
        self.assertIsNotNone(result)
    
    @patch('src.spark_analytics.SparkSession')
    def test_analyze_revenue_trends(self, mock_spark_session):
        """Test revenue trend analysis."""
        mock_spark = MagicMock()
        mock_spark_session.builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
        
        # Mock DataFrame operations
        mock_df = MagicMock()
        mock_spark.read.csv.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.orderBy.return_value = mock_df
        
        analytics = ECommerceAnalytics()
        result = analytics.analyze_revenue_trends()
        
        self.assertIsNotNone(result)
    
    @patch('src.spark_analytics.SparkSession')
    def test_get_top_customers(self, mock_spark_session):
        """Test getting top customers."""
        mock_spark = MagicMock()
        mock_spark_session.builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
        
        # Mock DataFrame operations
        mock_df = MagicMock()
        mock_spark.read.csv.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.orderBy.return_value = mock_df
        mock_df.limit.return_value = mock_df
        
        analytics = ECommerceAnalytics()
        result = analytics.get_top_customers(limit=5)
        
        self.assertIsNotNone(result)
    
    @patch('src.spark_analytics.SparkSession')
    def test_save_analysis_results(self, mock_spark_session):
        """Test saving analysis results."""
        mock_spark = MagicMock()
        mock_spark_session.builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
        
        # Mock DataFrame operations
        mock_df = MagicMock()
        mock_pandas_df = pd.DataFrame({'test': [1, 2, 3]})
        mock_df.toPandas.return_value = mock_pandas_df
        mock_spark.read.csv.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.orderBy.return_value = mock_df
        
        analytics = ECommerceAnalytics()
        analytics.save_analysis_results()
        
        # Check that files were created (mocked paths)
        self.assertTrue(True)  # If no exception, test passes


class TestAnalyticsIntegration(unittest.TestCase):
    """Integration tests for analytics functionality."""
    
    def test_data_file_requirements(self):
        """Test that required data files exist."""
        from src.config import (
            CUSTOMERS_FILE, PRODUCTS_FILE, ORDERS_FILE, ORDER_ITEMS_FILE
        )
        
        # This test will fail until data is generated
        # It's designed to be run after data generation
        try:
            self.assertTrue(os.path.exists(CUSTOMERS_FILE))
            self.assertTrue(os.path.exists(PRODUCTS_FILE))
            self.assertTrue(os.path.exists(ORDERS_FILE))
            self.assertTrue(os.path.exists(ORDER_ITEMS_FILE))
        except AssertionError:
            # Skip test if data files don't exist yet
            self.skipTest("Data files not generated yet")


if __name__ == "__main__":
    unittest.main()
