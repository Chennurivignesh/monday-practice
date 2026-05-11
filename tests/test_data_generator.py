"""
Test module for data_generator.py

This module contains unit tests for the data generation functionality.
"""

import unittest
import tempfile
import os
from unittest.mock import patch, MagicMock
import pandas as pd

from src.data_generator import (
    generate_customers,
    generate_products,
    generate_orders,
    save_data_to_csv
)


class TestDataGenerator(unittest.TestCase):
    """Test cases for data generation functions."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.num_test_customers = 10
        self.num_test_products = 5
        self.num_test_orders = 20
    
    def tearDown(self):
        """Clean up after each test method."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_generate_customers_valid_input(self):
        """Test generate_customers with valid input."""
        customers = generate_customers(self.num_test_customers)
        
        self.assertEqual(len(customers), self.num_test_customers)
        self.assertIn("customer_id", customers[0])
        self.assertIn("first_name", customers[0])
        self.assertIn("email", customers[0])
        self.assertIn("age", customers[0])
        self.assertTrue(18 <= customers[0]["age"] <= 80)
    
    def test_generate_customers_invalid_input(self):
        """Test generate_customers with invalid input."""
        with self.assertRaises(ValueError):
            generate_customers(0)
        
        with self.assertRaises(ValueError):
            generate_customers(-5)
    
    def test_generate_products_valid_input(self):
        """Test generate_products with valid input."""
        products = generate_products(self.num_test_products)
        
        self.assertEqual(len(products), self.num_test_products)
        self.assertIn("product_id", products[0])
        self.assertIn("product_name", products[0])
        self.assertIn("category", products[0])
        self.assertIn("price", products[0])
        self.assertTrue(10.0 <= products[0]["price"] <= 1000.0)
    
    def test_generate_products_invalid_input(self):
        """Test generate_products with invalid input."""
        with self.assertRaises(ValueError):
            generate_products(0)
    
    def test_generate_orders_valid_input(self):
        """Test generate_orders with valid input."""
        orders, order_items = generate_orders(
            self.num_test_orders,
            self.num_test_customers,
            self.num_test_products
        )
        
        self.assertEqual(len(orders), self.num_test_orders)
        self.assertIn("order_id", orders[0])
        self.assertIn("customer_id", orders[0])
        self.assertIn("order_date", orders[0])
        
        # Check that all order items reference valid orders
        for item in order_items:
            self.assertIn(item["order_id"], [order["order_id"] for order in orders])
            self.assertIn(item["product_id"], range(1, self.num_test_products + 1))
    
    def test_generate_orders_invalid_input(self):
        """Test generate_orders with invalid input."""
        with self.assertRaises(ValueError):
            generate_orders(0, 10, 5)
        
        with self.assertRaises(ValueError):
            generate_orders(10, 0, 5)
        
        with self.assertRaises(ValueError):
            generate_orders(10, 5, 0)
    
    @patch('src.data_generator.CUSTOMERS_FILE')
    @patch('src.data_generator.PRODUCTS_FILE')
    @patch('src.data_generator.ORDERS_FILE')
    @patch('src.data_generator.ORDER_ITEMS_FILE')
    def test_save_data_to_csv(self, mock_order_items_file, mock_orders_file, 
                             mock_products_file, mock_customers_file):
        """Test save_data_to_csv function."""
        # Mock file paths
        mock_customers_file.__str__ = lambda: os.path.join(self.temp_dir, "customers.csv")
        mock_products_file.__str__ = lambda: os.path.join(self.temp_dir, "products.csv")
        mock_orders_file.__str__ = lambda: os.path.join(self.temp_dir, "orders.csv")
        mock_order_items_file.__str__ = lambda: os.path.join(self.temp_dir, "order_items.csv")
        
        # Run the function
        save_data_to_csv()
        
        # Check that files were created
        self.assertTrue(os.path.exists(mock_customers_file.__str__()))
        self.assertTrue(os.path.exists(mock_products_file.__str__()))
        self.assertTrue(os.path.exists(mock_orders_file.__str__()))
        self.assertTrue(os.path.exists(mock_order_items_file.__str__()))
        
        # Check file contents
        customers_df = pd.read_csv(mock_customers_file.__str__())
        self.assertGreater(len(customers_df), 0)
        self.assertIn("customer_id", customers_df.columns)


class TestDataIntegration(unittest.TestCase):
    """Integration tests for data generation."""
    
    def test_data_relationships(self):
        """Test that generated data maintains proper relationships."""
        # Generate small dataset
        customers = generate_customers(5)
        products = generate_products(3)
        orders, order_items = generate_orders(10, 5, 3)
        
        # Check customer IDs in orders
        customer_ids = {customer["customer_id"] for customer in customers}
        order_customer_ids = {order["customer_id"] for order in orders}
        self.assertTrue(order_customer_ids.issubset(customer_ids))
        
        # Check product IDs in order items
        product_ids = {product["product_id"] for product in products}
        order_item_product_ids = {item["product_id"] for item in order_items}
        self.assertTrue(order_item_product_ids.issubset(product_ids))
    
    def test_data_quality(self):
        """Test data quality and consistency."""
        customers = generate_customers(10)
        products = generate_products(10)
        
        # Check for duplicate customer IDs
        customer_ids = [c["customer_id"] for c in customers]
        self.assertEqual(len(customer_ids), len(set(customer_ids)))
        
        # Check for duplicate product IDs
        product_ids = [p["product_id"] for p in products]
        self.assertEqual(len(product_ids), len(set(product_ids)))
        
        # Check email format
        for customer in customers:
            self.assertIn("@", customer["email"])
            self.assertIn(".", customer["email"])


if __name__ == "__main__":
    unittest.main()
