"""
Configuration settings for the e-commerce data pipeline project.

This module contains all configuration parameters including file paths,
data generation settings, and Spark configuration.
"""

import os
from pathlib import Path
from typing import Dict, Any

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Data generation settings
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 500
NUM_ORDERS = 5000

# File paths for generated data
CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
PRODUCTS_FILE = RAW_DATA_DIR / "products.csv"
ORDERS_FILE = RAW_DATA_DIR / "orders.csv"
ORDER_ITEMS_FILE = RAW_DATA_DIR / "order_items.csv"

# Spark configuration
SPARK_CONFIG: Dict[str, Any] = {
    "app_name": "E-Commerce Analytics",
    "master": "local[*]",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
}

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Data analysis output files
CUSTOMER_ANALYSIS_FILE = PROCESSED_DATA_DIR / "customer_analysis.csv"
PRODUCT_ANALYSIS_FILE = PROCESSED_DATA_DIR / "product_analysis.csv"
REVENUE_ANALYSIS_FILE = PROCESSED_DATA_DIR / "revenue_analysis.csv"

# Faker seed for reproducible data
FAKER_SEED = 42

# Date range for data generation
START_DATE = "2023-01-01"
END_DATE = "2024-12-31"

# Price ranges
MIN_PRODUCT_PRICE = 10.0
MAX_PRODUCT_PRICE = 1000.0

# Order quantities
MIN_ORDER_QUANTITY = 1
MAX_ORDER_QUANTITY = 10
