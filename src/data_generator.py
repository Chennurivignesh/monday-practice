"""
Data generation module for creating fake e-commerce data.

This module generates realistic fake customer, product, and order data
using the Faker library and saves them as CSV files.
"""

import logging
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any

import faker
import pandas as pd

from config import (
    NUM_CUSTOMERS,
    NUM_PRODUCTS,
    NUM_ORDERS,
    CUSTOMERS_FILE,
    PRODUCTS_FILE,
    ORDERS_FILE,
    ORDER_ITEMS_FILE,
    FAKER_SEED,
    START_DATE,
    END_DATE,
    MIN_PRODUCT_PRICE,
    MAX_PRODUCT_PRICE,
    MIN_ORDER_QUANTITY,
    MAX_ORDER_QUANTITY,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize Faker with seed for reproducibility
fake = faker.Faker()
fake.seed_instance(FAKER_SEED)
random.seed(FAKER_SEED)


def generate_customers(num_customers: int = NUM_CUSTOMERS) -> List[Dict[str, Any]]:
    """
    Generate fake customer data.
    
    Args:
        num_customers: Number of customers to generate
        
    Returns:
        List of customer dictionaries
        
    Raises:
        ValueError: If num_customers is less than 1
    """
    if num_customers < 1:
        raise ValueError("Number of customers must be at least 1")
    
    logger.info(f"Generating {num_customers} customers...")
    
    customers = []
    for i in range(1, num_customers + 1):
        customer = {
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address().replace("\n", ", "),
            "city": fake.city(),
            "state": fake.state(),
            "zip_code": fake.zipcode(),
            "country": fake.country(),
            "registration_date": fake.date_between_dates(
                date_start=datetime.strptime(START_DATE, "%Y-%m-%d"),
                date_end=datetime.strptime(END_DATE, "%Y-%m-%d")
            ),
            "age": random.randint(18, 80),
            "gender": random.choice(["Male", "Female", "Other"]),
        }
        customers.append(customer)
    
    logger.info(f"Successfully generated {len(customers)} customers")
    return customers


def generate_products(num_products: int = NUM_PRODUCTS) -> List[Dict[str, Any]]:
    """
    Generate fake product data.
    
    Args:
        num_products: Number of products to generate
        
    Returns:
        List of product dictionaries
        
    Raises:
        ValueError: If num_products is less than 1
    """
    if num_products < 1:
        raise ValueError("Number of products must be at least 1")
    
    logger.info(f"Generating {num_products} products...")
    
    categories = [
        "Electronics", "Clothing", "Books", "Home & Garden", "Sports",
        "Toys", "Food", "Beauty", "Automotive", "Health"
    ]
    
    products = []
    for i in range(1, num_products + 1):
        product = {
            "product_id": i,
            "product_name": fake.catch_phrase(),
            "category": random.choice(categories),
            "price": round(random.uniform(MIN_PRODUCT_PRICE, MAX_PRODUCT_PRICE), 2),
            "description": fake.text(max_nb_chars=200),
            "weight": round(random.uniform(0.1, 50.0), 2),
            "dimensions": f"{random.randint(1, 100)}x{random.randint(1, 100)}x{random.randint(1, 100)}",
            "created_date": fake.date_between_dates(
                date_start=datetime.strptime(START_DATE, "%Y-%m-%d"),
                date_end=datetime.strptime(END_DATE, "%Y-%m-%d")
            ),
            "in_stock": random.choice([True, False]),
            "stock_quantity": random.randint(0, 1000),
        }
        products.append(product)
    
    logger.info(f"Successfully generated {len(products)} products")
    return products


def generate_orders(
    num_orders: int = NUM_ORDERS,
    num_customers: int = NUM_CUSTOMERS,
    num_products: int = NUM_PRODUCTS
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Generate fake order and order item data.
    
    Args:
        num_orders: Number of orders to generate
        num_customers: Total number of customers available
        num_products: Total number of products available
        
    Returns:
        Tuple of (orders_list, order_items_list)
        
    Raises:
        ValueError: If any parameter is less than 1
    """
    if num_orders < 1 or num_customers < 1 or num_products < 1:
        raise ValueError("All parameters must be at least 1")
    
    logger.info(f"Generating {num_orders} orders...")
    
    orders = []
    order_items = []
    
    for i in range(1, num_orders + 1):
        # Generate order
        order_date = fake.date_between_dates(
            date_start=datetime.strptime(START_DATE, "%Y-%m-%d"),
            date_end=datetime.strptime(END_DATE, "%Y-%m-%d")
        )
        
        order = {
            "order_id": i,
            "customer_id": random.randint(1, num_customers),
            "order_date": order_date,
            "shipping_address": fake.address().replace("\n", ", "),
            "shipping_city": fake.city(),
            "shipping_state": fake.state(),
            "shipping_zip": fake.zipcode(),
            "shipping_method": random.choice(["Standard", "Express", "Overnight"]),
            "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "Apple Pay"]),
            "order_status": random.choice(["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]),
        }
        orders.append(order)
        
        # Generate order items (1-5 items per order)
        num_items = random.randint(1, 5)
        used_products = set()
        
        for item_num in range(num_items):
            product_id = random.randint(1, num_products)
            while product_id in used_products and len(used_products) < num_products:
                product_id = random.randint(1, num_products)
            used_products.add(product_id)
            
            order_item = {
                "order_item_id": len(order_items) + 1,
                "order_id": i,
                "product_id": product_id,
                "quantity": random.randint(MIN_ORDER_QUANTITY, MAX_ORDER_QUANTITY),
                "unit_price": round(random.uniform(MIN_PRODUCT_PRICE, MAX_PRODUCT_PRICE), 2),
            }
            order_items.append(order_item)
    
    logger.info(f"Successfully generated {len(orders)} orders and {len(order_items)} order items")
    return orders, order_items


def save_data_to_csv() -> None:
    """
    Generate all data and save to CSV files.
    
    This function orchestrates the entire data generation process
    and saves the results to CSV files in the data/raw directory.
    
    Raises:
        Exception: If there's an error saving any file
    """
    try:
        logger.info("Starting data generation process...")
        
        # Generate data
        customers = generate_customers()
        products = generate_products()
        orders, order_items = generate_orders()
        
        # Save to CSV files
        logger.info("Saving data to CSV files...")
        
        pd.DataFrame(customers).to_csv(CUSTOMERS_FILE, index=False)
        logger.info(f"Customers saved to {CUSTOMERS_FILE}")
        
        pd.DataFrame(products).to_csv(PRODUCTS_FILE, index=False)
        logger.info(f"Products saved to {PRODUCTS_FILE}")
        
        pd.DataFrame(orders).to_csv(ORDERS_FILE, index=False)
        logger.info(f"Orders saved to {ORDERS_FILE}")
        
        pd.DataFrame(order_items).to_csv(ORDER_ITEMS_FILE, index=False)
        logger.info(f"Order items saved to {ORDER_ITEMS_FILE}")
        
        logger.info("Data generation completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in data generation: {str(e)}")
        raise


def main() -> None:
    """
    Main function to run the data generation process.
    """
    save_data_to_csv()


if __name__ == "__main__":
    main()
