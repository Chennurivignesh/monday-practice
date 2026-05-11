# PySpark Sales Analytics

A comprehensive PySpark analytics class for sales data processing with optimized Spark configuration and advanced analytics capabilities.

## Features

### 🔧 Spark Configuration
- **4GB Driver Memory**: Optimized for local analytics
- **Adaptive Query Execution**: Automatic query optimization
- **Kryo Serialization**: Better performance for data serialization
- **Local Mode**: 2 cores for efficient local processing
- **Arrow Optimization**: Faster Python-Java communication

### 📊 Analytics Methods

#### 1. `create_spark_session()`
Creates an optimized Spark session with all performance configurations.

#### 2. `top_customers(orders_df, products_df, n=10)`
- Calculates total spend per customer
- Uses Window functions for ranking
- Returns top N customers by spending

#### 3. `sales_by_category(orders_df, products_df)`
- Groups sales by product category
- Calculates total revenue and units sold
- Includes average order value and distinct product counts

#### 4. `monthly_trends(orders_df, products_df)`
- Calculates month-over-month revenue growth
- Uses Window functions with lag operations
- Handles null values and percentage calculations

#### 5. `create_sample_data()`
- Generates realistic sample data for testing
- Proper schema definitions
- Ready-to-use test datasets

## Requirements

```bash
# Install Java (required for PySpark)
brew install openjdk
export JAVA_HOME=$(/usr/libexec/java_home)

# Install PySpark
pip install pyspark
```

## Usage

### Basic Usage with Context Manager

```python
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
```

### Manual Session Management

```python
analytics = SalesAnalytics()
spark = analytics.create_spark_session()

try:
    # Your analytics operations here
    results = analytics.top_customers(orders_df, products_df, n=5)
    results.show()
finally:
    analytics.stop_spark_session()
```

### Loading Your Own Data

```python
# Load CSV files
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)

# Run analytics
category_performance = analytics.sales_by_category(orders_df, products_df)
category_performance.show()
```

## Expected Data Schemas

### Orders DataFrame
```
order_id: Integer
customer_id: Integer
product_id: Integer
quantity: Integer
order_date: String/Timestamp
```

### Products DataFrame
```
product_id: Integer
product_name: String
category: String
price: Double
```

## Output Examples

### Top Customers
```
+-----------+------------------+-----------+----+
|customer_id|        total_spend|order_count|rank|
+-----------+------------------+-----------+----+
|        101|           2999.96|          3|   1|
|        102|            899.97|          2|   2|
|        103|            599.98|          2|   3|
+-----------+------------------+-----------+----+
```

### Sales by Category
```
+-----------+-------------+---------------+----------------+------------------+----------+
|   category| total_revenue|total_units_sold|avg_order_value|distinct_products|order_count|
+-----------+-------------+---------------+----------------+------------------+----------+
|Electronics|      2999.96|             14|          214.28|                 4|         6|
|  Furniture|       999.96|              5|          199.99|                 3|         3|
|Appliances|       599.94|              6|           99.99|                 3|         3|
+-----------+-------------+---------------+----------------+------------------+----------+
```

### Monthly Trends
```
+----------+-------+----------------------+--------------+---------------+----------+----------------+
|year_month|revenue|previous_month_revenue|revenue_change|mom_growth_pct|order_count|unique_customers|
+----------+-------+----------------------+--------------+---------------+----------+----------------+
|   2023-01|2149.88|                  null|          null|           null|         5|               5|
|   2023-02| 949.93|                2149.88|       -1199.95|         -55.81|         5|               5|
|   2023-03| 599.98|                 949.93|        -349.95|         -36.84|         3|               3|
+----------+-------+----------------------+--------------+---------------+----------+----------------+
```

## Performance Optimizations

- **Kryo Serialization**: Faster data serialization
- **Adaptive Query Execution**: Automatic query optimization
- **Memory Management**: 4GB driver memory allocation
- **Broadcast Joins**: Optimized for small lookup tables
- **Arrow Communication**: Faster Python-Java data transfer

## Code Quality

- ✅ **Type Hints**: All methods include comprehensive type annotations
- ✅ **Docstrings**: Detailed documentation for all methods
- ✅ **Error Handling**: Input validation and exception handling
- ✅ **Logging**: Integrated logging for debugging and monitoring
- ✅ **Resource Management**: Proper Spark session cleanup
- ✅ **Context Manager**: Support for `with` statement usage

## Validation

Run the validation script to verify the implementation:

```bash
python validate_analytics.py
```

This script validates:
- Class structure and required methods
- Import statements and dependencies
- Spark configuration settings
- Window functions and aggregations
- Type hints and documentation

## Running the Example

```bash
python spark_analytics.py
```

This will demonstrate all analytics methods using sample data and show the complete functionality of the SalesAnalytics class.
