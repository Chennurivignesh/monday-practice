#!/usr/bin/env python3
"""
PySpark Sales Analytics Class
A comprehensive analytics class for sales data processing using Apache Spark.
"""

from typing import Optional, List, Dict, Any
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, sum, count, desc, asc, rank, dense_rank,
    month, year, lag, when, isnull, round as spark_round,
    to_date, date_format, avg, min, max
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType
import logging


class SalesAnalytics:
    """
    A comprehensive PySpark analytics class for sales data processing.
    
    This class provides methods for analyzing sales data including customer rankings,
    category performance, and monthly trend analysis with proper Spark configuration
    and optimization settings.
    
    Attributes:
        spark (SparkSession): The configured Spark session
        logger (logging.Logger): Logger for debugging and monitoring
    """
    
    def __init__(self, app_name: str = "SalesAnalytics"):
        """
        Initialize the SalesAnalytics class.
        
        Args:
            app_name (str): Name for the Spark application
        """
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        self.logger = logging.getLogger(__name__)
        
    def create_spark_session(self) -> SparkSession:
        """
        Create and configure a Spark session with optimal settings for local analytics.
        
        Configures Spark with:
        - 4GB driver memory
        - Adaptive query execution enabled
        - Kryo serialization for better performance
        - Local mode with 2 cores
        
        Returns:
            SparkSession: Configured Spark session
            
        Raises:
            Exception: If Spark session creation fails
        """
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master("local[2]") \
                .config("spark.driver.memory", "4g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.kryoserializer.buffer.max", "512m") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
                .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000") \
                .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
                .getOrCreate()
            
            # Set log level to WARN to reduce verbosity
            self.spark.sparkContext.setLogLevel("WARN")
            
            self.logger.info("Spark session created successfully with optimized configuration")
            return self.spark
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise
    
    def top_customers(self, orders_df: DataFrame, products_df: DataFrame, n: int = 10) -> DataFrame:
        """
        Calculate total spend per customer and return top N customers.
        
        This method joins orders and products data, calculates total spending per customer,
        and uses Window functions to rank customers by their total spending.
        
        Args:
            orders_df (DataFrame): Orders dataframe with columns: order_id, customer_id, product_id, quantity, order_date
            products_df (DataFrame): Products dataframe with columns: product_id, product_name, category, price
            n (int): Number of top customers to return (default: 10)
            
        Returns:
            DataFrame: Top N customers with columns: customer_id, total_spend, rank
            
        Raises:
            ValueError: If required columns are missing from dataframes
        """
        if self.spark is None:
            raise RuntimeError("Spark session not initialized. Call create_spark_session() first.")
        
        try:
            # Validate required columns
            self._validate_columns(orders_df, ['customer_id', 'product_id', 'quantity'])
            self._validate_columns(products_df, ['product_id', 'price'])
            
            # Join orders with products to get price information
            orders_with_price = orders_df.join(
                products_df.select('product_id', 'price'),
                on='product_id',
                how='inner'
            )
            
            # Calculate total spend per customer
            customer_spend = orders_with_price.withColumn(
                'total_order_value',
                col('quantity') * col('price')
            ).groupBy('customer_id') \
             .agg(
                 sum('total_order_value').alias('total_spend'),
                 count('order_id').alias('order_count')
             )
            
            # Create Window specification for ranking
            window_spec = Window.orderBy(desc('total_spend'))
            
            # Apply ranking to get top customers
            top_customers_df = customer_spend.withColumn(
                'rank',
                rank().over(window_spec)
            ).filter(col('rank') <= n) \
             .select(
                 'customer_id',
                 'total_spend',
                 'order_count',
                 'rank'
             ) \
             .orderBy(asc('rank'))
            
            self.logger.info(f"Successfully calculated top {n} customers")
            return top_customers_df
            
        except Exception as e:
            self.logger.error(f"Error in top_customers: {str(e)}")
            raise
    
    def sales_by_category(self, orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """
        Group by product category and calculate total revenue and units sold.
        
        This method joins orders and products data, aggregates metrics by product category,
        and provides comprehensive sales performance metrics per category.
        
        Args:
            orders_df (DataFrame): Orders dataframe with columns: order_id, customer_id, product_id, quantity, order_date
            products_df (DataFrame): Products dataframe with columns: product_id, product_name, category, price
            
        Returns:
            DataFrame: Sales by category with columns: category, total_revenue, total_units_sold, 
                      avg_order_value, distinct_products, order_count
            
        Raises:
            ValueError: If required columns are missing from dataframes
        """
        if self.spark is None:
            raise RuntimeError("Spark session not initialized. Call create_spark_session() first.")
        
        try:
            # Validate required columns
            self._validate_columns(orders_df, ['product_id', 'quantity'])
            self._validate_columns(products_df, ['product_id', 'category', 'price'])
            
            # Join orders with products
            orders_with_products = orders_df.join(
                products_df,
                on='product_id',
                how='inner'
            )
            
            # Calculate metrics by category
            category_sales = orders_with_products.groupBy('category') \
                .agg(
                    sum(col('quantity') * col('price')).alias('total_revenue'),
                    sum('quantity').alias('total_units_sold'),
                    avg(col('quantity') * col('price')).alias('avg_order_value'),
                    count('order_id').alias('order_count'),
                    countDistinct('product_id').alias('distinct_products')
                ) \
                .orderBy(desc('total_revenue'))
            
            # Round numerical columns for better readability
            category_sales = category_sales.withColumn(
                'total_revenue',
                spark_round('total_revenue', 2)
            ).withColumn(
                'avg_order_value',
                spark_round('avg_order_value', 2)
            )
            
            self.logger.info("Successfully calculated sales by category")
            return category_sales
            
        except Exception as e:
            self.logger.error(f"Error in sales_by_category: {str(e)}")
            raise
    
    def monthly_trends(self, orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """
        Calculate month-over-month revenue growth percentage.
        
        This method joins orders and products data, aggregates revenue by month,
        and calculates month-over-month growth using Window functions and lag operations.
        
        Args:
            orders_df (DataFrame): Orders dataframe with columns: order_id, customer_id, product_id, quantity, order_date
            products_df (DataFrame): Products dataframe with columns: product_id, product_name, category, price
            
        Returns:
            DataFrame: Monthly trends with columns: year_month, revenue, previous_month_revenue, 
                      mom_growth_pct, revenue_change
            
        Raises:
            ValueError: If required columns are missing from dataframes
        """
        if self.spark is None:
            raise RuntimeError("Spark session not initialized. Call create_spark_session() first.")
        
        try:
            # Validate required columns
            self._validate_columns(orders_df, ['product_id', 'quantity', 'order_date'])
            self._validate_columns(products_df, ['product_id', 'price'])
            
            # Join orders with products and ensure order_date is in date format
            orders_with_products = orders_df.join(
                products_df.select('product_id', 'price'),
                on='product_id',
                how='inner'
            ).withColumn(
                'order_date',
                to_date(col('order_date'))
            )
            
            # Calculate revenue per order
            orders_with_revenue = orders_with_products.withColumn(
                'revenue',
                col('quantity') * col('price')
            )
            
            # Aggregate revenue by month
            monthly_revenue = orders_with_revenue \
                .withColumn('year_month', date_format(col('order_date'), 'yyyy-MM')) \
                .groupBy('year_month') \
                .agg(
                    sum('revenue').alias('revenue'),
                    count('order_id').alias('order_count'),
                    countDistinct('customer_id').alias('unique_customers')
                ) \
                .orderBy('year_month')
            
            # Create Window specification for month-over-month calculation
            window_spec = Window.orderBy('year_month')
            
            # Calculate month-over-month growth
            monthly_trends_df = monthly_revenue.withColumn(
                'previous_month_revenue',
                lag('revenue', 1).over(window_spec)
            ).withColumn(
                'mom_growth_pct',
                when(
                    isnull(col('previous_month_revenue')) | (col('previous_month_revenue') == 0),
                    None
                ).otherwise(
                    spark_round(
                        ((col('revenue') - col('previous_month_revenue')) / col('previous_month_revenue')) * 100,
                        2
                    )
                )
            ).withColumn(
                'revenue_change',
                when(
                    isnull(col('previous_month_revenue')),
                    None
                ).otherwise(
                    spark_round(col('revenue') - col('previous_month_revenue'), 2)
                )
            ).select(
                'year_month',
                'revenue',
                'previous_month_revenue',
                'revenue_change',
                'mom_growth_pct',
                'order_count',
                'unique_customers'
            )
            
            # Round revenue columns
            monthly_trends_df = monthly_trends_df.withColumn(
                'revenue',
                spark_round('revenue', 2)
            ).withColumn(
                'previous_month_revenue',
                spark_round('previous_month_revenue', 2)
            )
            
            self.logger.info("Successfully calculated monthly trends")
            return monthly_trends_df
            
        except Exception as e:
            self.logger.error(f"Error in monthly_trends: {str(e)}")
            raise
    
    def _validate_columns(self, df: DataFrame, required_columns: List[str]) -> None:
        """
        Validate that required columns exist in the dataframe.
        
        Args:
            df (DataFrame): DataFrame to validate
            required_columns (List[str]): List of required column names
            
        Raises:
            ValueError: If any required columns are missing
        """
        df_columns = df.columns
        missing_columns = [col for col in required_columns if col not in df_columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}. Available columns: {df_columns}")
    
    def create_sample_data(self) -> Dict[str, DataFrame]:
        """
        Create sample data for testing the analytics methods.
        
        Returns:
            Dict[str, DataFrame]: Dictionary containing 'orders' and 'products' DataFrames
        """
        if self.spark is None:
            raise RuntimeError("Spark session not initialized. Call create_spark_session() first.")
        
        # Sample products data
        products_data = [
            (1, "Laptop Pro", "Electronics", 999.99),
            (2, "Wireless Mouse", "Electronics", 29.99),
            (3, "Office Chair", "Furniture", 199.99),
            (4, "Standing Desk", "Furniture", 499.99),
            (5, "Coffee Maker", "Appliances", 89.99),
            (6, "Blender", "Appliances", 59.99),
            (7, "Headphones", "Electronics", 149.99),
            (8, "Desk Lamp", "Furniture", 39.99),
            (9, "Toaster", "Appliances", 49.99),
            (10, "Monitor", "Electronics", 299.99)
        ]
        
        products_schema = StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("price", DoubleType(), False)
        ])
        
        products_df = self.spark.createDataFrame(products_data, products_schema)
        
        # Sample orders data
        orders_data = [
            (1, 101, 1, 2, "2023-01-15"),
            (2, 102, 2, 3, "2023-01-16"),
            (3, 103, 3, 1, "2023-01-17"),
            (4, 101, 4, 1, "2023-01-18"),
            (5, 104, 5, 2, "2023-01-19"),
            (6, 105, 6, 1, "2023-01-20"),
            (7, 102, 7, 1, "2023-02-01"),
            (8, 103, 8, 2, "2023-02-02"),
            (9, 106, 9, 3, "2023-02-03"),
            (10, 101, 10, 1, "2023-02-04"),
            (11, 107, 1, 1, "2023-02-05"),
            (12, 108, 2, 2, "2023-02-06"),
            (13, 109, 3, 1, "2023-03-01"),
            (14, 101, 4, 1, "2023-03-02"),
            (15, 110, 5, 1, "2023-03-03")
        ]
        
        orders_schema = StructType([
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("order_date", StringType(), False)
        ])
        
        orders_df = self.spark.createDataFrame(orders_data, orders_schema)
        
        return {'orders': orders_df, 'products': products_df}
    
    def stop_spark_session(self) -> None:
        """
        Stop the Spark session and clean up resources.
        """
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
            self.logger.info("Spark session stopped")
    
    def __enter__(self):
        """Context manager entry point."""
        self.create_spark_session()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.stop_spark_session()


def main():
    """
    Main function to demonstrate the SalesAnalytics class usage.
    """
    # Initialize logging
    logging.basicConfig(level=logging.INFO)
    
    # Create analytics instance using context manager
    with SalesAnalytics() as analytics:
        try:
            # Create sample data
            print("Creating sample data...")
            data = analytics.create_sample_data()
            orders_df = data['orders']
            products_df = data['products']
            
            print("\nSample Orders Data:")
            orders_df.show()
            
            print("\nSample Products Data:")
            products_df.show()
            
            # Test top customers
            print("\n" + "="*50)
            print("TOP CUSTOMERS ANALYSIS")
            print("="*50)
            top_customers = analytics.top_customers(orders_df, products_df, n=5)
            top_customers.show()
            
            # Test sales by category
            print("\n" + "="*50)
            print("SALES BY CATEGORY ANALYSIS")
            print("="*50)
            category_sales = analytics.sales_by_category(orders_df, products_df)
            category_sales.show()
            
            # Test monthly trends
            print("\n" + "="*50)
            print("MONTHLY TRENDS ANALYSIS")
            print("="*50)
            monthly_trends = analytics.monthly_trends(orders_df, products_df)
            monthly_trends.show()
            
            # Print schema information
            print("\n" + "="*50)
            print("SCHEMA INFORMATION")
            print("="*50)
            print("Top Customers Schema:")
            top_customers.printSchema()
            
            print("\nCategory Sales Schema:")
            category_sales.printSchema()
            
            print("\nMonthly Trends Schema:")
            monthly_trends.printSchema()
            
        except Exception as e:
            print(f"Error during analytics execution: {str(e)}")
            raise


if __name__ == "__main__":
    main()
