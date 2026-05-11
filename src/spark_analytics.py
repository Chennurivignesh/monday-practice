"""
PySpark analytics module for e-commerce data analysis.

This module performs various analyses on the generated e-commerce data
including customer segmentation, product performance, and revenue analysis.
"""

import logging
from typing import Dict, Any, List

import pandas as pd
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import (
    col, count, sum, avg, max, min, year, month, 
    when, rank, desc, asc, round as spark_round
)
from pyspark.sql.window import Window

from config import (
    SPARK_CONFIG,
    CUSTOMERS_FILE,
    PRODUCTS_FILE,
    ORDERS_FILE,
    ORDER_ITEMS_FILE,
    CUSTOMER_ANALYSIS_FILE,
    PRODUCT_ANALYSIS_FILE,
    REVENUE_ANALYSIS_FILE,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ECommerceAnalytics:
    """
    E-commerce analytics class using PySpark for data processing.
    
    This class provides methods to analyze customer behavior, product performance,
    and revenue trends from e-commerce data.
    """
    
    def __init__(self) -> None:
        """Initialize the Spark session and load data."""
        self.spark = self._create_spark_session()
        self.customers_df = None
        self.products_df = None
        self.orders_df = None
        self.order_items_df = None
        self._load_data()
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session.
        
        Returns:
            Configured SparkSession instance
            
        Raises:
            Exception: If Spark session creation fails
        """
        try:
            logger.info("Creating Spark session...")
            spark = SparkSession.builder \
                .appName(SPARK_CONFIG["app_name"]) \
                .master(SPARK_CONFIG["master"]) \
                .config("spark.sql.adaptive.enabled", SPARK_CONFIG["spark.sql.adaptive.enabled"]) \
                .config("spark.sql.adaptive.coalescePartitions.enabled", SPARK_CONFIG["spark.sql.adaptive.coalescePartitions.enabled"]) \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", SPARK_CONFIG["spark.sql.adaptive.advisoryPartitionSizeInBytes"]) \
                .getOrCreate()
            
            logger.info("Spark session created successfully")
            return spark
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {str(e)}")
            raise
    
    def _load_data(self) -> None:
        """
        Load all CSV data into Spark DataFrames.
        
        Raises:
            Exception: If any file loading fails
        """
        try:
            logger.info("Loading data files...")
            
            self.customers_df = self.spark.read.csv(str(CUSTOMERS_FILE), header=True, inferSchema=True)
            self.products_df = self.spark.read.csv(str(PRODUCTS_FILE), header=True, inferSchema=True)
            self.orders_df = self.spark.read.csv(str(ORDERS_FILE), header=True, inferSchema=True)
            self.order_items_df = self.spark.read.csv(str(ORDER_ITEMS_FILE), header=True, inferSchema=True)
            
            logger.info("All data files loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise
    
    def analyze_customer_segments(self) -> SparkDataFrame:
        """
        Analyze customer segments based on purchase behavior.
        
        Returns:
            DataFrame with customer segmentation analysis
            
        Raises:
            Exception: If analysis fails
        """
        try:
            logger.info("Performing customer segmentation analysis...")
            
            # Join orders with order items to get total spending per customer
            customer_spending = self.orders_df.join(
                self.order_items_df, "order_id"
            ).groupBy("customer_id").agg(
                count("order_id").alias("total_orders"),
                sum(col("quantity") * col("unit_price")).alias("total_spent"),
                avg(col("quantity") * col("unit_price")).alias("avg_order_value"),
                max("order_date").alias("last_order_date"),
                min("order_date").alias("first_order_date")
            )
            
            # Join with customer demographics
            customer_analysis = customer_spending.join(
                self.customers_df, "customer_id"
            ).select(
                "customer_id", "first_name", "last_name", "email", "city", "state",
                "age", "gender", "total_orders", "total_spent", "avg_order_value",
                "first_order_date", "last_order_date"
            )
            
            # Create customer segments based on spending
            customer_analysis = customer_analysis.withColumn(
                "customer_segment",
                when(col("total_spent") > 5000, "VIP")
                .when(col("total_spent") > 2000, "Premium")
                .when(col("total_spent") > 500, "Regular")
                .otherwise("New")
            )
            
            logger.info("Customer segmentation analysis completed")
            return customer_analysis
            
        except Exception as e:
            logger.error(f"Customer segmentation analysis failed: {str(e)}")
            raise
    
    def analyze_product_performance(self) -> SparkDataFrame:
        """
        Analyze product performance metrics.
        
        Returns:
            DataFrame with product performance analysis
            
        Raises:
            Exception: If analysis fails
        """
        try:
            logger.info("Performing product performance analysis...")
            
            # Calculate product metrics
            product_metrics = self.order_items_df.groupBy("product_id").agg(
                count("order_id").alias("total_orders"),
                sum("quantity").alias("total_quantity_sold"),
                sum(col("quantity") * col("unit_price")).alias("total_revenue"),
                avg("unit_price").alias("avg_selling_price"),
                max("unit_price").alias("max_price"),
                min("unit_price").alias("min_price")
            )
            
            # Join with product information
            product_analysis = product_metrics.join(
                self.products_df, "product_id"
            ).select(
                "product_id", "product_name", "category", "price", "description",
                "total_orders", "total_quantity_sold", "total_revenue",
                "avg_selling_price", "max_price", "min_price", "in_stock", "stock_quantity"
            )
            
            # Calculate profit margin (assuming 70% of price is cost)
            product_analysis = product_analysis.withColumn(
                "estimated_profit",
                col("total_revenue") * 0.3
            ).withColumn(
                "profit_margin_percent",
                spark_round((col("estimated_profit") / col("total_revenue")) * 100, 2)
            )
            
            # Rank products within each category
            window_spec = Window.partitionBy("category").orderBy(desc("total_revenue"))
            product_analysis = product_analysis.withColumn(
                "category_rank",
                rank().over(window_spec)
            )
            
            logger.info("Product performance analysis completed")
            return product_analysis
            
        except Exception as e:
            logger.error(f"Product performance analysis failed: {str(e)}")
            raise
    
    def analyze_revenue_trends(self) -> SparkDataFrame:
        """
        Analyze revenue trends over time.
        
        Returns:
            DataFrame with revenue trend analysis
            
        Raises:
            Exception: If analysis fails
        """
        try:
            logger.info("Performing revenue trend analysis...")
            
            # Join orders with order items
            orders_with_items = self.orders_df.join(self.order_items_df, "order_id")
            
            # Calculate revenue by date
            daily_revenue = orders_with_items.withColumn(
                "order_date", col("order_date").cast("date")
            ).groupBy("order_date").agg(
                count("order_id").alias("total_orders"),
                sum(col("quantity") * col("unit_price")).alias("daily_revenue"),
                avg(col("quantity") * col("unit_price")).alias("avg_order_value")
            ).orderBy("order_date")
            
            # Calculate monthly revenue
            monthly_revenue = orders_with_items.withColumn(
                "order_month", month("order_date")
            ).withColumn(
                "order_year", year("order_date")
            ).groupBy("order_year", "order_month").agg(
                count("order_id").alias("total_orders"),
                sum(col("quantity") * col("unit_price")).alias("monthly_revenue"),
                avg(col("quantity") * col("unit_price")).alias("avg_order_value")
            ).orderBy("order_year", "order_month")
            
            # Revenue by state
            revenue_by_state = orders_with_items.join(
                self.customers_df, "customer_id"
            ).groupBy("state").agg(
                count("order_id").alias("total_orders"),
                sum(col("quantity") * col("unit_price")).alias("total_revenue"),
                avg(col("quantity") * col("unit_price")).alias("avg_order_value")
            ).orderBy(desc("total_revenue"))
            
            # Revenue by category
            revenue_by_category = orders_with_items.join(
                self.products_df, "product_id"
            ).groupBy("category").agg(
                count("order_id").alias("total_orders"),
                sum(col("quantity") * col("unit_price")).alias("total_revenue"),
                avg(col("quantity") * col("unit_price")).alias("avg_order_value")
            ).orderBy(desc("total_revenue"))
            
            logger.info("Revenue trend analysis completed")
            
            # Return combined results
            return daily_revenue
            
        except Exception as e:
            logger.error(f"Revenue trend analysis failed: {str(e)}")
            raise
    
    def get_top_customers(self, limit: int = 10) -> SparkDataFrame:
        """
        Get top customers by total spending.
        
        Args:
            limit: Number of top customers to return
            
        Returns:
            DataFrame with top customers
            
        Raises:
            Exception: If analysis fails
        """
        try:
            logger.info(f"Getting top {limit} customers...")
            
            top_customers = self.orders_df.join(
                self.order_items_df, "order_id"
            ).join(self.customers_df, "customer_id")
            
            customer_spending = top_customers.groupBy(
                "customer_id", "first_name", "last_name", "email"
            ).agg(
                count("order_id").alias("total_orders"),
                sum(col("quantity") * col("unit_price")).alias("total_spent"),
                avg(col("quantity") * col("unit_price")).alias("avg_order_value")
            ).orderBy(desc("total_spent")).limit(limit)
            
            logger.info(f"Retrieved top {limit} customers")
            return customer_spending
            
        except Exception as e:
            logger.error(f"Failed to get top customers: {str(e)}")
            raise
    
    def save_analysis_results(self) -> None:
        """
        Save all analysis results to CSV files.
        
        Raises:
            Exception: If saving fails
        """
        try:
            logger.info("Saving analysis results...")
            
            # Customer analysis
            customer_analysis = self.analyze_customer_segments()
            customer_analysis.toPandas().to_csv(CUSTOMER_ANALYSIS_FILE, index=False)
            logger.info(f"Customer analysis saved to {CUSTOMER_ANALYSIS_FILE}")
            
            # Product analysis
            product_analysis = self.analyze_product_performance()
            product_analysis.toPandas().to_csv(PRODUCT_ANALYSIS_FILE, index=False)
            logger.info(f"Product analysis saved to {PRODUCT_ANALYSIS_FILE}")
            
            # Revenue analysis
            revenue_analysis = self.analyze_revenue_trends()
            revenue_analysis.toPandas().to_csv(REVENUE_ANALYSIS_FILE, index=False)
            logger.info(f"Revenue analysis saved to {REVENUE_ANALYSIS_FILE}")
            
            logger.info("All analysis results saved successfully!")
            
        except Exception as e:
            logger.error(f"Failed to save analysis results: {str(e)}")
            raise
    
    def close_spark_session(self) -> None:
        """Close the Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session closed")


def main() -> None:
    """
    Main function to run the analytics process.
    """
    analytics = None
    try:
        analytics = ECommerceAnalytics()
        
        # Run all analyses
        logger.info("Starting e-commerce analytics...")
        
        customer_segments = analytics.analyze_customer_segments()
        product_performance = analytics.analyze_product_performance()
        revenue_trends = analytics.analyze_revenue_trends()
        top_customers = analytics.get_top_customers()
        
        # Save results
        analytics.save_analysis_results()
        
        # Print some insights
        logger.info("=== ANALYSIS INSIGHTS ===")
        
        # Customer segments summary
        customer_segments.groupBy("customer_segment").count().show()
        
        # Top 5 products by revenue
        product_performance.select("product_name", "category", "total_revenue").orderBy(desc("total_revenue")).limit(5).show()
        
        # Revenue by state (top 5)
        revenue_by_state = analytics.orders_df.join(analytics.order_items_df, "order_id") \
            .join(analytics.customers_df, "customer_id") \
            .groupBy("state") \
            .agg(sum(col("quantity") * col("unit_price")).alias("total_revenue")) \
            .orderBy(desc("total_revenue")).limit(5)
        revenue_by_state.show()
        
        logger.info("Analytics completed successfully!")
        
    except Exception as e:
        logger.error(f"Analytics process failed: {str(e)}")
        raise
    finally:
        if analytics:
            analytics.close_spark_session()


if __name__ == "__main__":
    main()
