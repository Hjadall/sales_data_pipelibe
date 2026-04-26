"""Tests for Spark transformations used in the medallion architecture.

Tests cover pure Spark SQL/DataFrame operations:
- Bronze layer: Raw data ingestion and schema validation
- Silver layer: Data cleaning and standardization
- Gold layer: Fact and dimension table joins
- Data quality: Validation checks
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, concat, lit


class TestBronzeLayerTransformations:
    """Tests for Bronze layer - raw data ingestion."""

    def test_bronze_customers_schema_validation(self, spark: SparkSession):
        """Test that bronze_customers table has correct schema."""
        # Bronze layer: Load raw customer data
        raw_customers = [
            {"customer_id": 1, "first_name": "John", "last_name": "Doe", "email": "john@example.com", "city": "NYC"},
            {"customer_id": 2, "first_name": "Jane", "last_name": "Smith", "email": "jane@example.com", "city": "LA"},
        ]
        bronze_customers = spark.createDataFrame(raw_customers)

        # Assertions
        assert "customer_id" in bronze_customers.columns
        assert "first_name" in bronze_customers.columns
        assert "last_name" in bronze_customers.columns
        assert bronze_customers.count() == 2

    def test_bronze_orders_schema_validation(self, spark: SparkSession):
        """Test that bronze_orders table has correct schema."""
        raw_orders = [
            {
                "order_id": 1,
                "customer_id": 1,
                "product_id": 100,
                "order_date": "2024-01-15",
                "quantity": 2,
                "total_amount": 49.99,
            },
            {
                "order_id": 2,
                "customer_id": 2,
                "product_id": 101,
                "order_date": "2024-01-16",
                "quantity": 1,
                "total_amount": 29.99,
            },
        ]
        bronze_orders = spark.createDataFrame(raw_orders)

        assert "order_id" in bronze_orders.columns
        assert "customer_id" in bronze_orders.columns
        assert "total_amount" in bronze_orders.columns
        assert bronze_orders.count() == 2


class TestSilverLayerTransformations:
    """Tests for Silver layer - data cleaning and standardization."""

    def test_customer_name_concatenation(self, spark: SparkSession):
        """Test concatenating first_name + last_name into full_name."""
        # Bronze data
        raw_data = [
            {"customer_id": 1, "first_name": "John", "last_name": "Doe"},
            {"customer_id": 2, "first_name": "Jane", "last_name": "Smith"},
        ]
        bronze_df = spark.createDataFrame(raw_data)

        # Silver transformation: concatenate names
        silver_df = bronze_df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))).drop(
            "first_name", "last_name"
        )

        # Assertions
        assert "full_name" in silver_df.columns
        assert "first_name" not in silver_df.columns
        assert "last_name" not in silver_df.columns

        # Verify data
        results = silver_df.collect()
        assert results[0]["full_name"] == "John Doe"
        assert results[1]["full_name"] == "Jane Smith"

    def test_order_date_to_timestamp_conversion(self, spark: SparkSession):
        """Test converting order_date string to timestamp type."""
        raw_data = [
            {"order_id": 1, "order_date": "2024-01-15", "total_amount": 100.00},
            {"order_id": 2, "order_date": "2024-01-16", "total_amount": 200.00},
        ]
        bronze_df = spark.createDataFrame(raw_data)

        # Silver transformation: convert date string to timestamp
        silver_df = bronze_df.withColumn("order_date", col("order_date").cast("timestamp"))

        # Assertions
        assert silver_df.schema["order_date"].dataType.simpleString() == "timestamp"
        assert silver_df.count() == 2

    def test_add_discount_column_to_products(self, spark: SparkSession):
        """Test adding discount percentage to products."""
        raw_data = [
            {"product_id": 100, "product_name": "Widget", "price": 100.00},
            {"product_id": 101, "product_name": "Gadget", "price": 50.00},
        ]
        bronze_df = spark.createDataFrame(raw_data)

        # Silver transformation: add 10% discount
        silver_df = bronze_df.withColumn("discount_pct", lit(10.0))

        # Assertions
        assert "discount_pct" in silver_df.columns
        results = silver_df.collect()
        assert all(row["discount_pct"] == 10.0 for row in results)

    def test_add_processing_timestamp(self, spark: SparkSession):
        """Test adding last_updated timestamp to track when data was processed."""
        raw_data = [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"},
        ]
        bronze_df = spark.createDataFrame(raw_data)

        # Silver transformation: add timestamp
        silver_df = bronze_df.withColumn("last_updated", current_timestamp())

        # Assertions
        assert "last_updated" in silver_df.columns
        assert silver_df.filter(col("last_updated").isNull()).count() == 0


class TestGoldLayerTransformations:
    """Tests for Gold layer - fact and dimension tables."""

    def test_add_scd2_columns_to_dimension(self, spark: SparkSession):
        """Test adding SCD Type 2 tracking columns (__start_at, __end_at) to dimension tables."""
        raw_data = [
            {"product_id": 100, "product_name": "Widget", "price": 100.00},
            {"product_id": 101, "product_name": "Gadget", "price": 50.00},
        ]
        df = spark.createDataFrame(raw_data)

        # Add SCD Type 2 columns
        gold_df = df.withColumn("__start_at", current_timestamp()).withColumn("__end_at", lit(None).cast("timestamp"))

        # Assertions
        assert "__start_at" in gold_df.columns
        assert "__end_at" in gold_df.columns
        assert gold_df.filter(col("__end_at").isNull()).count() == 2  # All records are active

    def test_filter_active_dimension_records(self, spark: SparkSession):
        """Test filtering only active records from dimension table (where __end_at is NULL)."""
        # Simulate dimension table with both active and inactive records
        data = [
            {"product_id": 100, "product_name": "Widget", "__end_at": None},  # Active
            {"product_id": 101, "product_name": "Gadget", "__end_at": "2024-01-01"},  # Inactive
            {"product_id": 102, "product_name": "Tool", "__end_at": None},  # Active
        ]
        df = spark.createDataFrame(data)

        # Filter active records only
        active_df = df.filter(col("__end_at").isNull())

        # Assertions
        assert active_df.count() == 2
        active_ids = [row["product_id"] for row in active_df.collect()]
        assert 100 in active_ids
        assert 102 in active_ids
        assert 101 not in active_ids

    def test_fact_table_left_join_orders_with_customers(self, spark: SparkSession):
        """Test left join: orders with customer dimension (keep all orders)."""
        # Orders data
        orders = [
            {"order_id": 1, "customer_id": 1, "amount": 100.00},
            {"order_id": 2, "customer_id": 2, "amount": 200.00},
            {"order_id": 3, "customer_id": 999, "amount": 50.00},  # Customer doesn't exist in dimension
        ]
        orders_df = spark.createDataFrame(orders)

        # Customers dimension (only 2 customers)
        customers = [
            {"customer_id": 1, "full_name": "John Doe"},
            {"customer_id": 2, "full_name": "Jane Smith"},
        ]
        customers_df = spark.createDataFrame(customers)

        # Left join: keep all orders even if customer doesn't exist
        fact_df = orders_df.join(customers_df, on="customer_id", how="left")

        # Assertions
        assert fact_df.count() == 3  # All 3 orders present
        assert fact_df.filter(col("full_name").isNull()).count() == 1  # 1 order with NULL customer name

    def test_fact_table_multi_join_orders_with_customers_and_products(self, spark: SparkSession):
        """Test chaining joins: orders -> customers -> products."""
        # Orders
        orders = [
            {"order_id": 1, "customer_id": 1, "product_id": 100, "quantity": 2},
        ]
        orders_df = spark.createDataFrame(orders)

        # Customers
        customers = [
            {"customer_id": 1, "full_name": "John Doe", "city": "NYC"},
        ]
        customers_df = spark.createDataFrame(customers)

        # Products
        products = [
            {"product_id": 100, "product_name": "Widget", "price": 50.00},
        ]
        products_df = spark.createDataFrame(products)

        # Chain joins
        fact_df = (
            orders_df.alias("o")
            .join(customers_df.alias("c"), on="customer_id", how="left")
            .join(products_df.alias("p"), on="product_id", how="left")
            .select(
                col("o.order_id"),
                col("o.quantity"),
                col("c.full_name").alias("customer_name"),
                col("c.city"),
                col("p.product_name"),
                col("p.price"),
            )
        )

        # Assertions
        assert fact_df.count() == 1
        row = fact_df.first()
        assert row["customer_name"] == "John Doe"
        assert row["product_name"] == "Widget"
        assert row["city"] == "NYC"


class TestDataQualityTransformations:
    """Tests for data quality checks and validations."""

    def test_detect_null_values_in_key_column(self, spark: SparkSession):
        """Test detecting NULL values in primary key column."""
        data = [
            {"customer_id": 1, "name": "John"},
            {"customer_id": None, "name": "Jane"},  # NULL customer_id
            {"customer_id": 3, "name": "Bob"},
        ]
        df = spark.createDataFrame(data)

        # Find NULL values
        nulls = df.filter(col("customer_id").isNull())

        # Assertions
        assert nulls.count() == 1

    def test_remove_duplicates_by_key(self, spark: SparkSession):
        """Test removing duplicate records by key column."""
        data = [
            {"customer_id": 1, "name": "John", "email": "john@example.com"},
            {"customer_id": 1, "name": "John", "email": "john@example.com"},  # Duplicate
            {"customer_id": 2, "name": "Jane", "email": "jane@example.com"},
        ]
        df = spark.createDataFrame(data)

        # Remove duplicates
        unique_df = df.dropDuplicates(["customer_id"])

        # Assertions
        assert unique_df.count() == 2

    def test_validate_email_format(self, spark: SparkSession):
        """Test detecting invalid email formats (must contain @)."""
        data = [
            {"id": 1, "email": "john@example.com"},
            {"id": 2, "email": "invalid-email"},  # No @
            {"id": 3, "email": "jane@example.com"},
        ]
        df = spark.createDataFrame(data)

        # Find invalid emails
        invalid_emails = df.filter(~col("email").contains("@"))

        # Assertions
        assert invalid_emails.count() == 1

    def test_data_type_conversion_validation(self, spark: SparkSession):
        """Test converting and validating data types."""
        data = [
            {"id": "1", "amount": "100.50"},
            {"id": "2", "amount": "200.75"},
        ]
        df = spark.createDataFrame(data)

        # Convert types
        converted_df = df.withColumn("id", col("id").cast("integer")).withColumn("amount", col("amount").cast("double"))

        # Assertions
        assert converted_df.schema["id"].dataType.simpleString() == "int"
        assert converted_df.schema["amount"].dataType.simpleString() == "double"

    def test_count_records_by_category(self, spark: SparkSession):
        """Test aggregating and counting records by category."""
        data = [
            {"order_id": 1, "customer_id": 1, "status": "completed"},
            {"order_id": 2, "customer_id": 2, "status": "pending"},
            {"order_id": 3, "customer_id": 1, "status": "completed"},
            {"order_id": 4, "customer_id": 3, "status": "cancelled"},
        ]
        df = spark.createDataFrame(data)

        # Group and count by status
        status_counts = df.groupBy("status").count()

        # Assertions
        assert status_counts.count() == 3
        completed = status_counts.filter(col("status") == "completed").first()
        assert completed["count"] == 2

    def test_find_duplicate_keys_across_orders(self, spark: SparkSession):
        """Test finding duplicate customer_ids in orders (customers ordering multiple times)."""
        data = [
            {"order_id": 1, "customer_id": 1},
            {"order_id": 2, "customer_id": 1},  # Customer 1 again
            {"order_id": 3, "customer_id": 2},
            {"order_id": 4, "customer_id": 1},  # Customer 1 again
        ]
        df = spark.createDataFrame(data)

        # Count orders per customer
        customer_order_counts = df.groupBy("customer_id").count()

        # Assertions
        assert customer_order_counts.count() == 2
        customer_1_orders = customer_order_counts.filter(col("customer_id") == 1).first()
        assert customer_1_orders["count"] == 3
