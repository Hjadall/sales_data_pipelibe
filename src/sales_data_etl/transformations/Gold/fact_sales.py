import dlt
from pyspark.sql.functions import col, coalesce, lit

@dlt.table(
    name="fact_sales",
    comment="Final Gold Fact table joining Silver Orders with Current-State Gold Dimensions"
)
def fact_sales():
    # 1. Read the Orders as a stream (only process new data)
    orders_df = dlt.read_stream("silver_orders")
    
    # 2. Read the Gold Dimensions (Filtered for current active records)
    # This ensures we get the most up-to-date name, city, and price
    customers_df = dlt.read("gold_customers").filter(col("__end_at").isNull())
    products_df = dlt.read("gold_products").filter(col("__end_at").isNull())
    
    # 3. Join them all together
    return (
        orders_df.alias("o")
        .join(
            customers_df.alias("c"), 
            on="customer_id", 
            how="left" # Left join to ensure we never lose an order
        )
        .join(
            products_df.alias("p"), 
            on="product_id", 
            how="left"
        )
        .select(
            col("o.order_id"),
            col("o.order_date"),
            col("o.total_amount"),
            col("o.quantity"),
            # Use coalesce to provide a default if a dimension record isn't found yet
            coalesce(col("c.full_name"), lit("Unknown")).alias("customer_name"),
            coalesce(col("c.city"), lit("Unknown")).alias("customer_city"),
            coalesce(col("p.product_name"), lit("Unknown")).alias("product_name"),
            col("p.brand"),
            col("p.category")
        )
    )