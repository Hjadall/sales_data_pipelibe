import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Define temporary view with transformations
@dlt.view(name="orders_transformed_view")
def orders_transformed():
    # We must read from the bronze table created in your previous script
    df = dlt.read_stream("bronze_orders") 
    
    # Transformations
    df = df.withColumn("order_date", to_timestamp(col('order_date'))) \
           .withColumn("year", year(col('order_date')))
    
    # Window functions work perfectly here
    # window_spec = Window.partitionBy("year").orderBy(desc("total_amount"))
    # df = df.withColumn("flag", dense_rank().over(window_spec))
    
    return df

# Step 1: Create target table first
dlt.create_streaming_table(name="silver_orders")

# Step 2: Define CDC flow (called directly, not as decorator)
dlt.create_auto_cdc_flow (
    target="silver_orders",
    source="orders_transformed_view",
    keys=["order_id"],
    sequence_by="order_date",
    stored_as_scd_type=1
)