import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Define temporary view with transformations
@dlt.view(name="products_transformed_view")
def products_transformed():
    
    df = dlt.read_stream("bronze_products").withColumn("last_updated", current_timestamp())
    
    # Transformations
    df = df.withColumn("discounted_price", col("price")* 0.9)
    df = df.withColumn("brand", upper(col("brand")))
    
    return df

# Step 1: Create target table first
dlt.create_streaming_table(name="silver_products")

# Step 2: Define CDC flow (called directly, not as decorator)
dlt.create_auto_cdc_flow (
    target="silver_products",
    source="products_transformed_view",
    keys=["product_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1
)
