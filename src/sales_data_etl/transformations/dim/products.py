import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# Step 1: Create target table first
dlt.create_streaming_table(name="gold_products")

# Step 2: Define CDC flow (called directly, not as decorator)
dlt.create_auto_cdc_flow (
    target="gold_products",
    source="products_transformed_view",
    keys=["product_id"],
    sequence_by="last_updated",
    stored_as_scd_type=2
)
