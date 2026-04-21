import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Define temporary view with transformations
@dlt.view(name="regions_transformed_view")
def regions_transformed():
    
    df = dlt.read_stream("bronze_regions").withColumn("last_updated", current_timestamp())
    
    return df

# Step 1: Create target table first
dlt.create_streaming_table(name="silver_regions")

# Step 2: Define CDC flow (called directly, not as decorator)
dlt.create_auto_cdc_flow (
    target="silver_regions",
    source="regions_transformed_view",
    keys=["region_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1
)
