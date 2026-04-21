import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Define temporary view with transformations
@dlt.view(name="customers_transformed_view")
def customers_transformed():
    
    df = dlt.read_stream("bronze_customers").withColumn("last_updated", current_timestamp())
    
    # Transformations
    df = df.withColumn("full_name",concat(col('first_name'),lit(' '),col('last_name')))
    df = df.drop('first_name','last_name')
    
    return df

# Step 1: Create target table first
dlt.create_streaming_table(name="silver_customers")

# Step 2: Define CDC flow (called directly, not as decorator)
dlt.create_auto_cdc_flow (
    target="silver_customers",
    source="customers_transformed_view",
    keys=["customer_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1
)
