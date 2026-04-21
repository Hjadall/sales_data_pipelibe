import dlt
from pyspark.sql.functions import *

source_tables = ["orders", "customers", "products", "regions"]

# We create a wrapper function to "capture" the table name correctly
def create_bronze_table(table_name):

    @dlt.table(
        name=f"bronze_{table_name}", # Added 'bronze_' prefix for clarity
        comment=f"Raw Auto Loader ingestion for {table_name}"
    )
    def ingestion_logic():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            # Using the captured table_name variable
            .option("cloudFiles.schemaLocation", f"/Volumes/sales_data/source/{table_name}/_schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"/Volumes/sales_data/source/{table_name}")
        )

# Now we call the wrapper inside the loop
for table in source_tables:
    create_bronze_table(table)

