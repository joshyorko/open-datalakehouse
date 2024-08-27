import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from utils import configure_spark

def process_silver_lake_data():

    # PostgreSQL connection properties
    spark = configure_spark("minio","main")
    
    

    # Process Data (transformations, cleaning, etc.)
    # Remove duplicates from companies Name column
    companies_df = companies_df.dropDuplicates(["Name"])

    # Add processing timestamp
    companies_df = companies_df.withColumn("processed_at", current_timestamp())
    employees_df = employees_df.withColumn("processed_at", current_timestamp())
    departments_df = departments_df.withColumn("processed_at", current_timestamp())

    # Write Data to Iceberg tables using Nessie catalog
    companies_df.writeTo("nessie.silver.companies").using("iceberg").createOrReplace()
    employees_df.writeTo("nessie.silver.employees").using("iceberg").createOrReplace()
    departments_df.writeTo("nessie.silver.departments").using("iceberg").createOrReplace()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    process_silver_lake_data()
