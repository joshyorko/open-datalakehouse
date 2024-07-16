import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def process_silver_lake_data():
    """
    This script processes data from the PostgreSQL database and writes the transformed data to the silver data lake
    using Nessie catalog and Iceberg tables.
    """
    # Initialize Spark session with Nessie and Iceberg configurations
    spark = SparkSession.builder \
        .appName("Silver Lake Processing") \
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3,"
                "software.amazon.awssdk:bundle:2.17.81,"
                "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres:5432/your_database"
    connection_properties = {
        "user": "your_username",
        "password": "your_password",
        "driver": "org.postgresql.Driver"
    }

    # Read data from PostgreSQL
    companies_df = spark.read.jdbc(url=jdbc_url, table="companies", properties=connection_properties)
    employees_df = spark.read.jdbc(url=jdbc_url, table="employees", properties=connection_properties)
    departments_df = spark.read.jdbc(url=jdbc_url, table="departments", properties=connection_properties)

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
