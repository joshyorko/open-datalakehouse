import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def process_gold_lake_data():
    """
    This script reads Iceberg table data from the Silver layer using Nessie catalog,
    performs additional transformations or optimizations,
    and writes the data to the Gold layer as Iceberg tables with partitioning.
    """
    # Initialize Spark session with Nessie and Iceberg configurations
    spark = SparkSession.builder \
        .appName("Gold Lake Processing") \
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

    # Read Iceberg table data from Silver layer using Nessie catalog
    companies_df = spark.table("nessie.silver.companies")
    employees_df = spark.table("nessie.silver.employees")
    departments_df = spark.table("nessie.silver.departments")

    # Additional transformations or optimizations
    companies_df = companies_df.withColumnRenamed("Employees", "num_employees")

    # Add processing timestamp
    companies_df = companies_df.withColumn("processed_at", current_timestamp())
    employees_df = employees_df.withColumn("processed_at", current_timestamp())
    departments_df = departments_df.withColumn("processed_at", current_timestamp())

    # Write data to the Gold layer as Iceberg tables with partitioning
    companies_df.writeTo("nessie.gold.companies") \
        .partitionedBy("Industry") \
        .using("iceberg") \
        .createOrReplace()

    employees_df.writeTo("nessie.gold.employees") \
        .partitionedBy("Company_Name") \
        .using("iceberg") \
        .createOrReplace()

    departments_df.writeTo("nessie.gold.departments") \
        .partitionedBy("Location") \
        .using("iceberg") \
        .createOrReplace()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    process_gold_lake_data()
