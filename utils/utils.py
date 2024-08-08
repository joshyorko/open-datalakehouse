import pandas as pd
import re
import pyspark
from pyspark.sql.functions import col, collect_list, udf, concat_ws
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
from boto3.s3.transfer import TransferConfig
import subprocess
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed



def get_db_tables(table_name, date, spark):
    # Initialize Spark session with the configured SparkConf

    query = f'SELECT * FROM nessie.{table_name} WHERE DATE = \'{date}\';'
    df = spark.sql(query)
    
    # Convert column headers to uppercase
    #df.columns = [col.upper() for col in df.columns]


    return df


def remove_s3_directory(bucket_name, s3_prefix):
    aws_access_key, aws_secret_key = get_credentials('minio')
    print(aws_access_key)
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        endpoint_url="http://a8c78c2e92dd442ff93f5d247b6a2cc7-1221119726.us-west-2.elb.amazonaws.com:9000"
    )
    s3 = boto3.client("s3")
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)

    for page in pages:
        if 'Contents' in page:
            keys = [{'Key': obj['Key']} for obj in page['Contents']]
            s3.delete_objects(Bucket=bucket_name, Delete={'Objects': keys})

def upload_to_s3(output_folder, s3_bucket, s3_prefix):
    aws_access_key, aws_secret_key = get_credentials('minio')
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        endpoint_url="http://a8c78c2e92dd442ff93f5d247b6a2cc7-1221119726.us-west-2.elb.amazonaws.com:9000"  # Ensure this is the correct endpoint
    )
    
    allocation_s3_path = 'allocation/allocation.csv'

    for root, dirs, files in os.walk(output_folder):
        for file in files:
            local_path = os.path.join(root, file)

            # Determine S3 path based on file
            if file == 'allocation.csv':
                s3_path = allocation_s3_path
            else:
                relative_path = os.path.relpath(local_path, output_folder)
                s3_path = os.path.join(s3_prefix, relative_path)

            print(f"Uploading {local_path} to s3://{s3_bucket}/{s3_path}")
            s3.upload_file(local_path, s3_bucket, s3_path)

def upload_csv_to_nessie(tables,branch):
    """Upload CSV files to the Nessie database."""
    spark = configure_spark('minio',branch)
   
    try:
        for table in tables:
            df = spark.read.option("header", "true").csv(f'downloads/{table}.csv')
            df_casted = df.withColumn("date", col("date").cast("date"))
            df_casted.write.format("iceberg").mode("append").partitionBy("date").save(
                f"nessie.{table}"
            )
            print(f"Successfully uploaded {table}.csv to table {table}")
    except Exception as e:
        print(f"Failed to process file {table}.csv : {e}")
    finally:
        spark.stop()


def get_credentials(provider):
    if provider == 'aws':
        return os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY")
    elif provider == 'minio':
        return os.getenv("MINIO_ACCESS_KEY"), os.getenv("MINIO_SECRET_KEY")
    else:
        raise ValueError("Unsupported provider. Use 'aws' or 'minio'.")

def configure_environment(provider):
    access_key, secret_key = get_credentials(provider)
    if not access_key or not secret_key:
        raise EnvironmentError(f"{provider.upper()} credentials not found.")
    os.environ["AWS_ACCESS_KEY_ID"] = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    print(f"Configured {provider.upper()} environment")

def configure_spark(provider, branch):
    """Configure and initialize Spark session."""
    configure_environment(provider)
    NESSIE_URI = "http://192.168.1.83:19120/api/v1"
    WAREHOUSE = "s3a://lakehouse/"
    AWS_S3_ENDPOINT = "http://192.168.1.82:9000"
    AWS_REGION = "us-east-1"
    iceberg_warehouse_path = "iceberg_warehouse"  # Change this to your desired path

    conf = (
        pyspark.SparkConf()
        .setAppName("Iceberg Partitioned Data Write")
        .set(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3,software.amazon.awssdk:bundle:2.17.81,org.apache.hadoop:hadoop-aws:3.3.1",
        )
        .set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .set("spark.sql.catalog.nessie.ref", branch)
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .set("spark.sql.catalog.nessie.s3.endpoint", AWS_S3_ENDPOINT)
        .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)
        .set("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .set("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .set("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.spark_catalog.type", "hadoop")
        .set("spark.sql.catalog.spark_catalog.warehouse", iceberg_warehouse_path)
        .set("spark.executor.memory", "8g")
        .set("spark.driver.memory", "8g")
        .set("spark.executor.instances", "5")
        .set("spark.local.dir", "/tmp/spark-temp")
    )

    return SparkSession.builder.config(conf=conf).getOrCreate()





