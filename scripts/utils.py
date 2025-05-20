import pandas as pd
import re
import pyspark
from pyspark.sql.functions import col, collect_list, udf, concat_ws
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import boto3
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import duckdb  # Add this import
from dremio_simple_query.connect import DremioConnection
from dotenv import load_dotenv
import os
import tarfile
from datetime import datetime
from rich import print as rprint


# Load environment variables from .env file
load_dotenv()

minio_url = os.getenv("MINIO_LOAD_BALANCER", "http://localhost:9000")

def create_iceberg_table_if_not_exists(spark, table_name, df, partition_column="Name"):
    """Create an Iceberg table if it doesn't exist."""
    schema = ", ".join(
        [
            f"`{field.name}` {field.dataType.simpleString()}"
            for field in df.schema.fields
        ]
    )
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        )
        USING iceberg
        PARTITIONED BY ({partition_column})
    """
    rprint(f"Creating table {table_name} if it does not exist.")
    spark.sql(create_table_query)
    rprint(f"Table {table_name} is ready.")


def query_and_write_to_local_table(spark, nessie_table_name, date, local_table_name):
    """Query data from Nessie table for a given date and write it to the local Iceberg table if it doesn't already exist."""
    rprint(f"Processing date: {date} for table: {nessie_table_name}")
    try:
        existing_data = spark.sql(
            f"SELECT * FROM {local_table_name} WHERE DATE = '{date}'"
        )
        if existing_data.count() > 0:
            rprint(
                f"Data for {date} already exists in {local_table_name}. Skipping append."
            )
            return
    except Exception as e:
        rprint(
            f"No existing data found for {date} in {local_table_name}. Proceeding with append. Error: {e}"
        )

    query = f"SELECT * FROM nessie.{nessie_table_name} WHERE DATE = '{date}'"
    rprint(f"Executing query: {query}")
    df = spark.sql(query)
    if df.count() == 0:
        rprint(
            f"No data found for {date} in Nessie table {nessie_table_name}. Skipping."
        )
        return
    create_iceberg_table_if_not_exists(spark, local_table_name, df)
    df.writeTo(local_table_name).append()
    rprint(f"Successfully appended data for {date} to {local_table_name}.")


def get_dates_in_nessie(spark, nessie_table_name):
    """Get a list of distinct dates available in the Nessie table."""
    rprint(f"Fetching distinct dates from Nessie table: {nessie_table_name}")
    df = spark.sql(f"SELECT DISTINCT DATE FROM nessie.{nessie_table_name}")
    dates = [row["DATE"] for row in df.collect()]
    rprint(f"Found {len(dates)} dates in Nessie table {nessie_table_name}.")
    return dates


def get_dates_in_local(spark, local_table_name):
    """Get a list of distinct dates available in the local Iceberg table."""
    rprint(f"Fetching distinct dates from local table: {local_table_name}")
    try:
        df = spark.sql(f"SELECT DISTINCT DATE FROM {local_table_name}")
        dates = [row["DATE"] for row in df.collect()]
        rprint(f"Found {len(dates)} dates in local table {local_table_name}.")
    except Exception as e:
        rprint(
            f"Table {local_table_name} does not exist. Initializing with empty dates list. Error: {e}"
        )
        dates = []
    return dates


def read_local_and_write_to_nessie(spark, local_table_name, nessie_table_name):
    """Read data from local Iceberg table and write it to a new Nessie-managed Iceberg table with enhanced logging."""
    rprint(f"Starting to read from local table {local_table_name}")
    try:
        if local_table_name.endswith(".csv"):
            local_df = spark.read.option("header", "true").csv(local_table_name)
            # Removed duplicate "iceberg_warehouse." prefix
            table_base = local_table_name.split("/")[-1].replace(".csv", "")
            save_table = table_base
            local_df.write.format("iceberg").mode("overwrite").saveAsTable(save_table)
            local_df = spark.read.format("iceberg").table(save_table)
        elif local_table_name.endswith(".json"):
            local_df = spark.read.option("multiline", "true").json(local_table_name)
            table_base = local_table_name.split("/")[-1].replace(".json", "")
            save_table = table_base
            local_df.write.format("iceberg").mode("overwrite").saveAsTable(save_table)
            local_df = spark.read.format("iceberg").table(save_table)
        elif local_table_name.endswith(".parquet"):
            local_df = spark.read.parquet(local_table_name)
            table_base = local_table_name.split("/")[-1].replace(".parquet", "")
            save_table = table_base
            local_df.write.format("iceberg").mode("overwrite").saveAsTable(save_table)
            local_df = spark.read.format("iceberg").table(save_table)
        else:
            local_df = spark.read.format("iceberg").table(local_table_name)
        
        record_count = local_df.count()
        rprint(f"Read {record_count} records from {local_table_name}")
    except Exception as e:
        rprint(f"Error reading from local table {local_table_name}: {e}")
        return

    # Sanitize table name to remove file extension if present.
    cleaned_table_name = nessie_table_name.rsplit('.', 1)[0] if nessie_table_name.endswith((".csv", ".json", ".parquet")) else nessie_table_name
    nessie_iceberg_table_name = f"nessie.{cleaned_table_name}"
    
    rprint(f"Creating or verifying Nessie table {nessie_iceberg_table_name}")
    try:
        create_iceberg_table_if_not_exists(spark, nessie_iceberg_table_name, local_df)
    except Exception as e:
        rprint(f"Error creating Nessie table {nessie_iceberg_table_name}: {e}")
        return

    rprint(f"Writing data to Nessie table {nessie_iceberg_table_name}")
    try:
        local_df.writeTo(nessie_iceberg_table_name).append()
        rprint(f"Successfully wrote data to Nessie table {nessie_iceberg_table_name}")
    except Exception as e:
        rprint(f"Error writing to Nessie table {nessie_iceberg_table_name}: {e}")


def get_dates_in_duckdb(duckdb_conn, table_name):
    """Get a list of distinct dates available in the DuckDB table."""
    rprint(f"Fetching distinct dates from DuckDB table: {table_name}")
    try:
        query_check_table = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '{table_name}'"
        table_exists = duckdb_conn.execute(query_check_table).fetchone()[0]
        if table_exists == 0:
            rprint(f"DuckDB table {table_name} does not exist.")
            dates = []
        else:
            df = duckdb_conn.execute(
                f"SELECT DISTINCT DATE FROM {table_name}"
            ).fetchall()
            dates = [row[0] for row in df]
            rprint(f"Found {len(dates)} dates in DuckDB table {table_name}.")
    except Exception as e:
        rprint(f"Error fetching dates from DuckDB table {table_name}: {e}")
        dates = []
    return dates


def write_to_duckdb(duckdb_conn, table_name, date):
    """Write data for the specific date from Iceberg to DuckDB."""
    rprint(f"Writing date {date} to DuckDB table {table_name}")
    # Install and load the Iceberg extension
    duckdb_conn.execute("INSTALL iceberg;")
    duckdb_conn.execute("LOAD iceberg;")

    # Check if the DuckDB table exists
    query_check_table = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '{table_name}'"
    table_exists = duckdb_conn.execute(query_check_table).fetchone()[0]

    if table_exists == 0:
        rprint(
            f"Table {table_name} does not exist in DuckDB. Creating table and inserting data for date {date}."
        )
        query_insert = f"""
        CREATE TABLE {table_name} AS
        SELECT *
        FROM iceberg_scan('iceberg_warehouse/default/{table_name}', allow_moved_paths = true)
        WHERE DATE = '{date}';
        """
        duckdb_conn.execute(query_insert)
        rprint(
            f"Successfully created table {table_name} and inserted data for date {date} in DuckDB."
        )
    else:
        query_check = f"SELECT COUNT(*) FROM {table_name} WHERE DATE = '{date}'"
        result = duckdb_conn.execute(query_check).fetchone()[0]

        if result > 0:
            rprint(
                f"Data for date {date} already exists in {table_name} in DuckDB. Skipping append."
            )
        else:
            query_insert = f"""
            INSERT INTO {table_name}
            SELECT *
            FROM iceberg_scan('iceberg_warehouse/default/{table_name}', allow_moved_paths = true)
            WHERE DATE = '{date}';
            """
            duckdb_conn.execute(query_insert)
            rprint(
                f"Successfully appended data for date {date} to {table_name} in DuckDB."
            )


def tar_iceberg_warehouse(iceberg_warehouse_path, date_prefix, output_folder):
    """Tar the iceberg_warehouse directory with a date prefix."""
    tar_path = f"{output_folder}/{date_prefix}_iceberg_warehouse.tar.gz"
    rprint(f"Creating tarball {tar_path} for {iceberg_warehouse_path}")
    try:
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(
                iceberg_warehouse_path, arcname=os.path.basename(iceberg_warehouse_path)
            )
        rprint(f"Tarred {iceberg_warehouse_path} to {tar_path} successfully.")
    except Exception as e:
        rprint(f"Failed to tar {iceberg_warehouse_path}. Error: {e}")


def untar_latest_iceberg_warehouse(iceberg_warehouse_path):
    """Untar the most recent iceberg_warehouse tar.gz file if the directory doesn't exist."""
    base_name = os.path.basename(iceberg_warehouse_path)
    directory = os.path.dirname(iceberg_warehouse_path) or "."

    try:
        tar_files = [f for f in os.listdir(directory) if f.endswith(".tar.gz")]

        if not tar_files:
            rprint(
                f"No tar.gz file found for {iceberg_warehouse_path}. Proceeding without un-tarring."
            )
            return

        latest_tar_file = max(
            tar_files, key=lambda f: os.path.getctime(os.path.join(directory, f))
        )
        latest_tar_path = os.path.join(directory, latest_tar_file)

        if not os.path.exists(iceberg_warehouse_path):
            rprint(
                f"Extracting {latest_tar_path} as {iceberg_warehouse_path} does not exist."
            )
            with tarfile.open(latest_tar_path, "r:gz") as tar:
                tar.extractall(path=directory)
            rprint(f"Un-tarred {latest_tar_path} successfully.")
        else:
            rprint(
                f"Directory {iceberg_warehouse_path} already exists. Skipping un-tarring."
            )
    except Exception as e:
        rprint(f"Failed to un-tar {latest_tar_path}. Error: {e}")


def process_table(
    spark,
    nessie_table_name,
    local_table_name,
    duckdb_conn,
    write_local=True,
    write_duckdb=True,
    boot_strap_nessie=False,
):
    """Process a single table by syncing missing dates from Nessie to local Iceberg and DuckDB."""
    rprint(f"Processing table: {nessie_table_name}")

    if boot_strap_nessie:
        rprint("Bootstrapping Nessie table.")
        read_local_and_write_to_nessie(spark, local_table_name, nessie_table_name)
        return

    dates_in_nessie = set(get_dates_in_nessie(spark, nessie_table_name))
    dates_in_local = set(get_dates_in_local(spark, local_table_name))
    dates_in_duckdb = set(get_dates_in_duckdb(duckdb_conn, nessie_table_name))

    dates_missing_in_local = dates_in_nessie - dates_in_local
    rprint(
        f"{len(dates_missing_in_local)} dates missing in local table {local_table_name}."
    )

    if write_local:
        for date in dates_missing_in_local:
            query_and_write_to_local_table(
                spark, nessie_table_name, date, local_table_name
            )
        dates_in_local.update(dates_missing_in_local)
    else:
        rprint("write_local is disabled. Skipping writing to local table.")

    dates_missing_in_duckdb = dates_in_local - dates_in_duckdb
    rprint(
        f"{len(dates_missing_in_duckdb)} dates missing in DuckDB table {nessie_table_name}."
    )

    if write_duckdb:
        for date in dates_missing_in_duckdb:
            write_to_duckdb(duckdb_conn, nessie_table_name, date)
    else:
        rprint("write_duckdb is disabled. Skipping writing to DuckDB.")


def get_current_snapshot_spark_duckdb(
    bucket_name,
    provider,
    prefix,
    local_dir,
    today_date,
    iceberg_warehouse_path,
    output_folder,
    tables_to_process,
    branch,
    download_latest_snapshot_flag=True,
    upload_to_s3_flag=False,
    boot_strap_nessie=False,
):
    """Main function to process the tables and upload to S3."""
    if download_latest_snapshot_flag:
        rprint("Downloading the latest snapshot from S3.")
        download_latest_snapshot(bucket_name, prefix, local_dir)

    if not os.path.exists(output_folder):
        rprint(f"Creating directory {output_folder}.")
        os.makedirs(output_folder, exist_ok=True)

    rprint("Starting the untar process for the Iceberg warehouse.")
    untar_latest_iceberg_warehouse(iceberg_warehouse_path)

    # Initialize Spark session
    rprint("Configuring Spark session.")
    spark = configure_spark(provider, branch)
    rprint(f"Today's date: {today_date}")

    rprint(f"Tables to process: {tables_to_process}")

    # Open DuckDB connection
    rprint("Connecting to DuckDB.")
    duckdb_conn = duckdb.connect(f"{output_folder}/cadbo_datalke.db")
     #Process each table
    for table in tables_to_process:
        rprint(f"Starting processing for table: {table}")
        nessie_table_name = table
        if table.endswith((".csv", ".json", ".parquet")):
            local_table_name = table
        else:
            local_table_name = f"spark_catalog.default.{table}"
        process_table(
            spark,
            nessie_table_name,
            local_table_name,
            duckdb_conn,
            boot_strap_nessie=boot_strap_nessie,
        )

    # Close DuckDB connection
    rprint("Closing DuckDB connection.")
    duckdb_conn.close()

    # Stop the Spark session
    rprint("Stopping Spark session.")
    spark.stop()

    if upload_to_s3_flag:
        rprint("Uploading tarball to S3.")
        tar_iceberg_warehouse(iceberg_warehouse_path, today_date, output_folder)
        upload_to_s3(
            output_folder,
            bucket_name,
            "cadbo-datalake-snapshots",
            provider="aws2",
            delete_existing=False,
        )

    



def query_dremio(query: str) -> pd.DataFrame:
    
    """
    Queries a Dremio table within the 'nessie-1' catalog filtered by a specific date.
    
    :param table_name: The name of the table to query.
    :param date: The date to filter the query (YYYY-MM-DD format).
    :return: A pandas DataFrame containing the query results.
    """

    arrow_endpoint = "grpc+tls://data.dremio.cloud:443"

    token  = os.getenv('TOKEN')
    # Establish Dremio Connection
    dremio = DremioConnection(token, arrow_endpoint)

    # Query data from Dremio and load it into a pandas DataFrame
    pandas_df = dremio.toPandas(query)
    pandas_df.columns = [col.upper() for col in pandas_df.columns]

    return pandas_df


def query_duckdb(table_name, date, file_type, provider):
    aws_access_key, aws_secret_key, region = get_credentials(provider)
    filename = f"temporary_{table_name}.{file_type}"
    with duckdb.connect() as con:
        # Install and load the httpfs extension
        con.sql("INSTALL httpfs;")
        con.sql("LOAD httpfs;")

        # Create a persistent secret for S3 access
        con.sql(f"""
        CREATE SECRET cadbo_secret (
            TYPE S3,
            KEY_ID '{aws_access_key}',
            SECRET '{aws_secret_key}',
            REGION '{region}'
        );
        """)

        # Attach the S3 bucket as a database
        con.sql("ATTACH 's3://bps-hc-rpa-sftp/cadbo-datalake-snapshots/cadbo_datalke.db' AS cadbo_datlake;")

        # Export the specified table to a Parquet file
        con.sql(f"COPY (SELECT * FROM cadbo_datlake.{table_name} WHERE DATE = '{date}') TO '{filename}';")
        if file_type == 'parquet':
            df = pd.read_parquet(filename, engine='pyarrow')
        else:
            df = pd.read_csv(filename, engine='pyarrow')
        con.close()
        os.remove(filename)
        return df
    
def get_db_tables(table_name, date, spark):
    # Initialize Spark session with the configured SparkConf

    query = f'SELECT * FROM nessie.{table_name} WHERE date = \'{date}\';'
    df = spark.sql(query).toPandas()
    
    # Convert column headers to uppercase
    df.columns = [col.upper() for col in df.columns]


    return df

def delete_s3_directory(s3, bucket, directory):
    # Ensure the directory name ends with a slash to target only this directory
    if not directory.endswith('/'):
        directory += '/'
    
    # List all objects in the exact directory
    response = s3.list_objects_v2(Bucket=bucket, Prefix=directory)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"Deleting {obj['Key']} from s3://{bucket}/{obj['Key']}")
            s3.delete_object(Bucket=bucket, Key=obj['Key'])

def upload_to_s3(output_folder, s3_bucket, s3_prefix, provider, delete_existing=False):
    aws_access_key, aws_secret_key, region = get_credentials(provider)
    if provider == 'minio':
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region,
            endpoint_url=minio_url  # Use the MinIO endpoint
        )
    else:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

    # Delete the existing prefix directory if it exists and the flag is set to True
    if delete_existing:
        delete_s3_directory(s3, s3_bucket, s3_prefix)

    for root, dirs, files in os.walk(output_folder):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, output_folder)
            s3_path = os.path.join(s3_prefix, relative_path)

            print(f"Uploading {local_path} to s3://{s3_bucket}/{s3_path}")
            s3.upload_file(local_path, s3_bucket, s3_path)


def download_from_s3(bucket_name, provider, s3_file_key, local_file_path):
    aws_access_key, aws_secret_key, region = get_credentials(provider)
    if provider == 'minio':
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region,
            endpoint_url=minio_url  # Use the MinIO endpoint
        )
    else:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
    
    try:
        print(f"Downloading {s3_file_key} from bucket {bucket_name} to {local_file_path}.")
        s3.download_file(bucket_name, s3_file_key, local_file_path)
        print("Download completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")



def upload_csv_to_nessie(tables, branch, directory_prefix):
    """Upload CSV files to the Nessie database."""
    spark = configure_spark('aws', branch)
   
    try:
        for table in tables:
            df = spark.read.option("header", "true").csv(f'{directory_prefix}/{table}.csv')
            # Sanitize table name to remove file extension if present.
            cleaned_table = table.rsplit('.', 1)[0] if table.endswith((".csv", ".json", ".parquet")) else table
            nessie_table = f"nessie.{cleaned_table}"
            try:
                df_casted = df.withColumn("date", col("date").cast("date"))
                # Check if the date already exists in the table
                existing_dates = spark.read.format("iceberg").load(nessie_table).select("date").distinct()
                new_dates = df_casted.select("date").distinct()
                dates_to_insert = new_dates.subtract(existing_dates)
                
                if dates_to_insert.count() > 0:
                    df_casted = df_casted.join(dates_to_insert, on="date", how="inner")
                    df_casted.write.format("iceberg").mode("append").partitionBy("date").save(nessie_table)
                    print(f"Successfully uploaded {table}.csv to table {table}")
                else:
                    print(f"No new dates to insert for table {table}")
            except Exception as e:
                df_casted = df.withColumn("DATE", col("DATE").cast("DATE"))
                existing_dates = spark.read.format("iceberg").load(nessie_table).select("DATE").distinct()
                new_dates = df_casted.select("DATE").distinct()
                dates_to_insert = new_dates.subtract(existing_dates)
                
                if dates_to_insert.count() > 0:
                    df_casted = df_casted.join(dates_to_insert, on="DATE", how="inner")
                    df_casted.write.format("iceberg").mode("append").partitionBy("DATE").save(nessie_table)
                    print(f"Successfully uploaded {table}.csv to table {table}")
                else:
                    print(f"No new dates to insert for table {table}")
    except Exception as e:
        print(f"Failed to process file {table}.csv : {e}")
    finally:
        spark.stop()



def get_credentials(provider):
    if provider == 'aws':
        return os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY"), os.getenv("AWS_REGION")
    elif provider == 'minio':
        return os.getenv("MINIO_ACCESS_KEY"), os.getenv("MINIO_SECRET_KEY"), os.getenv("MINIO_REGION")
    elif provider == 'aws2':
        return os.getenv("AWS2_ACCESS_KEY"), os.getenv("AWS2_SECRET_KEY"), os.getenv("AWS2_REGION")
    else:
        raise ValueError("Unsupported provider. Use 'aws' or 'minio' or 'aws2.")
    

def configure_environment(provider):
    access_key, secret_key , region = get_credentials(provider)
    if not access_key or not secret_key or not region:
        raise EnvironmentError(f"{provider.upper()} credentials not found.")
    os.environ["AWS_ACCESS_KEY_ID"] = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    os.environ["AWS_REGION"] = region
    print(f"Configured {provider.upper()} environment for the Following Region: {region}")

def configure_spark(provider, branch):
    """Configure and initialize Spark session."""
    configure_environment(provider)
    NESSIE_URI = "https://nessie.yorko.io/api/v1"
    WAREHOUSE = "s3a://warehouse/"
    AWS_S3_ENDPOINT = "https://minio-backend.yorko.io"
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

def download_latest_snapshot(bucket_name, prefix, local_dir):
    """
    Downloads the latest tar file and the cadbo_datalke.db file from the S3 bucket and saves them to the local directory.
    """
    aws_access_key, aws_secret_key, region = get_credentials('aws2')
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    # List all tar.gz files and cadbo_datalke.db files
    tar_files = []
    db_files = []
    for page in response_iterator:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.tar.gz'):
                tar_files.append({'Key': key, 'LastModified': obj['LastModified']})
            elif key.endswith('cadbo_datalke.db'):
                db_files.append({'Key': key, 'LastModified': obj['LastModified']})

    if not tar_files:
        print(f"No tar files found in s3://{bucket_name}/{prefix}")
        return

    if not db_files:
        print(f"No cadbo_datalke.db files found in s3://{bucket_name}/{prefix}")
        return

    # Find the latest tar.gz file and cadbo_datalke.db file
    latest_tar_file = max(tar_files, key=lambda x: x['LastModified'])
    latest_db_file = max(db_files, key=lambda x: x['LastModified'])

    # Download the latest tar file to the local directory
    local_tar_file_path = os.path.join(local_dir, os.path.basename(latest_tar_file['Key']))
    print(f"Downloading {latest_tar_file['Key']} to {local_tar_file_path}")
    s3.download_file(bucket_name, latest_tar_file['Key'], local_tar_file_path)

    # Download the latest cadbo_datalke.db file to the prefix/cadbo_datalke.db
    local_db_file_path = os.path.join(local_dir, prefix,'cadbo_datalke.db')
    print(f"Downloading {latest_db_file['Key']} to {local_db_file_path}")
    s3.download_file(bucket_name, latest_db_file['Key'], local_db_file_path)

    # Extract the tar.gz file
    import tarfile
    if local_tar_file_path.endswith("tar.gz"):
        with tarfile.open(local_tar_file_path, "r:gz") as tar:
            tar.extractall(path=local_dir)
            print(f"Extracted {local_tar_file_path} to {local_dir}")
