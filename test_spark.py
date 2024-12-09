import pyspark
from pyspark.sql import SparkSession


from pyspark.sql.functions import col
import os
import tarfile
from datetime import datetime, timedelta
from utils.utils import configure_spark
import os


# Set environment variables for MinIO access
os.environ["MINIO_ACCESS_KEY"] = "your_minio_access_key"
os.environ["MINIO_SECRET_KEY"] = "your_minio_secret_key"

def create_iceberg_table_if_not_exists(spark, table_name, df, partition_column):
    """Create an Iceberg table if it doesn't exist."""
    schema = ', '.join([f'`{field.name}` {field.dataType.simpleString()}' for field in df.schema.fields])
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        )
        USING iceberg
        PARTITIONED BY ({partition_column})
    """
    spark.sql(create_table_query)

def query_and_write_to_local_table(spark, nessie_table_name, date, local_table_name):
    """Query data from Nessie table for a given date and write it to the local Iceberg table if it doesn't already exist."""
    try:
        existing_data = spark.sql(f"SELECT * FROM {local_table_name} WHERE DATE = '{date}'")
        if existing_data.count() > 0:
            print(f"Data for {date} already exists in {local_table_name}. Skipping append.")
            return
    except Exception as e:
        print(f"No existing data found for {date} in {local_table_name}. Proceeding with append.")

    query = f"SELECT * FROM nessie.{nessie_table_name} WHERE DATE = '{date}';"
    df = spark.sql(query)
    create_iceberg_table_if_not_exists(spark, local_table_name, df)
    df.writeTo(local_table_name).append()
    print(f"Successfully appended data for {date} to {local_table_name}.")

def untar_latest_iceberg_warehouse(iceberg_warehouse_path):
    """Untar the most recent iceberg_warehouse tar.gz file if the directory doesn't exist."""
    base_name = os.path.basename(iceberg_warehouse_path)
    directory = os.path.dirname(iceberg_warehouse_path) or '.'  # Default to current directory if no parent

    try:
        # hdrusted to match the prefix pattern based on date
        tar_files = [f for f in os.listdir(directory) if f.endswith(f'{base_name}.tar.gz')]
    
        if not tar_files:
            print(f"No tar.gz file found for {iceberg_warehouse_path}. Proceeding without un-tarring.")
            return

        latest_tar_file = max(tar_files, key=lambda f: os.path.getctime(os.path.join(directory, f)))
        latest_tar_path = os.path.join(directory, latest_tar_file)
    
        if not os.path.exists(iceberg_warehouse_path):
            with tarfile.open(latest_tar_path, "r:gz") as tar:
                tar.extractall(path=directory)
            print(f"Un-tarred {latest_tar_path} successfully.")
        else:
            print(f"Directory {iceberg_warehouse_path} already exists. Skipping un-tarring.")
    except Exception as e:
        print(f"Failed to un-tar {latest_tar_path}. Error: {e}")


def read_local_and_write_to_nessie(spark, local_table_name, nessie_table_name, partition_column):
    """Read data from local Iceberg table or MinIO S3 Parquet file and write it to the Nessie-managed Iceberg table."""
    if local_table_name.startswith("s3://"):
        local_df = spark.read.parquet(local_table_name)
    else:
        minio_path = f"s3a://upload/bronze-data/{local_table_name}"
        local_df = spark.read.parquet(minio_path)
    
    create_iceberg_table_if_not_exists(spark, nessie_table_name, local_df, partition_column)
    local_df.writeTo(nessie_table_name).append()
    
def tar_iceberg_warehouse(iceberg_warehouse_path, date_prefix):
    """Tar the iceberg_warehouse directory with a date prefix."""
    tar_path = f"{date_prefix}_iceberg_warehouse.tar.gz"
    try:
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(iceberg_warehouse_path, arcname=os.path.basename(iceberg_warehouse_path))
        print(f"Tarred {iceberg_warehouse_path} to {tar_path} successfully.")
    except Exception as e:
        print(f"Failed to tar {iceberg_warehouse_path}. Error: {e}")

def process_dates(spark, nessie_table_name, local_table_name, dates):
    """Process a list of dates to query from Nessie and write to both local and Nessie Iceberg tables."""
    for date in dates:
        read_local_and_write_to_nessie(spark, local_table_name, nessie_table_name)
        query_and_write_to_local_table(spark, nessie_table_name, date, local_table_name)
        read_local_and_write_to_nessie(spark, local_table_name, nessie_table_name)
iceberg_warehouse_path = "iceberg_warehouse"

untar_latest_iceberg_warehouse(iceberg_warehouse_path)

if not os.path.exists(iceberg_warehouse_path):
    os.makedirs(iceberg_warehouse_path, exist_ok=True)

# Set environment variables for MinIO access
os.environ["MINIO_ACCESS_KEY"] = "your_minio_access_key"
os.environ["MINIO_SECRET_KEY"] = "your_minio_secret_key"

spark = configure_spark('minio', 'main')

tables_to_process = ['employees']

# Detect today's date
today_date = datetime.today().date()
#date = today_date - timedelta(days=1)
print(today_date)

# Process each table for today's date
for table in tables_to_process:
    nessie_table_name = table
    local_table_name = f"spark_catalog.default.{table}"
    process_dates(spark, nessie_table_name, local_table_name, [today_date])

# Stop the Spark session
spark.stop()

tar_iceberg_warehouse(iceberg_warehouse_path, today_date)
