import os
import time
from datetime import datetime
from utils import get_current_snapshot_spark_duckdb
from rich import print
import sys




if __name__ == "__main__":
    start_time = time.time()
    today_date = datetime.today().date()
    iceberg_warehouse_path = "iceberg_warehouse"
    bucket_name = "bps-hc-rpa-sftp"
    prefix = "cadbo-datalake-snapshots/"
    dir_path = os.getcwd()
    local_dir = dir_path  # Root directory of the project
    output_folder = f"cadbo-datalake-snapshots"
    tar_path = f"{today_date}_iceberg_warehouse.tar.gz"
    duckdb_path = "cadbo_datalke.db"

    
    if not os.path.exists(f"cadbo-datalake-snapshots"):
        os.mkdir(f"cadbo-datalake-snapshots")
    

    provider = "minio"




    get_current_snapshot_spark_duckdb(
        bucket_name,
        provider,
        prefix,
        local_dir,
        today_date,
        iceberg_warehouse_path,
        output_folder,
         ['companies.parquet'],
        branch = 'main',
        download_latest_snapshot_flag=False,
        upload_to_s3_flag=False,
        boot_strap_nessie=True,
    )

    end_time = time.time()
    print(f"Script execution duration: {end_time - start_time} seconds")