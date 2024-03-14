import os
from pathlib import Path

import boto3
import pandas as pd

DATA_PATH_ORIGIN = "/opt/airflow/dags/data_lake_pipelines/L1_raw_layer/data"
DATA_PATH_DESTINATION = (
    "/opt/airflow/dags/data_lake_pipelines/L1_raw_layer/data_parquet"
)


def process_files() -> None:
    """transform json to parquet for better performance"""
    df = pd.DataFrame()
    for filename in os.listdir(DATA_PATH_ORIGIN):
        with open(
            os.path.join(DATA_PATH_ORIGIN, filename), "r"
        ) as f:  # open in readonly mode
            df = pd.read_json(f, lines=True)
            df.to_parquet(DATA_PATH_DESTINATION + "/" + filename[:-5] + ".parquet")


def upload() -> None:
    """upload parquet file into Data Lake, divided by folders per hour"""
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("confidential")
    for filename in os.listdir(DATA_PATH_DESTINATION):
        file_path = os.path.join(DATA_PATH_DESTINATION, filename)
        pd.read_parquet(file_path)
        arr_filename = filename.split("-")
        # year + month + day + hour + filename
        new_file_path = (
            arr_filename[0]
            + "-"
            + arr_filename[1]
            + "-"
            + arr_filename[2]
            + " "
            + arr_filename[3]
            + "-00-00/"
            + filename
        )
        bucket.upload_file(file_path, new_file_path)


def upload_data_into_data_lake() -> None:
    """Upload data into data lake"""
    process_files()
    upload()
