import logging
from pathlib import Path
from typing import Any, List, Tuple, Union

import boto3

# Set up root root_logger
root_logger = logging.getLogger(__name__)


def get_file_folders(
    s3_client, bucket_name: str, prefix: str = ""
) -> Tuple[List[str], List[str]]:
    """Get metadata from s3 bucket

    Args:
        s3_client (boto3): boto3 obj
        bucket_name (str): name of s3 bucket
        prefix (str, optional): prefix of s3 bucket. Defaults to "".

    Returns:
        Tuple[List[str], List[str]]: returns file and folder names
    """
    file_names = []
    folders = []

    default_kwargs = {"Bucket": bucket_name, "Prefix": prefix}
    next_token = ""

    while next_token is not None:
        updated_kwargs = default_kwargs.copy()
        if next_token != "":
            updated_kwargs["ContinuationToken"] = next_token

        response = s3_client.list_objects_v2(**updated_kwargs)
        contents = response.get("Contents")

        for result in contents:
            key = result.get("Key")
            if key[-1] == "/":
                folders.append(key)
            else:
                file_names.append(key)

        next_token = response.get("NextContinuationToken")

    return file_names, folders


def download_files(
    s3_client,
    bucket_name: str,
    local_path: Union[str, Path],
    file_names: List[str],
    folders: List[str],
) -> None:
    """Download all data from bucket and store it in local path

    Args:
        s3_client (_type_): boto3 obj
        bucket_name (str): name of s3 bucket
        local_path (Path): path to store files
        file_names (List[str]): list of files's names
        folders (List[str]): list of folder's names
    """
    local_path = Path(local_path)

    for folder in folders:
        folder_path = Path.joinpath(local_path, folder)
        # Create all folders in the path
        folder_path.mkdir(parents=True, exist_ok=True)
    # counter = 0
    for file_name in file_names:
        file_path = Path.joinpath(local_path, file_name)
        # Create folder for parent directory
        file_path.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(bucket_name, file_name, str(file_path))
        root_logger.info(f"File: {file_name} in path: {file_path}")
        import os

        cwd = os.getcwd()
        root_logger.info(f"current directory: {cwd}")
        # if counter == 1:
        #     break
        # counter += 1


def get_data_raw_from_s3() -> None:
    """Retrieve data from S3 into local"""

    client = boto3.client("s3")
    file_names, folders = get_file_folders(client, "confidential")
    download_files(
        client,
        "confidential",
        "/opt/airflow/dags/data_lake_pipelines/L1_raw_layer",
        file_names,
        folders,
    )
