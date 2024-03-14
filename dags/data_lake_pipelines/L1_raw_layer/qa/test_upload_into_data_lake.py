import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from data_lake_pipelines.L1_raw_layer.upload_into_data_lake import (
    upload_data_into_data_lake,
)

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="data_lake_pipelines.L1_raw_layer.upload_into_data_lake",
    default_args=default_args,
    start_date=datetime(2023, 8, 13, 20, 0, 0),
    schedule_interval="@hourly",
)
def test_upload_into_data_lake():
    """unit test for pipeline upload_into_data_lake.py"""

    @task()
    def perform_main():

        module_name = "data_lake_pipelines.L1_raw_layer.upload_into_data_lake"
        imported_function = "upload_data_into_data_lake"
        root_logger.info("")
        upload_data_into_data_lake()
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        root_logger.info("")

    perform_main()


result = test_upload_into_data_lake()
