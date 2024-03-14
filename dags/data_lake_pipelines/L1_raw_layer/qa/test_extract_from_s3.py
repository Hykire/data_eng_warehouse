import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from data_lake_pipelines.L1_raw_layer.extract_from_s3 import get_data_raw_from_s3

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="data_lake_pipelines.L1_raw_layer.extract_from_s3",
    default_args=default_args,
    start_date=datetime(2023, 8, 13, 20, 0, 0),
    schedule_interval="@hourly",
)
def test_extract_from_s3():
    """unit test for pipeline extract_from_s3.py"""

    @task()
    def perform_main():

        module_name = "data_lake_pipelines.L1_raw_layer.extract_from_s3"
        imported_function = "get_data_raw_from_s3"
        root_logger.info("")
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        get_data_raw_from_s3()
        root_logger.info("")

    perform_main()


result = test_extract_from_s3()
