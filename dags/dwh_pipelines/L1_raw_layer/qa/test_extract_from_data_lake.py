import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from dwh_pipelines.L1_raw_layer.extract_from_data_lake import get_data_raw_from_dl

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="dwh_pipelines.L1_raw_layer.extract_from_data_lake",
    default_args=default_args,
    start_date=datetime(2023, 8, 14, 16, 0, 0),
    schedule_interval="@hourly",
)
def test_dwh_L1_ray_layer_extract_from_data_lake() -> None:
    """unit test for extract_from_data_lake"""

    @task()
    def perform_process() -> None:
        module_name = "dwh_pipelines.L1_raw_layer.extract_from_data_lake"
        imported_function = "get_data_raw_from_dl"
        root_logger.info("")
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        get_data_raw_from_dl()
        root_logger.info("")

    perform_process()


result = test_dwh_L1_ray_layer_extract_from_data_lake()
