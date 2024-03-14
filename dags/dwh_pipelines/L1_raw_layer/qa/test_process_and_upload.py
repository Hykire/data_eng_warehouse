import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from dwh_pipelines.L1_raw_layer.process_and_upload import main

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="dwh_pipelines.L1_raw_layer.process_and_upload",
    default_args=default_args,
    start_date=datetime(2023, 8, 17, 17, 0, 0),
    schedule_interval="@hourly",
)
def test_dwh_L1_ray_layer_process_and_upload() -> None:
    """unit test for process_and_upload"""

    @task()
    def perform_main() -> None:
        module_name = "dwh_pipelines.L1_raw_layer.process_and_upload"
        imported_function = "main"
        root_logger.info("")
        main(dir_path="/opt/airflow/dags/data_lake_pipelines/L1_raw_layer/data_parquet")
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        root_logger.info("")

    perform_main()


result = test_dwh_L1_ray_layer_process_and_upload()
