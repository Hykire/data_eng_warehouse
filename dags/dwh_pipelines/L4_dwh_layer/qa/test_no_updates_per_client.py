import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from dwh_pipelines.L4_dwh_layer.user_access_layer.no_updates_per_client import (
    perform_create_view_no_updates_per_client,
)

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="dwh_pipelines.L4_dwh_layer.user_access_layer.no_updates_per_client",
    default_args=default_args,
    start_date=datetime(2023, 8, 16, 20, 0, 0),
    schedule_interval="@hourly",
)
def perform_test() -> None:
    """unit test for perform_create_view_no_updates_per_client"""

    @task()
    def perform_main() -> None:
        module_name = (
            "dwh_pipelines.L4_dwh_layer.user_access_layer.no_updates_per_client"
        )
        imported_function = "perform_create_view_no_updates_per_client"
        root_logger.info("")
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        perform_create_view_no_updates_per_client()
        root_logger.info("")

    perform_main()


result = perform_test()
