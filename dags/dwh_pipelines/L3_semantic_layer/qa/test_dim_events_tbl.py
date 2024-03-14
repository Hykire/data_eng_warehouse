import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from dwh_pipelines.L3_semantic_layer.dev.dim_events_tbl import main_dwh_l3_dev

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="dwh_pipelines.L3_semantic_layer.dev.dim_events_tbl",
    default_args=default_args,
    start_date=datetime(2023, 8, 17, 17, 0, 0),
    schedule_interval="@hourly",
)
def perform_test() -> None:
    """unit test for main_dwh_l3_dev"""

    @task()
    def perform_main() -> None:
        module_name = "dwh_pipelines.L3_semantic_layer.dev.dim_events_tbl"
        imported_function = "main_dwh_l3_dev"
        root_logger.info("")
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        main_dwh_l3_dev()
        root_logger.info("")

    perform_main()


result = perform_test()
