import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from performance_tunning.dwh_indexes import dwh_create_indexes

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="performance_tunning.dwh_indexes",
    default_args=default_args,
    start_date=datetime(2023, 8, 16, 21, 0, 0),
    schedule_interval="@hourly",
)
def perform_test() -> None:
    """unit test for dwh_create_indexes"""

    @task()
    def perform_main() -> None:
        module_name = "performance_tunning.dwh_indexes"
        imported_function = "dwh_create_indexes"
        root_logger.info("")
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        dwh_create_indexes()
        root_logger.info("")

    perform_main()


result = perform_test()
