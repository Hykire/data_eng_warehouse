import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from governance.data_team_rbac import perform_dwh_access_control

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="governance.data_team_rbac",
    default_args=default_args,
    start_date=datetime(2023, 8, 16, 22, 0, 0),
    schedule_interval="@hourly",
)
def perform_test() -> None:
    """unit test for perform_dwh_access_control"""

    @task()
    def perform_main() -> None:
        module_name = "governance.data_team_rbac"
        imported_function = "perform_dwh_access_control"
        root_logger.info("")
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        perform_dwh_access_control()
        root_logger.info("")

    perform_main()


result = perform_test()
