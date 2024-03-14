import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from dwh_pipelines.L4_dwh_layer.reporting.report_generator import create_plots

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="dwh_pipelines.L4_dwh_layer.reporting.report_generator",
    default_args=default_args,
    start_date=datetime(2023, 8, 16, 20, 0, 0),
    schedule_interval="@hourly",
)
def perform_test() -> None:
    """unit test for create_plots"""

    @task()
    def perform_main() -> None:
        module_name = "dwh_pipelines.L4_dwh_layer.reporting.report_generator"
        imported_function = "create_plots"
        root_logger.info("")
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        create_plots()
        root_logger.info("")

    perform_main()


result = perform_test()
