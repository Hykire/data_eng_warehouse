import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from dwh_pipelines.L3_semantic_layer.prod.create_dim_prod_env import main_dwh_l3_prod

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="dwh_pipelines.L3_semantic_layer.prod.create_dim_prod_env",
    default_args=default_args,
    start_date=datetime(2023, 8, 16, 15, 0, 0),
    schedule_interval="@hourly",
)
def perform_test() -> None:
    """unit test for main_dwh_l3_prod"""

    @task()
    def perform_main() -> None:
        module_name = "dwh_pipelines.L3_semantic_layer.prod.create_dim_prod_env"
        imported_function = "main_dwh_l3_prod"
        root_logger.info("")
        root_logger.info(
            f"Now importing '{imported_function}' function from '{module_name}' module..."
        )
        main_dwh_l3_prod()
        root_logger.info("")

    perform_main()


result = perform_test()
