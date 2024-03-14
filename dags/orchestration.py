import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from data_lake_pipelines.L1_raw_layer.extract_from_s3 import get_data_raw_from_s3
from data_lake_pipelines.L1_raw_layer.upload_into_data_lake import (
    upload_data_into_data_lake,
)
from dwh_pipelines.L1_raw_layer.extract_from_data_lake import get_data_raw_from_dl
from dwh_pipelines.L1_raw_layer.process_and_upload import main
from dwh_pipelines.L2_staging_layer.dev.stg_events_tbl import main_dwh_l2_staging
from dwh_pipelines.L2_staging_layer.prod.create_stg_prod_env import main_dwh_l2_prod
from dwh_pipelines.L3_semantic_layer.dev.dim_events_tbl import main_dwh_l3_dev
from dwh_pipelines.L3_semantic_layer.prod.create_dim_prod_env import main_dwh_l3_prod
from dwh_pipelines.L4_dwh_layer.datamarts.fact_events_tbl import perform_fact_events_tbl
from dwh_pipelines.L4_dwh_layer.reporting.report_generator import create_plots
from dwh_pipelines.L4_dwh_layer.user_access_layer.latest_updates_on_top1_client import (
    perform_create_view_latest_updates_on_top1_client,
)
from dwh_pipelines.L4_dwh_layer.user_access_layer.no_updates_per_client import (
    perform_create_view_no_updates_per_client,
)
from governance.data_team_rbac import perform_dwh_access_control
from performance_tunning.dwh_indexes import dwh_create_indexes

default_args = {"owner": "Hykire", "retries": 5, "retry_delay": timedelta(minutes=5)}
# Set up root root_logger
root_logger = logging.getLogger(__name__)


@dag(
    dag_id="main_dag",
    default_args=default_args,
    start_date=datetime(2023, 8, 17, 20, 0, 0),
    schedule_interval="@hourly",
)
def main_dag() -> None:
    """Main pipeline"""

    @task()
    def extract_from_source() -> int:
        """extract from s3

        Returns:
            int: 0 ok, 1 fail
        """
        module_name = "data_lake_pipelines.L1_raw_layer.extract_from_s3"
        imported_function = "get_data_raw_from_s3"
        try:
            root_logger.info("")
            root_logger.info(
                f"Now importing '{imported_function}' function from '{module_name}' module..."
            )
            get_data_raw_from_s3()
            root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def upload_into_dl(prev_status: int) -> int:
        """upload into data lake

        Args:
            prev_status (int): 0 ok, 1 fail
        """
        try:
            if prev_status == 0:
                module_name = "data_lake_pipelines.L1_raw_layer.upload_into_data_lake"
                imported_function = "upload_data_into_data_lake"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                upload_data_into_data_lake()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def get_data_from_dl(prev_status: int) -> int:
        """Eetrieve raw data from dl

        Args:
            prev_status (int): 0 ok, 1 fail

        Returns:
            int: 0 ok, 1 fail
        """
        try:
            if prev_status == 0:
                module_name = "dwh_pipelines.L1_raw_layer.extract_from_data_lake"
                imported_function = "get_data_raw_from_dl"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                get_data_raw_from_dl()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def dwh_l1_process_and_upload(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "dwh_pipelines.L1_raw_layer.process_and_upload"
                imported_function = "main"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                main(
                    dir_path="/opt/airflow/dags/data_lake_pipelines/L1_raw_layer/data_parquet"
                )
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def dwh_l2_perform_stg_events_tbl(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "dwh_pipelines.L2_staging_layer.dev.stg_events_tbl"
                imported_function = "main_dwh_l2_staging"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                main_dwh_l2_staging()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def dwh_l2_perform_create_stg_prod_env(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "dwh_pipelines.L2_staging_layer.prod.create_stg_prod_env"
                imported_function = "main_dwh_l2_prod"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                main_dwh_l2_prod()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def dwh_l3_perform_create_dim_events_tbl(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "dwh_pipelines.L3_semantic_layer.dev.dim_events_tbl"
                imported_function = "main_dwh_l3_dev"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                main_dwh_l3_dev()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def dwh_l3_perform_create_dim_prod_env(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "dwh_pipelines.L3_semantic_layer.prod.create_dim_prod_env"
                imported_function = "main_dwh_l3_prod"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                main_dwh_l3_prod()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def dwh_l4_perform_fact_events_tbl(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "dwh_pipelines.L4_dwh_layer.datamarts.fact_events_tbl"
                imported_function = "perform_fact_events_tbl"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                perform_fact_events_tbl()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def perform_no_updates_per_client(prev_status: int) -> int:
        try:
            if prev_status == 0:
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
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def perform_view_latest_updates_on_top1_client(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "dwh_pipelines.L4_dwh_layer.user_access_layer.latest_updates_on_top1_client"
                imported_function = "perform_create_view_latest_updates_on_top1_client"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                perform_create_view_latest_updates_on_top1_client()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def perform_reporting(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "dwh_pipelines.L4_dwh_layer.reporting.report_generator"
                imported_function = "create_plots"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                create_plots()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def performance_tunning_dwh_create_indexes(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "performance_tunning.dwh_indexes"
                imported_function = "dwh_create_indexes"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                dwh_create_indexes()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    @task()
    def perform_data_team_rbca(prev_status: int) -> int:
        try:
            if prev_status == 0:
                module_name = "governance.data_team_rbac"
                imported_function = "perform_dwh_access_control"
                root_logger.info("")
                root_logger.info(
                    f"Now importing '{imported_function}' function from '{module_name}' module..."
                )
                perform_dwh_access_control()
                root_logger.info("")
            return 0
        except Exception as err:
            root_logger.error(err)
            return 1

    status = extract_from_source()
    status = upload_into_dl(status)
    status = get_data_from_dl(status)
    # status = 0
    status = dwh_l1_process_and_upload(status)
    status = dwh_l2_perform_stg_events_tbl(status)
    status = dwh_l2_perform_create_stg_prod_env(status)
    status = dwh_l3_perform_create_dim_events_tbl(status)
    status = dwh_l3_perform_create_dim_prod_env(status)
    status = dwh_l4_perform_fact_events_tbl(status)
    status = performance_tunning_dwh_create_indexes(status)
    status = perform_no_updates_per_client(status)
    status = perform_view_latest_updates_on_top1_client(status)
    status = perform_reporting(status)
    status = perform_data_team_rbca(status)


result = main_dag()
