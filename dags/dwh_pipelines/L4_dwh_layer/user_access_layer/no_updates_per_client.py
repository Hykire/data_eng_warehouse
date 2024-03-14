import logging
import time
from typing import Any, List, Tuple

from utils import get_constans, get_cursor

(
    db_type,
    host,
    port,
    database,
    username,
    password,
    CURRENT_TIMESTAMP,
    source_system,
    row_counter,
    fdw_extension,
    foreign_server,
    fdw_user,
    src_db_name,
    src_schema_name,
    active_schema_name,
    active_db_name,
    src_table_name,
    table_name,
    data_warehouse_layer,
    dev_schema_name,
    prod_schema_name,
) = get_constans(
    db_type="dwh_db",
    foreign_server="dim_db_server",
    src_db_name="semantic_db",
    src_schema_name="prod",
    active_schema_name="reporting",
    src_table_name="dim_events_tbl",
    table_name="no_updates_per_client",
    data_warehouse_layer="DWH - UAL",
)
# Set up root root_logger
root_logger = logging.getLogger(__name__)


def create_view_no_updates_per_client() -> None:
    """create reporting view"""
    root_logger.info("Beginning create_view_no_updates_per_client process...")
    try:
        postgres_connection, cursor = get_cursor(db_type, data_warehouse_layer)
        # Set up SQL statements for schema creation and validation check
        create_schema = f""" CREATE SCHEMA IF NOT EXISTS {active_schema_name}; """

        check_if_schema_exists = f""" SELECT schema_name
                                        from information_schema.schemata WHERE schema_name= '{active_schema_name}'
                                        ;
        """

        # Create schema in Postgres
        CREATING_SCHEMA_PROCESSING_START_TIME = time.time()
        cursor.execute(create_schema)
        root_logger.info("")
        root_logger.info(f"Successfully created {active_schema_name} schema. ")
        root_logger.info("")
        CREATING_SCHEMA_PROCESSING_END_TIME = time.time()

        CREATING_SCHEMA_VAL_CHECK_START_TIME = time.time()
        cursor.execute(check_if_schema_exists)
        CREATING_SCHEMA_VAL_CHECK_END_TIME = time.time()

        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(
                f"================================================================================================="
            )
            root_logger.info(
                f"SCHEMA CREATION SUCCESS: Managed to create {active_schema_name} schema in {active_db_name} "
            )
            root_logger.info(f"Schema name in Postgres: {sql_result} ")
            root_logger.info(
                f"SQL Query for validation check:  {check_if_schema_exists} "
            )
            root_logger.info(
                f"================================================================================================="
            )
            root_logger.debug(f"")

        else:
            root_logger.debug(f"")
            root_logger.error(
                f"================================================================================================="
            )
            root_logger.error(
                f"SCHEMA CREATION FAILURE: Unable to create schema for {active_db_name}..."
            )
            root_logger.info(
                f"SQL Query for validation check:  {check_if_schema_exists} "
            )
            root_logger.error(
                f"================================================================================================="
            )
            root_logger.debug(f"")

        create_view_no_updates_per_client = """
            create OR REPLACE view reporting.no_updates_per_client as
            select event, on_target_entity, data_id, min(t) min_t, max(t) max_t, count(*) n_events, max(updated_at) latest_update
            from live.fact_events_tbl
            group by 1,2,3
            order by count(*) desc
            ;
        """
        cursor.execute(create_view_no_updates_per_client)

        root_logger.info(
            f"============================================================================================================================================================================="
        )
        root_logger.info(
            f"view CREATION SUCCESS: Managed to create {table_name} view in {active_db_name}.  "
        )
        root_logger.info(
            f"============================================================================================================================================================================="
        )

    except Exception as e:
        root_logger.error(e)
    finally:
        # Close the cursor if it exists
        if cursor is not None:
            cursor.close()
            root_logger.debug("")
            root_logger.debug("Cursor closed successfully.")

        # Close the database connection to Postgres if it exists
        if postgres_connection is not None:
            postgres_connection.close()
            # root_logger.debug("")
            root_logger.debug("Session connected to Postgres database closed.")
    root_logger.info("Ending create_view_no_updates_per_client process...")


def perform_create_view_no_updates_per_client() -> None:
    """create reporting view"""
    create_view_no_updates_per_client()
