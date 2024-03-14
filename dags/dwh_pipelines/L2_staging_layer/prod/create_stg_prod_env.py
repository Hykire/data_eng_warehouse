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
    db_type="STAGING_DB",
    foreign_server="raw_db_server",
    src_db_name="raw_db",
    src_schema_name="main",
    active_schema_name="dev",
    src_table_name="raw_events_tbl",
    table_name="stg_events_tbl",
    data_warehouse_layer="STAGING",
)
# Set up root root_logger
root_logger = logging.getLogger(__name__)


def __create_infra() -> None:
    """create dwh l2 prod infra"""
    root_logger.info("Beginning create_infra process...")
    try:
        create_schema = f""" CREATE SCHEMA IF NOT EXISTS {prod_schema_name}; """
        check_if_schema_exists = f"""
            SELECT schema_name
            from information_schema.schemata
            WHERE schema_name= '{prod_schema_name}';
        """
        postgres_connection, cursor = get_cursor(db_type, data_warehouse_layer)

        # Create schema in Postgres
        cursor.execute(create_schema)
        root_logger.info("")
        root_logger.info(f"Successfully created '{prod_schema_name}' schema. ")
        root_logger.info("")

        cursor.execute(check_if_schema_exists)

        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(
                f"================================================================================================="
            )
            root_logger.info(
                f"SCHEMA CREATION SUCCESS: Managed to create {prod_schema_name} schema in {active_db_name} "
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
        # get all tables from dev
        root_logger.debug(f"")
        root_logger.debug(f"Now creating '{prod_schema_name}' environment ....")
        root_logger.debug(f"")
        sql_query = f""" SELECT table_name FROM information_schema.tables WHERE table_schema = '{dev_schema_name}' AND table_name LIKE '%stg%'
        """
        cursor.execute(sql_query)

        sql_results = cursor.fetchall()
        no_of_sql_results = len(sql_results)
        root_logger.debug(f"No of results: {no_of_sql_results} ")

        for table in sql_results:
            table_name = table[0]
            root_logger.info(f"")
            root_logger.info(
                f"Now creating '{table_name}' table in production environment ..."
            )

            sql_query = f"""  CREATE TABLE IF NOT EXISTS {prod_schema_name}.{table_name} as SELECT * FROM {dev_schema_name}.{table_name}
            """
            cursor.execute(sql_query)

            root_logger.info(
                f"Successfully created '{table_name}' table in production environment "
            )
            root_logger.info(f"")

        # postgres_connection.commit()
        root_logger.debug(f"")
        root_logger.debug(f"Successfully created '{prod_schema_name}' environment. ")
        root_logger.debug(f"")

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
    root_logger.info("Ending create_infra process...")


def main_dwh_l2_prod() -> None:
    """main process to create dwh l2 prod environment"""
    root_logger.info("Beginning main_dwh_l2_prod process...")
    __create_infra()
    root_logger.info("Ending main_dwh_l2_prod process...")
