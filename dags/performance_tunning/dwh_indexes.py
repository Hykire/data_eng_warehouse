import logging
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
    foreign_server="",
    src_db_name="",
    src_schema_name="",
    active_schema_name="live",
    src_table_name="",
    table_name="",
    data_warehouse_layer="DWH - UAL",
)
# Set up root root_logger
root_logger = logging.getLogger(__name__)


def dwh_create_indexes() -> None:
    """create reporting view"""
    root_logger.info("Beginning dwh_create_indexes process...")
    try:
        postgres_connection, cursor = get_cursor(db_type, data_warehouse_layer)
        # Create indexes
        tbl_name = "fact_events_tbl"
        column_name = "data_id"
        idx_name = f"idx_{active_schema_name}_{tbl_name}_{column_name}"
        create_index = f""" CREATE INDEX if not exists {idx_name} ON {active_schema_name}.{tbl_name}({column_name}); """
        cursor.execute(create_index)
        root_logger.info("")
        root_logger.info(f"Successfully created {idx_name} index. ")
        root_logger.info("")
        ######
        column_name = "event"
        idx_name = f"idx_{active_schema_name}_{tbl_name}_{column_name}"
        create_index = f""" CREATE INDEX if not exists {idx_name} ON {active_schema_name}.{tbl_name}({column_name}); """
        cursor.execute(create_index)
        root_logger.info("")
        root_logger.info(f"Successfully created {idx_name} index. ")
        root_logger.info("")
        ####
        column_name = "on_target_entity"
        idx_name = f"idx_{active_schema_name}_{tbl_name}_{column_name}"
        create_index = f""" CREATE INDEX if not exists {idx_name} ON {active_schema_name}.{tbl_name}({column_name}); """
        cursor.execute(create_index)
        root_logger.info("")
        root_logger.info(f"Successfully created {idx_name} index. ")
        root_logger.info("")
        ####
        column_name = "t"
        idx_name = f"idx_{active_schema_name}_{tbl_name}_{column_name}"
        create_index = f""" CREATE INDEX if not exists {idx_name} ON {active_schema_name}.{tbl_name}({column_name}); """
        cursor.execute(create_index)
        root_logger.info("")
        root_logger.info(f"Successfully created {idx_name} index. ")
        root_logger.info("")
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
    root_logger.info("Ending dwh_create_indexes process...")


def perform_dwh_create_indexes() -> None:
    """create indexes"""
    dwh_create_indexes()
