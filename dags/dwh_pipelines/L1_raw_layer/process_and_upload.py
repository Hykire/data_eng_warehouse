import configparser
import logging
import os
import random
import time
from datetime import datetime
from typing import Any, List, Tuple

import pandas as pd
import psycopg2
from psycopg2 import extras

# Constants
path = os.path.abspath("/opt/airflow/dags/db_credentials.ini")
config = configparser.ConfigParser()
config.read(path)
host = config["db_credentials"]["HOST"]
port = config["db_credentials"]["PORT"]
database = config["db_credentials"]["RAW_DB"]
username = config["db_credentials"]["USERNAME"]
password = config["db_credentials"]["PASSWORD"]
CURRENT_TIMESTAMP = datetime.now()
db_layer_name = database
schema_name = "main"
table_name = "raw_events_tbl"
data_warehouse_layer = "RAW"
source_system = ["s3://confidential"]
row_counter = 0

# Metrics constants
CREATING_SCHEMA_PROCESSING_START_TIME = -1
CREATING_SCHEMA_PROCESSING_END_TIME = -1
CREATING_SCHEMA_VAL_CHECK_START_TIME = -1
CREATING_SCHEMA_VAL_CHECK_END_TIME = -1
CREATING_TABLE_PROCESSING_START_TIME = -1
CREATING_TABLE_PROCESSING_END_TIME = -1
CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME = -1
CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME = -1
ADDING_DATA_LINEAGE_PROCESSING_START_TIME = -1
ADDING_DATA_LINEAGE_PROCESSING_END_TIME = -1
ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME = -1
ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME = -1
ROW_INSERTION_PROCESSING_START_TIME = -1
ROW_INSERTION_PROCESSING_END_TIME = -1
ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME = -1
ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME = -1
get_list_of_column_names = f""" SELECT column_name
                                FROM information_schema.columns
                                WHERE   table_name = '{table_name}'
                                ORDER BY ordinal_position
                                ;
"""
# Set up root root_logger
root_logger = logging.getLogger(__name__)


def __execute_dwh_sql(cursor: Any) -> None:
    """LOAD DL INTO DWH

    Args:
        cursor (Any): PostgreSQL's cursor
    """
    # Set up SQL statements for schema creation and validation check
    create_schema = f"""CREATE SCHEMA IF NOT EXISTS {schema_name};"""
    check_if_schema_exists = f"""SELECT schema_name from information_schema.schemata WHERE schema_name= '{schema_name}';"""
    # Set up SQL statements for table creation and validation check
    create_raw_events_tbl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                                    event                   varchar,
                                    on_target_entity        varchar,
                                    t                       timestamptz,
                                    organization_id         varchar,
                                    data_id                 varchar,
                                    data_location_at        timestamptz,
                                    data_location_lat       numeric,
                                    data_location_lng       numeric,
                                    data_finish             timestamptz,
                                    data_start              timestamptz
                                );
    """
    check_if_raw_events_tbl_exists = f""" SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
    """
    # Set up SQL statements for adding data lineage and validation check
    add_data_lineage_to_raw_events_tbl = f"""ALTER TABLE {schema_name}.{table_name}
                                                ADD COLUMN if not exists  created_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                ADD COLUMN if not exists  updated_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                ADD COLUMN if not exists  source_system               VARCHAR(255),
                                                ADD COLUMN if not exists  source_file                 VARCHAR(255),
                                                ADD COLUMN if not exists  load_timestamp              TIMESTAMP,
                                                ADD COLUMN if not exists  dwh_layer                   VARCHAR(255)
                                                ;
    """
    check_if_data_lineage_fields_are_added_to_tbl = f"""
                                                    SELECT *
                                                    FROM information_schema.columns
                                                    WHERE table_name      = '{table_name}'
                                                        AND     (column_name    = 'created_at'
                                                        OR      column_name     = 'updated_at'
                                                        OR      column_name     = 'source_system'
                                                        OR      column_name     = 'source_file'
                                                        OR      column_name     = 'load_timestamp'
                                                        OR      column_name     = 'dwh_layer')
                                                    ;
    """
    # Create schema in Postgres
    CREATING_SCHEMA_PROCESSING_START_TIME = time.time()
    cursor.execute(create_schema)
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
            f"SCHEMA CREATION SUCCESS: Managed to create {schema_name} schema in {db_layer_name} "
        )
        root_logger.info(f"Schema name in Postgres: {sql_result} ")
        root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
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
            f"SCHEMA CREATION FAILURE: Unable to create schema for {db_layer_name}..."
        )
        root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
        root_logger.error(
            f"================================================================================================="
        )
        root_logger.debug(f"")

    # Create table if it doesn't exist in Postgres
    CREATING_TABLE_PROCESSING_START_TIME = time.time()
    root_logger.info("entering table creation")
    cursor.execute(create_raw_events_tbl)
    CREATING_TABLE_PROCESSING_END_TIME = time.time()

    CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME = time.time()
    root_logger.info("entering table check")
    cursor.execute(check_if_raw_events_tbl_exists)
    CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME = time.time()

    sql_result = cursor.fetchone()[0]
    if sql_result:
        root_logger.debug(f"")
        root_logger.info(
            f"============================================================================================================================================================================="
        )
        root_logger.info(
            f"TABLE CREATION SUCCESS: Managed to create {table_name} table in {db_layer_name}.  "
        )
        root_logger.info(
            f"SQL Query for validation check:  {check_if_raw_events_tbl_exists} "
        )
        root_logger.info(
            f"============================================================================================================================================================================="
        )
        root_logger.debug(f"")
    else:
        root_logger.debug(f"")
        root_logger.error(
            f"=========================================================================================================================================================================="
        )
        root_logger.error(f"TABLE CREATION FAILURE: Unable to create {table_name}... ")
        root_logger.error(
            f"SQL Query for validation check:  {check_if_raw_events_tbl_exists} "
        )
        root_logger.error(
            f"=========================================================================================================================================================================="
        )
        root_logger.debug(f"")

    # Add data lineage to table
    ADDING_DATA_LINEAGE_PROCESSING_START_TIME = time.time()
    cursor.execute(add_data_lineage_to_raw_events_tbl)
    ADDING_DATA_LINEAGE_PROCESSING_END_TIME = time.time()

    ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME = time.time()
    cursor.execute(check_if_data_lineage_fields_are_added_to_tbl)
    ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME = time.time()
    sql_results = cursor.fetchall()

    if len(sql_results) == 6:
        root_logger.debug(f"")
        root_logger.info(
            f"============================================================================================================================================================================="
        )
        root_logger.info(
            f"DATA LINEAGE FIELDS CREATION SUCCESS: Managed to create data lineage columns in {schema_name}.{table_name}.  "
        )
        root_logger.info(
            f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} "
        )
        root_logger.info(
            f"============================================================================================================================================================================="
        )
        root_logger.debug(f"")
    else:
        root_logger.debug(f"")
        root_logger.error(
            f"=========================================================================================================================================================================="
        )
        root_logger.error(
            f"DATA LINEAGE FIELDS ALREADY CREATED: columns in {schema_name}.{table_name}.... "
        )
        root_logger.error(
            f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} "
        )
        root_logger.error(
            f"=========================================================================================================================================================================="
        )
        root_logger.debug(f"")

    # ======================================= SENSITIVE COLUMN IDENTIFICATION =======================================

    note_1 = """ IMPORTANT NOTE: Invest time in understanding the underlying data fields to avoid highlighting the incorrect fields or omitting fields containing confidential information."""
    note_2 = """ Involving the relevant stakeholders in the process of identifying sensitive data fields from the source data is a crucial step to protecting confidential information."""
    note_3 = """ Neglecting this step could expose customers and the wider company to serious harm (e.g. cybersecurity hacks, data breaches, unauthorized access to sensitive data), so approach this task with the utmost care."""

    root_logger.warning(f"")
    root_logger.warning(f"")
    root_logger.warning("================================================")
    root_logger.warning("           SENSITIVE COLUMN IDENTIFICATION              ")
    root_logger.warning("================================================")
    root_logger.warning(f"")
    root_logger.error(f"{note_1}")
    root_logger.error(f"")
    root_logger.error(f"{note_2}")
    root_logger.error(f"")
    root_logger.error(f"{note_3}")
    root_logger.warning(f"")
    root_logger.warning(f"")
    root_logger.warning(f"Now beginning the sensitive column identification stage ...")
    root_logger.warning(f"")

    # Add a flag for confirming if sensitive data fields have been highlighted
    sensitive_columns_selected = [
        "organization_id",
        "data_id",
        "data_location_lat",
        "data_location_lng",
    ]

    if len(sensitive_columns_selected) == 0:
        SENSITIVE_COLUMNS_IDENTIFIED = False
        root_logger.error(
            f"ERROR: No sensitive columns have been selected for '{table_name}' table "
        )
        root_logger.warning(f"")

    elif sensitive_columns_selected[0] is None:
        SENSITIVE_COLUMNS_IDENTIFIED = True
        root_logger.error(
            f"There are no sensitive columns for the '{table_name}' table "
        )
        root_logger.warning(f"")

    else:
        SENSITIVE_COLUMNS_IDENTIFIED = True
        root_logger.warning(
            f"Here are the columns considered sensitive in this table ..."
        )
        root_logger.warning(f"")

    if SENSITIVE_COLUMNS_IDENTIFIED is False:
        sql_statement_for_listing_columns_in_table = f"""
        SELECT column_name FROM information_schema.columns
        WHERE   table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        cursor.execute(get_list_of_column_names)
        list_of_column_names = cursor.fetchall()
        column_names = [sql_result[0] for sql_result in list_of_column_names]

        root_logger.warning(
            f"You are required to select the sensitive columns in this table. If there are none, enter 'None' in the 'sensitive_columns_selected' object."
        )
        root_logger.warning(f"")
        root_logger.warning(f"Here are the columns to choose from:")
        root_logger.warning(f"")
        total_sensitive_columns = 0
        for sensitive_column_name in column_names:
            total_sensitive_columns += 1
            root_logger.warning(
                f"""{total_sensitive_columns} : '{sensitive_column_name}'  """
            )
        root_logger.warning(f"")
        root_logger.warning(
            f"You can use this SQL query to list the columns in this table:"
        )
        root_logger.warning(
            f"              {sql_statement_for_listing_columns_in_table}                "
        )

    else:
        total_sensitive_columns = 0
        for sensitive_column_name in sensitive_columns_selected:
            total_sensitive_columns += 1
            root_logger.warning(
                f"""{total_sensitive_columns} : '{sensitive_column_name}'  """
            )
        if sensitive_columns_selected[0] is not None:
            root_logger.warning(f"")
            root_logger.warning(f"")
            root_logger.warning(
                f"Decide on the appropriate treatment for these tables. A few options to consider include:"
            )
            root_logger.warning(
                f"""1. Masking fields               -   This involves replacing sensitive columns with alternative characters e.g.  'xxxx-xxxx', '*****', '$$$$'. """
            )
            root_logger.warning(
                f"""2. Encrypting fields            -   This is converting sensitive columns to cipher text (unreadable text format).        """
            )
            root_logger.warning(
                f"""3. Role-based access control    -   Placing a system that delegates privileges based on team members' responsibilities        """
            )

        root_logger.warning(f"")
        root_logger.warning(
            f"Now terminating the sensitive column identification stage ..."
        )
        root_logger.warning(f"Sensitive column identification stage ended. ")
        root_logger.warning(f"")

    root_logger.warning(f"")
    root_logger.warning(f"")

    # adding unique index to avoid duplicates
    create_schema = f"""create unique index if not exists unique_idx_{schema_name}_{table_name} on {schema_name}.{table_name}(event,
    on_target_entity, t, organization_id, data_id, source_file)
    ;"""
    cursor.execute(create_schema)


def __get_cursor() -> Tuple[Any, Any]:
    """Get cursor from psycopg2 obj

    Raises:
        ConnectionError: error while connecting to postgresql
        Exception: general excepcion

    Returns:
        Tuple[Any, Any]: returns postgres connection and its cursor
    """
    try:
        postgres_connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=username,
            password=password,
        )
        postgres_connection.set_session(autocommit=True)
        cursor = postgres_connection.cursor()
        # Validate the Postgres database connection
        if postgres_connection.closed == 0:
            root_logger.debug(f"")
            root_logger.info(
                "================================================================================="
            )
            root_logger.info(
                f"CONNECTION SUCCESS: Managed to connect successfully to the {db_layer_name} database!!"
            )
            root_logger.info(f"Connection details: {postgres_connection.dsn} ")
            root_logger.info(
                "================================================================================="
            )
            root_logger.debug("")

        elif postgres_connection.closed != 0:
            raise ConnectionError(
                "CONNECTION ERROR: Unable to connect to the demo_company database..."
            )
    except Exception as e:
        root_logger.info(e)
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
        raise Exception(
            "CONNECTION ERROR: Unable to connect to the demo_company database..."
        )
    return postgres_connection, cursor


def create_dwh_infra() -> None:
    """create db infra (tables, schema, etc)"""
    # Begin the table creation process
    root_logger.info("")
    root_logger.info("---------------------------------------------")
    root_logger.info("Beginning create_dwh_infra process...")
    try:
        postgres_connection, cursor = __get_cursor()
        __execute_dwh_sql(cursor)
    except Exception as e:
        root_logger.info(e)
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
    root_logger.info("Ending create_dwh_infra process...")


def format_data_to_upload(
    dir_path: str,
) -> List[
    Tuple[
        str,
        str,
        Any,
        str,
        str,
        Any,
        float,
        float,
        Any,
        Any,
        str,
        str,
        Any,
        str,
        Any,
        Any,
    ]
]:
    """format data from parquet to list of tuples

    Args:
        dir_path (str): dir of parquet files

    Returns:
        List[
        Tuple[str,
            str,
            Any,
            str,
            str,
            Any,
            float,
            float,
            Any,
            Any,
            str,
            str,
            Any,
            str,
            Any,
            Any]
    ]: return list of tuples rdy to push into dwh
    """
    root_logger.info("Beginning format_data_to_upload process...")
    cum_df = pd.DataFrame()
    for root, _, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            temp_df = pd.read_parquet(file_path, engine="fastparquet")
            temp_df["source_file"] = file
            cum_df = pd.concat([cum_df, temp_df], ignore_index=True)

    lst_values: List[
        Tuple[
            str,
            str,
            Any,
            str,
            str,
            Any,
            float,
            float,
            Any,
            Any,
            str,
            str,
            Any,
            str,
            Any,
            Any,
        ]
    ] = []
    cum_df.columns = [
        "event",
        "on_target_entity",
        "t",
        "organization_id",
        "data_id",
        "data_location_at",
        "data_location_lat",
        "data_location_lng",
        "source_file",
        "data_finish",  # operating period
        "data_start",  # operating period
    ]
    cum_df.loc[cum_df["data_finish"].isnull(), "data_finish"] = None
    cum_df.loc[cum_df["data_start"].isnull(), "data_start"] = None
    cum_df.loc[cum_df["data_location_at"].isnull(), "data_location_at"] = None
    for _, row in cum_df.iterrows():
        values = (
            str(row["event"]),
            str(row["on_target_entity"]),
            row["t"],
            str(row["organization_id"]),
            str(row["data_id"]),
            row["data_location_at"],
            float(str(row["data_location_lat"])),
            float(str(row["data_location_lng"])),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            random.choice(source_system),
            row["source_file"],
            CURRENT_TIMESTAMP,
            data_warehouse_layer,
            row["data_finish"],
            row["data_start"],
        )
        lst_values.append(values)
    root_logger.info("Ending format_data_to_upload process...")
    return lst_values


def upload_data_to_dwh(
    lst_tuples: List[
        Tuple[
            str,
            str,
            Any,
            str,
            str,
            Any,
            float,
            float,
            Any,
            Any,
            str,
            str,
            Any,
            str,
            Any,
            Any,
        ]
    ]
) -> Tuple[int, int, int]:
    """upload data into dwh

    Args:
        lst_tuples (List[Tuple[str, str, str, str, str, str, float, float, str, str, str, Any, str, str, Any, Any]]): parquet processed data

    Returns:
        Tuple[int, int ,int]: original_rows, rows_inserted, total_rows_in_table
    """
    root_logger.info("Beginning upload_data_to_dwh process...")
    total_rows_in_table = 0
    row_counter = 0
    ROW_INSERTION_PROCESSING_START_TIME = time.time()
    try:
        postgres_connection, cursor = __get_cursor()
        check_total_row_count_before_insert_statement = (
            f"""   SELECT COUNT(*) FROM {schema_name}.{table_name}"""
        )
        cursor.execute(check_total_row_count_before_insert_statement)
        sql_result = cursor.fetchone()[0]
        root_logger.info(f"Rows before SQL insert in Postgres: {sql_result} ")
        root_logger.debug(f"")
        # Set up SQL statements for records insert and validation check
        insert_events_data = """INSERT INTO main.raw_events_tbl (
                                event,
                                on_target_entity,
                                t,
                                organization_id,
                                data_id,
                                data_location_at,
                                data_location_lat,
                                data_location_lng,
                                created_at,
                                updated_at,
                                source_system,
                                source_file,
                                load_timestamp,
                                dwh_layer,
                                data_finish,
                                data_start
                            )
                            VALUES %s
                            ON CONFLICT (event, on_target_entity, t, organization_id, data_id, source_file)
                            do update
                            set
                            data_location_at = EXCLUDED.data_location_at,
                            data_location_lat = EXCLUDED.data_location_lat,
                            data_location_lng = EXCLUDED.data_location_lng,
                            updated_at = EXCLUDED.updated_at,
                            source_system = EXCLUDED.source_system,
                            load_timestamp = EXCLUDED.load_timestamp,
                            dwh_layer = EXCLUDED.dwh_layer,
                            data_finish = EXCLUDED.data_finish,
                            data_start = EXCLUDED.data_start
                            ;
        """
        check_total_row_count_after_insert_statement = (
            f""" SELECT COUNT(*) FROM {schema_name}.{table_name} """
        )
        # Use high performance insert instead of row by row
        extras.execute_values(cursor, insert_events_data, lst_tuples, page_size=1000)
        # Validate insertion
        row_counter = len(lst_tuples)
        root_logger.debug(f"---------------------------------")
        root_logger.info(f"INSERT SUCCESS: Uploaded events record no {row_counter} ")
        root_logger.debug(f"---------------------------------")
        ROW_INSERTION_PROCESSING_END_TIME = time.time()
        ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME = time.time()
        cursor.execute(check_total_row_count_after_insert_statement)
        ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME = time.time()
        total_rows_in_table = cursor.fetchone()[0]
        root_logger.info(f"Rows after SQL insert in Postgres: {total_rows_in_table} ")
        cursor.execute(check_total_row_count_after_insert_statement)
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
    root_logger.info("Ending upload_data_to_dwh process...")
    return len(lst_tuples), int(row_counter), total_rows_in_table


def show_data_metrics(
    original_rows: int, rows_inserted: int, total_rows_in_table: int
) -> None:
    """DATA PROFILING METRICS

    Args:
        original_rows (int): original rows to push
        rows_inserted (int): no of inserted rows
        total_rows_in_table (int): count(*)
    """
    root_logger.info("Beginning show_data_metrics process...")
    try:
        postgres_connection, cursor = __get_cursor()
        # Prepare data profiling metrics
        # --------- A. Table statistics
        count_total_no_of_columns_in_table = f"""   SELECT COUNT(column_name)
                                                    FROM            information_schema.columns
                                                    WHERE           table_name      =   '{table_name}'
                                                    AND             table_schema    =   '{schema_name}'
                                                    ;
        """
        cursor.execute(count_total_no_of_columns_in_table)
        total_columns_in_table = cursor.fetchone()[0]

        count_total_no_of_unique_records_in_table = f""" SELECT COUNT(*)
                                                            FROM (SELECT DISTINCT * FROM {schema_name}.{table_name}) as unique_records
        """
        cursor.execute(count_total_no_of_unique_records_in_table)
        total_unique_records_in_table = cursor.fetchone()[0]
        total_duplicate_records_in_table = (
            total_rows_in_table - total_unique_records_in_table
        )

        cursor.execute(get_list_of_column_names)
        list_of_column_names = cursor.fetchall()
        column_names = [sql_result[0] for sql_result in list_of_column_names]

        # --------- B. Performance statistics (Python)
        EXECUTION_TIME_FOR_CREATING_SCHEMA = (
            CREATING_SCHEMA_PROCESSING_END_TIME - CREATING_SCHEMA_PROCESSING_START_TIME
        ) * 1000

        EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK = (
            CREATING_SCHEMA_VAL_CHECK_END_TIME - CREATING_SCHEMA_VAL_CHECK_START_TIME
        ) * 1000

        EXECUTION_TIME_FOR_CREATING_TABLE = (
            CREATING_TABLE_PROCESSING_END_TIME - CREATING_TABLE_PROCESSING_START_TIME
        ) * 1000

        EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK = (
            CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME
            - CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME
        ) * 1000

        EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE = (
            ADDING_DATA_LINEAGE_PROCESSING_END_TIME
            - ADDING_DATA_LINEAGE_PROCESSING_START_TIME
        ) * 1000

        EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK = (
            ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME
            - ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME
        ) * 1000

        EXECUTION_TIME_FOR_ROW_INSERTION = (
            ROW_INSERTION_PROCESSING_END_TIME - ROW_INSERTION_PROCESSING_START_TIME
        ) * 1000

        EXECUTION_TIME_FOR_ROW_COUNT = (
            ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME
            - ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME
        ) * 1000

        # Display data profiling metrics

        root_logger.info(f"")
        root_logger.info(f"")
        root_logger.info("================================================")
        root_logger.info("              DATA PROFILING METRICS              ")
        root_logger.info("================================================")
        root_logger.info(f"")
        root_logger.info(f"Now calculating table statistics...")
        root_logger.info(f"")
        root_logger.info(f"")
        root_logger.info(f"Table name:                                  {table_name} ")
        root_logger.info(f"Schema name:                                 {schema_name} ")
        root_logger.info(f"Database name:                               {database} ")
        root_logger.info(
            f"Data warehouse layer:                        {data_warehouse_layer} "
        )
        root_logger.info(f"")
        root_logger.info(f"")
        root_logger.info(
            f"Number of rows in table:                     {total_rows_in_table} "
        )
        root_logger.info(
            f"Number of columns in table:                  {total_columns_in_table} "
        )
        root_logger.info(f"")

        if rows_inserted == original_rows:
            root_logger.info(
                f"Successful records uploaded total :          {rows_inserted} / {original_rows}   "
            )

            root_logger.info(f"")
            root_logger.info(
                f"Successful records uploaded % :              {(rows_inserted / original_rows) * 100}    "
            )

            root_logger.info(f"")
        else:
            root_logger.warning(
                f"Successful records uploaded total :          {rows_inserted} / {original_rows}   "
            )

            root_logger.warning(f"")
            root_logger.warning(
                f"Successful records uploaded % :              {(rows_inserted / original_rows) * 100}    "
            )

            root_logger.warning(f"")

        if total_unique_records_in_table == total_rows_in_table:
            root_logger.info(
                f"Number of unique records:                    {total_unique_records_in_table} / {total_rows_in_table}"
            )
            root_logger.info(
                f"Number of duplicate records:                 {total_duplicate_records_in_table} / {total_rows_in_table}"
            )
            root_logger.info(f"")
            root_logger.info(
                f"Unique records %:                            {(total_unique_records_in_table / total_rows_in_table) * 100} "
            )
            root_logger.info(
                f"Duplicate records %:                         {(total_duplicate_records_in_table / total_rows_in_table)  * 100} "
            )
            root_logger.info(f"")

        else:
            root_logger.warning(
                f"Number of unique records:                    {total_unique_records_in_table} / {total_rows_in_table}"
            )
            root_logger.warning(
                f"Number of duplicate records:                 {total_duplicate_records_in_table} / {total_rows_in_table}"
            )
            root_logger.warning(f"")
            root_logger.warning(
                f"Unique records %:                            {(total_unique_records_in_table / total_rows_in_table) * 100} "
            )
            root_logger.warning(
                f"Duplicate records %:                         {(total_duplicate_records_in_table / total_rows_in_table)  * 100} "
            )
            root_logger.warning(f"")

        column_index = 0
        total_null_values_in_table = 0

        for column_name in column_names:
            cursor.execute(
                f"""
                    SELECT COUNT(*)
                    FROM {schema_name}.{table_name}
                    WHERE {column_name} is NULL
            """
            )
            sql_result = cursor.fetchone()[0]
            total_null_values_in_table += sql_result
            column_index += 1
            if sql_result == 0:
                root_logger.info(
                    f"Column name: {column_name},  Column no: {column_index},  Number of NULL values: {sql_result} "
                )
            else:
                root_logger.warning(
                    f"Column name: {column_name},  Column no: {column_index},  Number of NULL values: {sql_result} "
                )

        root_logger.info(f"")
        root_logger.info("================================================")
        root_logger.info(f"")
        root_logger.info(
            f"Now calculating performance statistics (from a Python standpoint)..."
        )
        root_logger.info(f"")
        root_logger.info(f"")

        if (EXECUTION_TIME_FOR_CREATING_SCHEMA > 1000) and (
            EXECUTION_TIME_FOR_CREATING_SCHEMA < 60000
        ):
            root_logger.info(
                f"1. Execution time for CREATING schema: {EXECUTION_TIME_FOR_CREATING_SCHEMA} ms ({    round   (EXECUTION_TIME_FOR_CREATING_SCHEMA  /   1000, 2)   } secs) "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        elif EXECUTION_TIME_FOR_CREATING_SCHEMA >= 60000:
            root_logger.info(
                f"1. Execution time for CREATING schema: {EXECUTION_TIME_FOR_CREATING_SCHEMA} ms  ({    round   (EXECUTION_TIME_FOR_CREATING_SCHEMA  /   1000, 2)   } secs)  ({   round  ((EXECUTION_TIME_FOR_CREATING_SCHEMA  /   1000) / 60, 4)     } mins)   "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        else:
            root_logger.info(
                f"1. Execution time for CREATING schema: {EXECUTION_TIME_FOR_CREATING_SCHEMA} ms "
            )
            root_logger.info(f"")
            root_logger.info(f"")

        if (EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK > 1000) and (
            EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK < 60000
        ):
            root_logger.info(
                f"2. Execution time for CREATING schema (VAL CHECK): {EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK  /   1000, 2)} secs)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        elif EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK >= 60000:
            root_logger.info(
                f"2. Execution time for CREATING schema (VAL CHECK): {EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK  /   1000, 2)} secs)    ({  round ((EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK  /   1000) / 60,  4)   } min)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        else:
            root_logger.info(
                f"2. Execution time for CREATING schema (VAL CHECK): {EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK} ms "
            )
            root_logger.info(f"")
            root_logger.info(f"")

        if (EXECUTION_TIME_FOR_CREATING_TABLE > 1000) and (
            EXECUTION_TIME_FOR_CREATING_TABLE < 60000
        ):
            root_logger.info(
                f"5. Execution time for CREATING table:  {EXECUTION_TIME_FOR_CREATING_TABLE} ms ({  round   (EXECUTION_TIME_FOR_CREATING_TABLE  /   1000, 2)} secs)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        elif EXECUTION_TIME_FOR_CREATING_TABLE >= 60000:
            root_logger.info(
                f"5. Execution time for CREATING table:  {EXECUTION_TIME_FOR_CREATING_TABLE} ms ({  round   (EXECUTION_TIME_FOR_CREATING_TABLE  /   1000, 2)} secs)    ({  round ((EXECUTION_TIME_FOR_CREATING_TABLE  /   1000) / 60,  4)   } min)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        else:
            root_logger.info(
                f"5. Execution time for CREATING table:  {EXECUTION_TIME_FOR_CREATING_TABLE} ms "
            )
            root_logger.info(f"")
            root_logger.info(f"")

        if (EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK > 1000) and (
            EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK < 60000
        ):
            root_logger.info(
                f"6. Execution time for CREATING table (VAL CHECK):  {EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK  /   1000, 2)} secs)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        elif EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK >= 60000:
            root_logger.info(
                f"6. Execution time for CREATING table (VAL CHECK):  {EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK  /   1000, 2)} secs)  ({  round ((EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK  /   1000) / 60,  4)   } min)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        else:
            root_logger.info(
                f"6. Execution time for CREATING table (VAL CHECK):  {EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK} ms "
            )
            root_logger.info(f"")
            root_logger.info(f"")

        if (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE > 1000) and (
            EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE < 60000
        ):
            root_logger.info(
                f"7. Execution time for ADDING data lineage:  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE} ms ({  round   (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE  /   1000, 2)} secs)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        elif EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE >= 60000:
            root_logger.info(
                f"7. Execution time for ADDING data lineage:  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE} ms ({  round   (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE  /   1000, 2)} secs)  ({  round ((EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE  /   1000) / 60,  4)   } min)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        else:
            root_logger.info(
                f"7. Execution time for ADDING data lineage:  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE} ms "
            )
            root_logger.info(f"")
            root_logger.info(f"")

        if (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK > 1000) and (
            EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK < 60000
        ):
            root_logger.info(
                f"8. Execution time for ADDING data lineage (VAL CHECK):  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK  /   1000, 2)} secs)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        elif EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK >= 60000:
            root_logger.info(
                f"8. Execution time for ADDING data lineage (VAL CHECK):  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK  /   1000, 2)} secs)   ({  round ((EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK  /   1000) / 60,  4)   } min)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        else:
            root_logger.info(
                f"8. Execution time for ADDING data lineage (VAL CHECK):  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK} ms "
            )
            root_logger.info(f"")
            root_logger.info(f"")

        if (EXECUTION_TIME_FOR_ROW_INSERTION > 1000) and (
            EXECUTION_TIME_FOR_ROW_INSERTION < 60000
        ):
            root_logger.info(
                f"9. Execution time for INSERTING rows to table:  {EXECUTION_TIME_FOR_ROW_INSERTION} ms ({  round   (EXECUTION_TIME_FOR_ROW_INSERTION  /   1000, 2)} secs)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        elif EXECUTION_TIME_FOR_ROW_INSERTION >= 60000:
            root_logger.info(
                f"9. Execution time for INSERTING rows to table:  {EXECUTION_TIME_FOR_ROW_INSERTION} ms ({  round   (EXECUTION_TIME_FOR_ROW_INSERTION  /   1000, 2)} secs)   ({  round ((EXECUTION_TIME_FOR_ROW_INSERTION  /   1000) / 60,  4)   } min)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        else:
            root_logger.info(
                f"9. Execution time for INSERTING rows to table:  {EXECUTION_TIME_FOR_ROW_INSERTION} ms "
            )
            root_logger.info(f"")
            root_logger.info(f"")

        if (EXECUTION_TIME_FOR_ROW_COUNT > 1000) and (
            EXECUTION_TIME_FOR_ROW_COUNT < 60000
        ):
            root_logger.info(
                f"10. Execution time for COUNTING uploaded rows to table:  {EXECUTION_TIME_FOR_ROW_COUNT} ms ({  round   (EXECUTION_TIME_FOR_ROW_COUNT  /   1000, 2)} secs)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        elif EXECUTION_TIME_FOR_ROW_COUNT >= 60000:
            root_logger.info(
                f"10. Execution time for COUNTING uploaded rows to table:  {EXECUTION_TIME_FOR_ROW_COUNT} ms ({  round   (EXECUTION_TIME_FOR_ROW_COUNT  /   1000, 2)} secs)    ({  round ((EXECUTION_TIME_FOR_ROW_COUNT  /   1000) / 60,  4)   } min)      "
            )
            root_logger.info(f"")
            root_logger.info(f"")
        else:
            root_logger.info(
                f"10. Execution time for COUNTING uploaded rows to table:  {EXECUTION_TIME_FOR_ROW_COUNT} ms "
            )
            root_logger.info(f"")
            root_logger.info(f"")

        root_logger.info(f"")
        root_logger.info("================================================")

        # Add conditional statements for data profile metrics

        if rows_inserted != original_rows:
            if rows_inserted == 0:
                root_logger.error(
                    f"ERROR: No records were upload to '{table_name}' table...."
                )
                raise ImportError(
                    "Trace filepath to highlight the root cause of the missing rows..."
                )
            else:
                root_logger.error(
                    f"ERROR: There are only {rows_inserted} records upload to '{table_name}' table...."
                )
                raise ImportError(
                    "Trace filepath to highlight the root cause of the missing rows..."
                )

        elif total_unique_records_in_table != total_rows_in_table:
            root_logger.error(
                f"ERROR: There are {total_duplicate_records_in_table} duplicated records in the uploads for '{table_name}' table...."
            )
            raise ImportError(
                "Trace filepath to highlight the root cause of the duplicated rows..."
            )

        elif total_duplicate_records_in_table > 0:
            root_logger.error(
                f"ERROR: There are {total_duplicate_records_in_table} duplicated records in the uploads for '{table_name}' table...."
            )
            raise ImportError(
                "Trace filepath to highlight the root cause of the duplicated rows..."
            )

        elif total_null_values_in_table > 0:
            root_logger.error(
                f"ERROR: There are {total_null_values_in_table} NULL values in '{table_name}' table...."
            )
            raise ImportError(
                "Examine table to highlight the columns with the NULL values - justify if these fields should contain NULLs ..."
            )

        else:
            root_logger.debug("")
            root_logger.info("DATA VALIDATION SUCCESS: All general DQ checks passed! ")
            root_logger.debug("")

        root_logger.info("Now saving changes made by SQL statements to Postgres DB....")
        root_logger.info(
            "Saved successfully, now terminating cursor and current session...."
        )

    except Exception as e:
        root_logger.info(e)

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
    root_logger.info("Ending show_data_metrics process...")


def main(dir_path: str) -> None:
    """create db tables, format data and upload into dwh L1

    Args:
        dir_path (str): directory path from where to read
    """
    create_dwh_infra()
    lst_values = format_data_to_upload(dir_path)
    original_rows, rows_inserted, total_rows_in_table = upload_data_to_dwh(lst_values)
    show_data_metrics(original_rows, rows_inserted, total_rows_in_table)
