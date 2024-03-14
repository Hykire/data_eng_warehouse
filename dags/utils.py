import configparser
import logging
import os
from datetime import datetime
from typing import Any, List, Tuple

import psycopg2

# Set up root root_logger
root_logger = logging.getLogger(__name__)


def print_sensitive_data(
    root_logger: Any,
    table_name: str,
    cursor: Any,
    sensitive_columns_selected: List[str],
):
    """Perform print in console/log

    Args:
        root_logger (Any): logger
        table_name (str): table name
        cursor (Any): postgresql cursor
        sensitive_columns_selected (List[str]): list of sensitive columns
    """

    note_1 = """IMPORTANT NOTE: Invest time in understanding the underlying data fields to avoid highlighting the incorrect fields or omitting fields containing confidential information.          """
    note_2 = """      Involving the relevant stakeholders in the process of identifying sensitive data fields from the source data is a crucial step to protecting confidential information. """
    note_3 = """      Neglecting this step could expose customers and the wider company to serious harm (e.g. cybersecurity hacks, data breaches, unauthorized access to sensitive data), so approach this task with the utmost care. """

    get_list_of_column_names = f"""
        SELECT column_name FROM information_schema.columns
        WHERE   table_name = '{table_name}'
        ORDER BY ordinal_position
    """
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
        "latitude",
        "longitude",
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


def show_metrics(
    root_logger: Any,
    cursor: Any,
    table_name: str,
    active_schema_name: str,
    total_rows_in_table: int,
    database: str,
    data_warehouse_layer: str,
    original_rows: int,
    rows_inserted: int,
    CREATING_SCHEMA_PROCESSING_END_TIME: int,
    CREATING_SCHEMA_PROCESSING_START_TIME: int,
    CREATING_SCHEMA_VAL_CHECK_END_TIME: int,
    CREATING_SCHEMA_VAL_CHECK_START_TIME: int,
    CREATING_TABLE_PROCESSING_END_TIME: float,
    CREATING_TABLE_PROCESSING_START_TIME: float,
    CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME: float,
    CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME: float,
    ADDING_DATA_LINEAGE_PROCESSING_END_TIME: float,
    ADDING_DATA_LINEAGE_PROCESSING_START_TIME: float,
    ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME: float,
    ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME: float,
    ROW_INSERTION_PROCESSING_END_TIME: float,
    ROW_INSERTION_PROCESSING_START_TIME: float,
    ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME: float,
    ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME: float,
):
    """Perform print in console/logs of metrics such as insert ones

    Args:
        root_logger (Any): logger
        cursor (Any): postgresql cursor
        table_name (str): table name
        active_schema_name (str): schema name
        total_rows_in_table (int): no of rows in table
        database (str): db name
        data_warehouse_layer (str): layer name
        original_rows (int): no of original rows
        rows_inserted (int): no of inserted rows
        CREATING_SCHEMA_PROCESSING_END_TIME (int): time
        CREATING_SCHEMA_PROCESSING_START_TIME (int): time
        CREATING_SCHEMA_VAL_CHECK_END_TIME (int): time
        CREATING_SCHEMA_VAL_CHECK_START_TIME (int): time
        CREATING_TABLE_PROCESSING_END_TIME (int): time
        CREATING_TABLE_PROCESSING_START_TIME (int): time
        CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME (int): time
        CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME (int): time
        ADDING_DATA_LINEAGE_PROCESSING_END_TIME (int): time
        ADDING_DATA_LINEAGE_PROCESSING_START_TIME (int): time
        ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME (int): time
        ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME (int): time
        ROW_INSERTION_PROCESSING_END_TIME (int): time
        ROW_INSERTION_PROCESSING_START_TIME (int): time
        ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME (int): time
        ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME (int): time
    """
    # Prepare data profiling metrics
    count_total_no_of_columns_in_table = f""" SELECT          COUNT(column_name)
                                                    FROM            information_schema.columns
                                                    WHERE           table_name      =   '{table_name}'
                                                    AND             table_schema    =   '{active_schema_name}'
    """
    count_total_no_of_unique_records_in_table = f""" SELECT COUNT(*) FROM
                                                            (SELECT DISTINCT * FROM {active_schema_name}.{table_name}) as unique_records
    """
    get_list_of_column_names = f"""SELECT      column_name
                                        FROM        information_schema.columns
                                        WHERE       table_name = '{table_name}'
                                        ORDER BY    ordinal_position
    """
    # --------- A. Table statistics
    cursor.execute(count_total_no_of_columns_in_table)
    total_columns_in_table = cursor.fetchone()[0]

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
    root_logger.info(
        f"Schema name:                                 {active_schema_name} "
    )
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
                FROM {active_schema_name}.{table_name}
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

    if (EXECUTION_TIME_FOR_ROW_COUNT > 1000) and (EXECUTION_TIME_FOR_ROW_COUNT < 60000):
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

    # Commit the changes made in Postgres
    root_logger.info("Now saving changes made by SQL statements to Postgres DB....")
    # postgres_connection.commit()
    root_logger.info(
        "Saved successfully, now terminating cursor and current session...."
    )


def get_constans(
    db_type: str,
    foreign_server: str,
    src_db_name: str,
    src_schema_name: str,
    active_schema_name: str,
    src_table_name: str,
    table_name: str,
    data_warehouse_layer: str,
) -> Tuple[
    str,
    str,
    str,
    str,
    str,
    str,
    datetime,
    List[str],
    Any,
    Any,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    Any,
    Any,
]:
    """return constants

    Args:
        db_type (str): db type in db_credentials.ini
        foreign_server (str): name of foreign server
        src_db_name (str): source db name
        src_schema_name (str): source db schema
        active_schema_name (str): active schema name
        src_table_name (str): source table name
        table_name (str): active table name
        data_warehouse_layer (str): active layer

    Returns:
        _type_: _description_
    """
    # Constants
    path = os.path.abspath("/opt/airflow/dags/db_credentials.ini")
    config = configparser.ConfigParser()
    config.read(path)
    host = config["db_credentials"]["HOST"]
    port = config["db_credentials"]["PORT"]
    database = config["db_credentials"][db_type]
    username = config["db_credentials"]["USERNAME"]
    password = config["db_credentials"]["PASSWORD"]
    CURRENT_TIMESTAMP = datetime.now()
    source_system = ["s3://confidential"]
    row_counter = 0
    fdw_extension = "postgres_fdw"
    fdw_user = username
    active_db_name = database
    return (
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
        "dev",
        "prod",
    )


def get_cursor(db_type: str, data_warehouse_layer: str) -> Tuple[Any, Any]:
    """Get cursor from psycopg2 obj

    Raises:
        ConnectionError: error while connecting to postgresql
        Exception: general excepcion

    Returns:
        Tuple[Any, Any]: returns postgres connection and its cursor
    """
    path = os.path.abspath("/opt/airflow/dags/db_credentials.ini")
    config = configparser.ConfigParser()
    config.read(path)
    host = config["db_credentials"]["HOST"]
    port = config["db_credentials"]["PORT"]
    database = config["db_credentials"][db_type]
    username = config["db_credentials"]["USERNAME"]
    password = config["db_credentials"]["PASSWORD"]
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
                f"CONNECTION SUCCESS: Managed to connect successfully to the {data_warehouse_layer} database!!"
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
