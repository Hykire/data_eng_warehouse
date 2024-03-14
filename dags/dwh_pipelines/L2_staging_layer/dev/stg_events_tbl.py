import logging
import random
import time
from typing import Any, List, Tuple

import numpy as np
import pandas as pd
import pytz
from psycopg2 import extras
from utils import get_constans, get_cursor, print_sensitive_data, show_metrics

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

CREATING_SCHEMA_PROCESSING_START_TIME = 0
CREATING_SCHEMA_PROCESSING_END_TIME = 0
CREATING_SCHEMA_VAL_CHECK_START_TIME = 0
CREATING_SCHEMA_VAL_CHECK_END_TIME = 0
CREATING_TABLE_PROCESSING_START_TIME = 0
CREATING_TABLE_PROCESSING_END_TIME = 0
CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME = 0
CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME = 0
ADDING_DATA_LINEAGE_PROCESSING_START_TIME = 0
ADDING_DATA_LINEAGE_PROCESSING_END_TIME = 0
ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME = 0
ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME = 0
ROW_INSERTION_PROCESSING_START_TIME = 0
ROW_INSERTION_PROCESSING_END_TIME = 0
ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME = 0
ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME = 0


def enable_cross_database() -> None:
    """ENABLING CROSS-DATABASE QUERYING VIA FDW"""
    root_logger.info("Beginning enable_cross_database process...")
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

        # postgres_connection.commit()

        # Create the postgres_fdw extension
        try:
            import_postgres_fdw = f"""    CREATE EXTENSION IF NOT EXISTS {fdw_extension}
                                            ;
            """

            cursor.execute(import_postgres_fdw)
            # postgres_connection.commit()

            root_logger.info("")
            root_logger.info(
                f"Successfully IMPORTED the '{fdw_extension}' extension. Now advancing to creating the foreign server..."
            )
            root_logger.info("")
        except Exception as e:
            root_logger.error(e)

        # Create the foreign server
        try:
            create_foreign_server = f"""
            DO
            $$BEGIN
                CREATE SERVER {foreign_server}
                FOREIGN DATA WRAPPER {fdw_extension}
                OPTIONS (host '{host}', dbname '{src_db_name}', port '{port}')
                ;
            EXCEPTION
                WHEN others THEN
                    NULL;  -- ignore
            END;$$;

            """
            cursor.execute(create_foreign_server)
            # postgres_connection.commit()
            root_logger.info("")
            root_logger.info(
                f"Successfully CREATED the '{foreign_server}' foreign server. Now advancing to user mapping stage..."
            )
            root_logger.info("")
        except Exception as e:
            root_logger.error(e)

        # Create the user mapping between the fdw_user and local user
        try:
            map_fdw_user_to_local_user = f"""
            DO
            $$BEGIN
                CREATE USER MAPPING FOR {username}
                SERVER {foreign_server}
                OPTIONS (user '{fdw_user}', password '{password}')
                ;
            EXCEPTION
                WHEN others THEN
                    NULL;  -- ignore
            END;$$;

            """

            cursor.execute(map_fdw_user_to_local_user)
            # postgres_connection.commit()

            root_logger.info("")
            root_logger.info(
                f"Successfully mapped the '{fdw_user}' fdw user to the '{username}' local user. "
            )
            root_logger.info("")

            root_logger.info("")
            root_logger.info(
                "-------------------------------------------------------------------------------------------------------------------------------------------"
            )
            root_logger.info("")
            root_logger.info(
                f"You should now be able to create and interact with the virtual tables that mirror the actual tables from the '{src_db_name}' database. "
            )
            root_logger.info("")
            root_logger.info(
                "-------------------------------------------------------------------------------------------------------------------------------------------"
            )
            root_logger.info("")
        except Exception as e:
            root_logger.error(e)

        # Import the foreign schema from the previous layer's source table
        try:
            import_foreign_schema = f"""
            DO
            $$BEGIN
                IMPORT FOREIGN SCHEMA "{src_schema_name}"
                LIMIT TO ({src_table_name})
                FROM SERVER {foreign_server}
                INTO {active_schema_name}
                ;
            EXCEPTION
                WHEN others THEN
                    NULL;  -- ignore
            END;$$;
            """

            cursor.execute(import_foreign_schema)
            # postgres_connection.commit()

            root_logger.info("")
            root_logger.info(
                f"Successfully imported the '{src_table_name}' table into '{active_db_name}' database . "
            )
            root_logger.info("")

        except Exception as e:
            root_logger.error(e)
            root_logger.error("")
            root_logger.error(
                f"Unable to import the '{src_table_name}' table into '{active_db_name}' database . "
            )
            root_logger.error("")

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
    root_logger.info("Ending enable_cross_database process...")


def extract_data_from_dwh_l1() -> pd.DataFrame:
    """extract event's data from layer 1

    Returns:
        pd.DataFrame: events_tbl_df
    """
    root_logger.info("Beginning extract_data_from_dwh_l1 process...")
    events_tbl_df = pd.DataFrame()
    try:
        postgres_connection, cursor = get_cursor(db_type, data_warehouse_layer)
        data_lineage_columns = [
            "created_at",
            "updated_at",
            "source_system",
            # "source_file",
            "load_timestamp",
            "dwh_layer",
        ]

        desired_data_lineage_sql_columns = []
        columns_w_events_data = []

        get_list_of_column_names = f"""SELECT      column_name
                                            FROM        information_schema.columns
                                            WHERE       table_name = '{src_table_name}'
                                            ORDER BY    ordinal_position
        """

        cursor.execute(get_list_of_column_names)
        # postgres_connection.commit()

        list_of_column_names = cursor.fetchall()
        column_names = [sql_result[0] for sql_result in list_of_column_names]

        total_desired_data_lineage_sql_columns_added = 0
        for column_name in data_lineage_columns:
            if column_name not in column_names:
                total_desired_data_lineage_sql_columns_added += 1
                desired_data_lineage_sql_columns.append(column_name)
                root_logger.info(
                    f""" {total_desired_data_lineage_sql_columns_added}:    Added column '{column_name}' to desired columns list...  """
                )
        if total_desired_data_lineage_sql_columns_added == 0:
            root_logger.info("")
            root_logger.info(
                f""" COMPLETED: Successfully imported 100% data lineage columns """
            )
            root_logger.info("")
        else:
            root_logger.info("")
            root_logger.info(
                f""" Some data lineage columns are missing from import: {desired_data_lineage_sql_columns} """
            )
            root_logger.info("")

        # Pull events_tbl data from staging tables in Postgres database
        columns_w_events_data = list(set(column_names) - set(data_lineage_columns))
        fetch_raw_events_tbl = f""" SELECT { ', '.join(columns_w_events_data) } FROM {active_schema_name}.{src_table_name};
        """
        root_logger.debug(fetch_raw_events_tbl)
        root_logger.info("")
        root_logger.info(
            f"Successfully IMPORTED the '{src_table_name}' virtual table from the '{foreign_server}' server into the '{active_schema_name}' schema for '{database}' database. Now advancing to data cleaning stage..."
        )
        root_logger.info("")

        # Execute SQL command to interact with Postgres database
        cursor.execute(fetch_raw_events_tbl)

        # Extract header names from cursor's description
        postgres_table_headers = [header[0] for header in cursor.description]

        # Execute script
        postgres_table_results = cursor.fetchall()

        # Use Postgres results to create data frame for events_tbl
        events_tbl_df = pd.DataFrame(
            data=postgres_table_results, columns=postgres_table_headers
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
    root_logger.info("Ending extract_data_from_dwh_l1 process...")
    return events_tbl_df


def transform_data(df_extracted: pd.DataFrame) -> pd.DataFrame:
    """Perform transformation such as convert string to date, convert string to float, etc

    Args:
        df_extracted (pd.DataFrame): data from dwh l1

    Returns:
        pd.DataFrame: transformed data
    """
    root_logger.info("Beginning transform_data process...")
    # perform transformations
    # Rename columns
    df_extracted = df_extracted.rename(columns={"data_location_lat": "latitude"})
    df_extracted = df_extracted.rename(columns={"data_location_at": "t_event"})
    df_extracted = df_extracted.rename(columns={"data_location_lng": "longitude"})
    df_extracted = df_extracted.rename(columns={"data_finish": "t_event_finish"})
    df_extracted = df_extracted.rename(columns={"data_start": "t_event_start"})
    # Change data type
    df_extracted["longitude"] = df_extracted["longitude"].astype("float")
    df_extracted["latitude"] = df_extracted["latitude"].astype("float")
    # change dwh_layer
    df_extracted["dwh_layer"] = data_warehouse_layer
    # Nulls
    df_extracted.loc[df_extracted["t_event"].isnull(), "t_event"] = None
    df_extracted.loc[df_extracted["latitude"].isnull(), "latitude"] = None
    df_extracted.loc[df_extracted["longitude"].isnull(), "longitude"] = None
    df_extracted.loc[df_extracted["t_event_start"].isnull(), "t_event_start"] = None
    df_extracted.loc[df_extracted["t_event_finish"].isnull(), "t_event_finish"] = None
    df_extracted = df_extracted.replace({np.NaN: None})
    root_logger.info(df_extracted.dtypes)
    root_logger.info("Ending transform_data process...")
    return df_extracted


def load_data_into_stg_tbl(df_transformed: pd.DataFrame) -> None:
    """Load transformed data into dwh l2

    Args:
        df_transformed (pd.DataFrame): transformed data from L1 ready to push into l2
    """
    root_logger.info("Beginning load_data_into_stg_tbl process...")
    try:
        postgres_connection, cursor = get_cursor(db_type, data_warehouse_layer)
        # Set up SQL statements for table creation and validation check
        create_stg_events_tbl = f"""CREATE TABLE IF NOT EXISTS {active_schema_name}.{table_name} (
                                        event                   varchar(255),
                                        on_target_entity        varchar(255),
                                        t                       timestamptz,
                                        organization_id         varchar(255),
                                        data_id                 VARCHAR(255),
                                        t_event                 timestamptz,
                                        latitude                numeric,
                                        longitude               numeric,
                                        t_event_start           timestamptz,
                                        t_event_finish          timestamptz
                                    );
        """

        check_if_stg_events_tbl_exists = f"""       SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
        """
        # Set up SQL statements for adding data lineage and validation check
        add_data_lineage_to_stg_events_tbl = f""" ALTER TABLE {active_schema_name}.{table_name}
                                                    ADD COLUMN if not exists  created_at                  timestamptz DEFAULT CURRENT_TIMESTAMP,
                                                    ADD COLUMN if not exists  updated_at                  timestamptz DEFAULT CURRENT_TIMESTAMP,
                                                    ADD COLUMN if not exists  source_system               VARCHAR(255),
                                                    ADD COLUMN if not exists  source_file                 VARCHAR(255),
                                                    ADD COLUMN if not exists  load_timestamp              TIMESTAMP,
                                                    ADD COLUMN if not exists  dwh_layer                   VARCHAR(255)
                                            ;
        """
        check_if_data_lineage_fields_are_added_to_tbl = f"""
                                                            SELECT *
                                                            FROM    information_schema.columns
                                                            WHERE   table_name      = '{table_name}'
                                                            AND table_schema = '{active_schema_name}'
                                                                AND     (column_name    = 'created_at'
                                                                OR      column_name     = 'updated_at'
                                                                OR      column_name     = 'source_system'
                                                                OR      column_name     = 'source_file'
                                                                OR      column_name     = 'load_timestamp'
                                                                OR      column_name     = 'dwh_layer');

        """
        check_total_row_count_before_insert_statement = (
            f""" SELECT COUNT(*) FROM {active_schema_name}.{table_name} """
        )
        insert_events_data = f"""INSERT INTO {active_schema_name}.{table_name} (
                                    event,
                                    on_target_entity,
                                    t,
                                    organization_id,
                                    data_id,
                                    t_event,
                                    latitude,
                                    longitude,
                                    created_at,
                                    updated_at,
                                    source_system,
                                    source_file,
                                    load_timestamp,
                                    dwh_layer,
                                    t_event_start,
                                    t_event_finish
                                )
                                VALUES %s
                                ON CONFLICT (event, on_target_entity, t, organization_id, data_id, source_file)
                                do update
                                set
                                t_event = EXCLUDED.t_event,
                                latitude = EXCLUDED.latitude,
                                longitude = EXCLUDED.longitude,
                                updated_at = EXCLUDED.updated_at,
                                source_system = EXCLUDED.source_system,
                                load_timestamp = EXCLUDED.load_timestamp,
                                dwh_layer = EXCLUDED.dwh_layer,
                                t_event_start = EXCLUDED.t_event_start,
                                t_event_finish = EXCLUDED.t_event_finish
                                ;
        """
        check_total_row_count_after_insert_statement = (
            f""" SELECT COUNT(*) FROM {active_schema_name}.{table_name} """
        )
        # Create table if it doesn't exist in Postgres
        CREATING_TABLE_PROCESSING_START_TIME = time.time()
        cursor.execute(create_stg_events_tbl)
        CREATING_TABLE_PROCESSING_END_TIME = time.time()

        CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME = time.time()
        cursor.execute(check_if_stg_events_tbl_exists)
        CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME = time.time()

        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(
                f"============================================================================================================================================================================="
            )
            root_logger.info(
                f"TABLE CREATION SUCCESS: Managed to create {table_name} table in {active_db_name}.  "
            )
            root_logger.info(
                f"SQL Query for validation check:  {check_if_stg_events_tbl_exists} "
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
                f"TABLE CREATION FAILURE: Unable to create {table_name}... "
            )
            root_logger.error(
                f"SQL Query for validation check:  {check_if_stg_events_tbl_exists} "
            )
            root_logger.error(
                f"=========================================================================================================================================================================="
            )
            root_logger.debug(f"")

        # Add data lineage to table
        ADDING_DATA_LINEAGE_PROCESSING_START_TIME = time.time()
        cursor.execute(add_data_lineage_to_stg_events_tbl)
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
                f"DATA LINEAGE FIELDS CREATION SUCCESS: Managed to create data lineage columns in {active_schema_name}.{table_name}.  "
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
                f"DATA LINEAGE FIELDS CREATION FAILURE: Unable to create data lineage columns in {active_schema_name}.{table_name}.... "
            )
            root_logger.error(
                f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} "
            )
            root_logger.error(
                f"=========================================================================================================================================================================="
            )
            root_logger.debug(f"")

        ROW_INSERTION_PROCESSING_START_TIME = time.time()
        cursor.execute(check_total_row_count_before_insert_statement)
        sql_result = cursor.fetchone()[0]
        root_logger.info(f"Rows before SQL insert in Postgres: {sql_result} ")
        root_logger.debug(f"")

        # adding unique index to avoid duplicates
        create_unique_index = f"""create unique index IF NOT EXISTS unique_idx_{active_schema_name}_{table_name} on {active_schema_name}.{table_name}(event,
            on_target_entity, t, organization_id, data_id, source_file)
            ;
        """
        cursor.execute(create_unique_index)
        lst_values = []
        for _, row in df_transformed.iterrows():
            values = (
                row["event"],
                str(row["on_target_entity"]),
                row["t"],
                str(row["organization_id"]),
                str(row["data_id"]),
                row["t_event"],
                row["latitude"],
                row["longitude"],
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                random.choice(source_system),
                row["source_file"],
                CURRENT_TIMESTAMP,
                data_warehouse_layer,
                row["t_event_start"],
                row["t_event_finish"],
            )
            lst_values.append(values)
        # Use high performance insert instead of row by row
        extras.execute_values(cursor, insert_events_data, lst_values, page_size=1000)
        # Validate insertion
        row_counter = len(lst_values)
        root_logger.debug(f"---------------------------------")
        root_logger.info(f"INSERT SUCCESS: Uploaded events record no {row_counter} ")
        root_logger.debug(f"---------------------------------")
        ROW_INSERTION_PROCESSING_END_TIME = time.time()

        ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME = time.time()
        cursor.execute(check_total_row_count_after_insert_statement)
        ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME = time.time()

        total_rows_in_table = cursor.fetchone()[0]
        root_logger.info(f"Rows after SQL insert in Postgres: {total_rows_in_table} ")
        root_logger.debug(f"")

        sensitive_columns_selected = [
            "organization_id",
            "data_id",
            "latitude",
            "longitude",
        ]
        print_sensitive_data(
            root_logger, table_name, cursor, sensitive_columns_selected
        )
        show_metrics(
            root_logger,
            cursor,
            table_name,
            active_schema_name,
            total_rows_in_table,
            database,
            data_warehouse_layer,
            len(lst_values),
            int(row_counter),
            CREATING_SCHEMA_PROCESSING_END_TIME,
            CREATING_SCHEMA_PROCESSING_START_TIME,
            CREATING_SCHEMA_VAL_CHECK_END_TIME,
            CREATING_SCHEMA_VAL_CHECK_START_TIME,
            CREATING_TABLE_PROCESSING_END_TIME,
            CREATING_TABLE_PROCESSING_START_TIME,
            CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME,
            CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME,
            ADDING_DATA_LINEAGE_PROCESSING_END_TIME,
            ADDING_DATA_LINEAGE_PROCESSING_START_TIME,
            ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME,
            ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME,
            ROW_INSERTION_PROCESSING_END_TIME,
            ROW_INSERTION_PROCESSING_START_TIME,
            ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME,
            ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME,
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
    root_logger.info("Ending load_data_into_stg_tbl process...")


def main_dwh_l2_staging() -> None:
    """Main process to get data into dwh l2"""
    enable_cross_database()
    df_extracted = extract_data_from_dwh_l1()
    df_transformed = transform_data(df_extracted)
    load_data_into_stg_tbl(df_transformed)
