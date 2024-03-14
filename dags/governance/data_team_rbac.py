import configparser
import logging
import os
from typing import Any, Dict, List, Tuple

import psycopg2
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


def __create_roles(
    cursor: Any,
    role_databases: Dict[str, Any],
    db_schemas: Dict[str, Any],
    postgres_connection: Any,
):
    """create 6 roles, data scientist, data analyst, data engineer, each one having a junior and senior role.

    Args:
        cursor (Any): postgresql cursor
        role_databases (Dict[str, Any]): dict of database's roles
        db_schemas (Dict[str, Any]): dict of db's schemas
        postgres_connection (Any): postgresql connection
    """
    # ================================================== CREATE CUSTOM ROLES =======================================

    try:
        root_logger.info(
            f"=========================================== CREATE CUSTOM ROLES ======================================="
        )
        root_logger.info(
            f"======================================================================================================"
        )
        root_logger.info(f"")
        root_logger.info(f"")

        for data_role in role_databases:
            checking_if_roles_exist_sql_query = (
                f"""SELECT 1 FROM pg_roles WHERE rolname = '{data_role}' ;"""
            )
            cursor.execute(checking_if_roles_exist_sql_query)
            # postgres_connection.commit()

            role_exists = cursor.fetchone()

            if role_exists:
                for data_role, databases in role_databases.items():
                    for db in databases:
                        if db in db_schemas:
                            schemas = db_schemas[db]
                            for schema in schemas:
                                try:
                                    revoke_all_privileges_from_database = f"""   REVOKE ALL PRIVILEGES ON DATABASE {db} FROM {data_role} ;  """
                                    cursor.execute(revoke_all_privileges_from_database)
                                    # postgres_connection.commit()
                                    root_logger.debug(
                                        f""" Revoking all privileges for all tables in '{db}' database for '{data_role}' role... """
                                    )

                                    revoke_all_privileges_from_all_tables = f""" REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} from {data_role} ;"""
                                    cursor.execute(
                                        revoke_all_privileges_from_all_tables
                                    )

                                    revoke_all_privileges_from_schema = f""" REVOKE ALL PRIVILEGES ON SCHEMA {schema} FROM {data_role}  ; """
                                    cursor.execute(revoke_all_privileges_from_schema)

                                    root_logger.debug(
                                        f""" Revoking all privileges for all tables in '{schema}' schema for '{data_role}' role... """
                                    )

                                    drop_role_sql_query = (
                                        f""" DROP ROLE {data_role}; """
                                    )
                                    cursor.execute(drop_role_sql_query)
                                    # postgres_connection.commit()
                                    root_logger.info(
                                        f'Dropped "{data_role}" successfully ... Now re-creating "{data_role}" role...'
                                    )

                                    creating_roles_sql_query = (
                                        f"""CREATE ROLE {data_role} NOLOGIN;"""
                                    )
                                    cursor.execute(creating_roles_sql_query)
                                    # postgres_connection.commit()
                                    root_logger.info(
                                        f"""Successfully created '{data_role}' role"""
                                    )

                                    root_logger.info(
                                        f"==========================================="
                                    )
                                    root_logger.info(f"")
                                    root_logger.info(f"")

                                except psycopg2.Error as e:
                                    postgres_connection.rollback()
                                    continue
                        else:
                            root_logger.error(
                                f"Database '{db}' not found in 'db_schema' dictionary "
                            )

            else:
                creating_roles_sql_query = f"""CREATE ROLE {data_role} NOLOGIN;"""
                cursor.execute(creating_roles_sql_query)
                # postgres_connection.commit()

                root_logger.info(f"""Successfully created '{data_role}' role""")
                root_logger.info(f"===========================================")
                root_logger.info(f"")
                root_logger.info(f"")

    except psycopg2.Error as e:
        root_logger.error(e)


def __setup_db_schema_access_da(
    cursor_dwh: Any,
    grant_jda_access_to_database_sql_query: str,
    dwh_db: str,
    grant_jda_access_to_schema_info_sql_query: str,
    grant_sda_access_to_database_sql_query: str,
    grant_sda_access_to_schema_info_sql_query: str,
    dwh_reporting_schema: str,
):
    """perform db_schema_access_da querys

    Args:
        cursor_dwh (Any): postgresql dwh cursor
        grant_jda_access_to_database_sql_query (str): query
        dwh_db (str): name of dwh db
        grant_jda_access_to_schema_info_sql_query (str): query
        grant_sda_access_to_database_sql_query (str): query
        grant_sda_access_to_schema_info_sql_query (str): query
        dwh_reporting_schema (str): name of dwh_reporting_schema
    """
    ## A. Data analysts
    cursor_dwh.execute(grant_jda_access_to_database_sql_query)
    root_logger.info(
        f"""Granted 'junior_data_analyst' role access to connecting to '{dwh_db}' database """
    )
    cursor_dwh.execute(grant_jda_access_to_schema_info_sql_query)
    root_logger.info(
        f"""Granted 'junior_data_analyst' role access to viewing the information on '{dwh_reporting_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_dwh.execute(grant_sda_access_to_database_sql_query)
    root_logger.info(
        f"""Granted 'senior_data_analyst' role access to connecting to '{dwh_db}' database """
    )
    cursor_dwh.execute(grant_sda_access_to_schema_info_sql_query)
    root_logger.info(
        f"""Granted 'senior_data_analyst' role access to viewing the information on '{dwh_reporting_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")
    root_logger.info(f"")
    root_logger.info(f"")


def __setup_db_schema_access_de(
    cursor_raw: Any,
    cursor_stg: Any,
    cursor_semantic: Any,
    cursor_dwh: Any,
    raw_db: str,
    staging_db: str,
    semantic_db: str,
    dwh_db: str,
    raw_main_schema: str,
    staging_dev_schema: str,
    staging_prod_schema: str,
    semantic_dev_schema: str,
    semantic_prod_schema: str,
    dwh_live_schema: str,
    dwh_reporting_schema: str,
    grant_jde_access_to_database_sql_query_1: str,
    grant_jde_access_to_schema_info_sql_query_1: str,
    grant_jde_access_to_database_sql_query_2: str,
    grant_jde_access_to_schema_info_sql_query_2: str,
    grant_jde_access_to_schema_info_sql_query_3: str,
    grant_jde_access_to_database_sql_query_3: str,
    grant_jde_access_to_schema_info_sql_query_4: str,
    grant_jde_access_to_schema_info_sql_query_5: str,
    grant_jde_access_to_database_sql_query_4: str,
    grant_jde_access_to_schema_info_sql_query_6: str,
    grant_jde_access_to_schema_info_sql_query_7: str,
    grant_sde_access_to_database_sql_query_1: str,
    grant_sde_access_to_schema_info_sql_query_1: str,
    grant_sde_access_to_database_sql_query_2: str,
    grant_sde_access_to_schema_info_sql_query_2: str,
    grant_sde_access_to_schema_info_sql_query_3: str,
    grant_sde_access_to_database_sql_query_3: str,
    grant_sde_access_to_schema_info_sql_query_4: str,
    grant_sde_access_to_schema_info_sql_query_5: str,
    grant_sde_access_to_database_sql_query_4: str,
    grant_sde_access_to_schema_info_sql_query_6: str,
    grant_sde_access_to_schema_info_sql_query_7: str,
):
    """Perform db_schema_access_de querys

    Args:
        cursor_raw (Any): postgresql cursor
        cursor_stg (Any): postgresql cursor
        cursor_semantic (Any): postgresql cursor
        cursor_dwh (Any): postgresql cursor
        raw_db (str): name of db
        staging_db (str): name of db
        semantic_db (str): name of db
        dwh_db (str): name of db
        raw_main_schema (str): name of schema
        staging_dev_schema (str): name of schema
        staging_prod_schema (str): name of schema
        semantic_dev_schema (str): name of schema
        semantic_prod_schema (str): name of schema
        dwh_live_schema (str): name of schema
        dwh_reporting_schema (str): name of schema
        grant_jde_access_to_database_sql_query_1 (str): query
        grant_jde_access_to_schema_info_sql_query_1 (str): query
        grant_jde_access_to_database_sql_query_2 (str): query
        grant_jde_access_to_schema_info_sql_query_2 (str): query
        grant_jde_access_to_schema_info_sql_query_3 (str): query
        grant_jde_access_to_database_sql_query_3 (str): query
        grant_jde_access_to_schema_info_sql_query_4 (str): query
        grant_jde_access_to_schema_info_sql_query_5 (str): query
        grant_jde_access_to_database_sql_query_4 (str): query
        grant_jde_access_to_schema_info_sql_query_6 (str): query
        grant_jde_access_to_schema_info_sql_query_7 (str): query
        grant_sde_access_to_database_sql_query_1 (str): query
        grant_sde_access_to_schema_info_sql_query_1 (str): query
        grant_sde_access_to_database_sql_query_2 (str): query
        grant_sde_access_to_schema_info_sql_query_2 (str): query
        grant_sde_access_to_schema_info_sql_query_3 (str): query
        grant_sde_access_to_database_sql_query_3 (str): query
        grant_sde_access_to_schema_info_sql_query_4 (str): query
        grant_sde_access_to_schema_info_sql_query_5 (str): query
        grant_sde_access_to_database_sql_query_4 (str): query
        grant_sde_access_to_schema_info_sql_query_6 (str): query
        grant_sde_access_to_schema_info_sql_query_7 (str): query
    """
    ## B. Data engineers

    cursor_raw.execute(grant_jde_access_to_database_sql_query_1)
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to connecting to '{raw_db}' database """
    )
    cursor_raw.execute(grant_jde_access_to_schema_info_sql_query_1)
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to viewing the information on '{raw_main_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_stg.execute(grant_jde_access_to_database_sql_query_2)
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to connecting to '{staging_db}' database """
    )
    cursor_stg.execute(grant_jde_access_to_schema_info_sql_query_2)
    cursor_stg.execute(grant_jde_access_to_schema_info_sql_query_3)
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to viewing the information on '{staging_dev_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to viewing the information on '{staging_prod_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_semantic.execute(grant_jde_access_to_database_sql_query_3)
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to connecting to '{semantic_db}' database """
    )
    cursor_semantic.execute(grant_jde_access_to_schema_info_sql_query_4)
    cursor_semantic.execute(grant_jde_access_to_schema_info_sql_query_5)
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to viewing the information on '{semantic_dev_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to viewing the information on '{semantic_prod_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_dwh.execute(grant_jde_access_to_database_sql_query_4)
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to connecting to '{dwh_db}' database """
    )
    cursor_dwh.execute(grant_jde_access_to_schema_info_sql_query_6)
    cursor_dwh.execute(grant_jde_access_to_schema_info_sql_query_7)
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to viewing the information on '{dwh_live_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'junior_data_engineer' role access to viewing the information on '{dwh_reporting_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_raw.execute(grant_sde_access_to_database_sql_query_1)
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to connecting to '{raw_db}' database """
    )
    cursor_raw.execute(grant_sde_access_to_schema_info_sql_query_1)
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to viewing the information on '{raw_main_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_stg.execute(grant_sde_access_to_database_sql_query_2)
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to connecting to '{staging_db}' database """
    )
    cursor_stg.execute(grant_sde_access_to_schema_info_sql_query_2)
    cursor_stg.execute(grant_sde_access_to_schema_info_sql_query_3)
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to viewing the information on '{staging_dev_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to viewing the information on '{staging_prod_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_semantic.execute(grant_sde_access_to_database_sql_query_3)
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to connecting to '{semantic_db}' database """
    )
    cursor_semantic.execute(grant_sde_access_to_schema_info_sql_query_4)
    cursor_semantic.execute(grant_sde_access_to_schema_info_sql_query_5)
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to viewing the information on '{semantic_dev_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to viewing the information on '{semantic_prod_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_dwh.execute(grant_sde_access_to_database_sql_query_4)
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to connecting to '{dwh_db}' database """
    )
    cursor_dwh.execute(grant_sde_access_to_schema_info_sql_query_6)
    cursor_dwh.execute(grant_sde_access_to_schema_info_sql_query_7)
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to viewing the information on '{dwh_live_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'senior_data_engineer' role access to viewing the information on '{dwh_reporting_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")


def __setup_db_schema_access_ds(
    cursor_dwh: Any,
    cursor_stg: Any,
    cursor_semantic: Any,
    dwh_db: str,
    staging_db: str,
    semantic_db: str,
    staging_dev_schema: str,
    staging_prod_schema: str,
    semantic_dev_schema: str,
    semantic_prod_schema: str,
    dwh_live_schema: str,
    dwh_reporting_schema: str,
    grant_jds_access_to_database_sql_query_1: str,
    grant_jds_access_to_schema_info_sql_query_1: str,
    grant_jds_access_to_schema_info_sql_query_2: str,
    grant_jds_access_to_database_sql_query_2: str,
    grant_jds_access_to_schema_info_sql_query_3: str,
    grant_jds_access_to_schema_info_sql_query_4: str,
    grant_jds_access_to_database_sql_query_3: str,
    grant_jds_access_to_schema_info_sql_query_5: str,
    grant_jds_access_to_schema_info_sql_query_6: str,
    grant_sds_access_to_database_sql_query_1: str,
    grant_sds_access_to_schema_info_sql_query_1: str,
    grant_sds_access_to_schema_info_sql_query_2: str,
    grant_sds_access_to_database_sql_query_2: str,
    grant_sds_access_to_schema_info_sql_query_3: str,
    grant_sds_access_to_schema_info_sql_query_4: str,
    grant_sds_access_to_database_sql_query_3: str,
    grant_sds_access_to_schema_info_sql_query_5: str,
    grant_sds_access_to_schema_info_sql_query_6: str,
):
    """Perform db_schema_access_ds query

    Args:
        cursor_dwh (Any): Postgresql cursor
        cursor_stg (Any): Postgresql cursor
        cursor_semantic (Any): Postgresql cursor
        dwh_db (str): name of db
        staging_db (str): name of db
        semantic_db (str): name of db
        staging_dev_schema (str): name of schema
        staging_prod_schema (str): name of schema
        semantic_dev_schema (str): name of schema
        semantic_prod_schema (str): name of schema
        dwh_live_schema (str): name of schema
        dwh_reporting_schema (str): name of schema
        grant_jds_access_to_database_sql_query_1 (str): query
        grant_jds_access_to_schema_info_sql_query_1 (str): query
        grant_jds_access_to_schema_info_sql_query_2 (str): query
        grant_jds_access_to_database_sql_query_2 (str): query
        grant_jds_access_to_schema_info_sql_query_3 (str): query
        grant_jds_access_to_schema_info_sql_query_4 (str): query
        grant_jds_access_to_database_sql_query_3 (str): query
        grant_jds_access_to_schema_info_sql_query_5 (str): query
        grant_jds_access_to_schema_info_sql_query_6 (str): query
        grant_sds_access_to_database_sql_query_1 (str): query
        grant_sds_access_to_schema_info_sql_query_1 (str): query
        grant_sds_access_to_schema_info_sql_query_2 (str): query
        grant_sds_access_to_database_sql_query_2 (str): query
        grant_sds_access_to_schema_info_sql_query_3 (str): query
        grant_sds_access_to_schema_info_sql_query_4 (str): query
        grant_sds_access_to_database_sql_query_3 (str): query
        grant_sds_access_to_schema_info_sql_query_5 (str): query
        grant_sds_access_to_schema_info_sql_query_6 (str): query
    """
    ## C. Data scientists
    cursor_stg.execute(grant_jds_access_to_database_sql_query_1)
    root_logger.info(
        f"""Granted 'junior_data_scientist' role access to connecting to '{staging_db}' database """
    )
    cursor_stg.execute(grant_jds_access_to_schema_info_sql_query_1)
    cursor_stg.execute(grant_jds_access_to_schema_info_sql_query_2)
    root_logger.info(
        f"""Granted 'junior_data_scientist' role access to viewing the information on '{staging_dev_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'junior_data_scientist' role access to viewing the information on '{staging_prod_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_semantic.execute(grant_jds_access_to_database_sql_query_2)
    root_logger.info(
        f"""Granted 'junior_data_scientist' role access to connecting to '{semantic_db}' database """
    )
    cursor_semantic.execute(grant_jds_access_to_schema_info_sql_query_3)
    cursor_semantic.execute(grant_jds_access_to_schema_info_sql_query_4)
    root_logger.info(
        f"""Granted 'junior_data_scientist' role access to viewing the information on '{semantic_dev_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'junior_data_scientist' role access to viewing the information on '{semantic_prod_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_dwh.execute(grant_jds_access_to_database_sql_query_3)
    root_logger.info(
        f"""Granted 'junior_data_scientist' role access to connecting to '{dwh_db}' database """
    )
    cursor_dwh.execute(grant_jds_access_to_schema_info_sql_query_5)
    cursor_dwh.execute(grant_jds_access_to_schema_info_sql_query_6)
    root_logger.info(
        f"""Granted 'junior_data_scientist' role access to viewing the information on '{dwh_live_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'junior_data_scientist' role access to viewing the information on '{dwh_reporting_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_stg.execute(grant_sds_access_to_database_sql_query_1)
    root_logger.info(
        f"""Granted 'senior_data_scientist' role access to connecting to '{staging_db}' database """
    )
    cursor_stg.execute(grant_sds_access_to_schema_info_sql_query_1)
    cursor_stg.execute(grant_sds_access_to_schema_info_sql_query_2)
    root_logger.info(
        f"""Granted 'senior_data_scientist' role access to viewing the information on '{staging_dev_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'senior_data_scientist' role access to viewing the information on '{staging_prod_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_semantic.execute(grant_sds_access_to_database_sql_query_2)
    root_logger.info(
        f"""Granted 'senior_data_scientist' role access to connecting to '{semantic_db}' database """
    )
    cursor_semantic.execute(grant_sds_access_to_schema_info_sql_query_3)
    cursor_semantic.execute(grant_sds_access_to_schema_info_sql_query_4)
    root_logger.info(
        f"""Granted 'senior_data_scientist' role access to viewing the information on '{semantic_dev_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'senior_data_scientist' role access to viewing the information on '{semantic_prod_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")

    cursor_dwh.execute(grant_sds_access_to_database_sql_query_3)
    root_logger.info(
        f"""Granted 'senior_data_scientist' role access to connecting to '{dwh_db}' database """
    )
    cursor_dwh.execute(grant_sds_access_to_schema_info_sql_query_5)
    cursor_dwh.execute(grant_sds_access_to_schema_info_sql_query_6)
    root_logger.info(
        f"""Granted 'senior_data_scientist' role access to viewing the information on '{dwh_live_schema}' schema's objects """
    )
    root_logger.info(
        f"""Granted 'senior_data_scientist' role access to viewing the information on '{dwh_reporting_schema}' schema's objects """
    )
    root_logger.info(f"===========================================")
    root_logger.info(f"")
    root_logger.info(f"")


def __grant_database_access(
    cursor_dwh: Any,
    cursor_raw: Any,
    cursor_stg: Any,
    cursor_semantic: Any,
    dwh_db: str,
    raw_db: str,
    staging_db: str,
    semantic_db: str,
    raw_main_schema: str,
    dwh_reporting_schema: str,
    staging_dev_schema: str,
    staging_prod_schema: str,
    semantic_dev_schema: str,
    semantic_prod_schema: str,
    dwh_live_schema: str,
    grant_jda_access_to_database_sql_query: str,
    grant_jda_access_to_schema_info_sql_query: str,
    grant_sda_access_to_database_sql_query: str,
    grant_sda_access_to_schema_info_sql_query: str,
    grant_jde_access_to_database_sql_query_1: str,
    grant_jde_access_to_schema_info_sql_query_1: str,
    grant_jde_access_to_database_sql_query_2: str,
    grant_jde_access_to_schema_info_sql_query_2: str,
    grant_jde_access_to_schema_info_sql_query_3: str,
    grant_jde_access_to_database_sql_query_3: str,
    grant_jde_access_to_schema_info_sql_query_4: str,
    grant_jde_access_to_schema_info_sql_query_5: str,
    grant_jde_access_to_database_sql_query_4: str,
    grant_jde_access_to_schema_info_sql_query_6: str,
    grant_jde_access_to_schema_info_sql_query_7: str,
    grant_sde_access_to_database_sql_query_1: str,
    grant_sde_access_to_schema_info_sql_query_1: str,
    grant_sde_access_to_database_sql_query_2: str,
    grant_sde_access_to_schema_info_sql_query_2: str,
    grant_sde_access_to_schema_info_sql_query_3: str,
    grant_sde_access_to_database_sql_query_3: str,
    grant_sde_access_to_schema_info_sql_query_4: str,
    grant_sde_access_to_schema_info_sql_query_5: str,
    grant_sde_access_to_database_sql_query_4: str,
    grant_sde_access_to_schema_info_sql_query_6: str,
    grant_sde_access_to_schema_info_sql_query_7: str,
    grant_jds_access_to_database_sql_query_1: str,
    grant_jds_access_to_schema_info_sql_query_1: str,
    grant_jds_access_to_schema_info_sql_query_2: str,
    grant_jds_access_to_database_sql_query_2: str,
    grant_jds_access_to_schema_info_sql_query_3: str,
    grant_jds_access_to_schema_info_sql_query_4: str,
    grant_jds_access_to_database_sql_query_3: str,
    grant_jds_access_to_schema_info_sql_query_5: str,
    grant_jds_access_to_schema_info_sql_query_6: str,
    grant_sds_access_to_database_sql_query_1: str,
    grant_sds_access_to_schema_info_sql_query_1: str,
    grant_sds_access_to_schema_info_sql_query_2: str,
    grant_sds_access_to_database_sql_query_2: str,
    grant_sds_access_to_schema_info_sql_query_3: str,
    grant_sds_access_to_schema_info_sql_query_4: str,
    grant_sds_access_to_database_sql_query_3: str,
    grant_sds_access_to_schema_info_sql_query_5: str,
    grant_sds_access_to_schema_info_sql_query_6: str,
):
    """Perform sql query such as grants

    Args:
        cursor_dwh (Any): postgresql cursor
        cursor_raw (Any): postgresql cursor
        cursor_stg (Any): postgresql cursor
        cursor_semantic (Any): postgresql cursor
        dwh_db (str): name of db
        raw_db (str): name of db
        staging_db (str): name of db
        semantic_db (str): name of db
        raw_main_schema (str): name of schema
        dwh_reporting_schema (str): name of schema
        staging_dev_schema (str): name of schema
        staging_prod_schema (str): name of schema
        semantic_dev_schema (str): name of schema
        semantic_prod_schema (str): name of schema
        dwh_live_schema (str): name of schema
        grant_jda_access_to_database_sql_query (str): query
        grant_jda_access_to_schema_info_sql_query (str): query
        grant_sda_access_to_database_sql_query (str): query
        grant_sda_access_to_schema_info_sql_query (str): query
        grant_jde_access_to_database_sql_query_1 (str): query
        grant_jde_access_to_schema_info_sql_query_1 (str): query
        grant_jde_access_to_database_sql_query_2 (str): query
        grant_jde_access_to_schema_info_sql_query_2 (str): query
        grant_jde_access_to_schema_info_sql_query_3 (str): query
        grant_jde_access_to_database_sql_query_3 (str): query
        grant_jde_access_to_schema_info_sql_query_4 (str): query
        grant_jde_access_to_schema_info_sql_query_5 (str): query
        grant_jde_access_to_database_sql_query_4 (str): query
        grant_jde_access_to_schema_info_sql_query_6 (str): query
        grant_jde_access_to_schema_info_sql_query_7 (str): query
        grant_sde_access_to_database_sql_query_1 (str): query
        grant_sde_access_to_schema_info_sql_query_1 (str): query
        grant_sde_access_to_database_sql_query_2 (str): query
        grant_sde_access_to_schema_info_sql_query_2 (str): query
        grant_sde_access_to_schema_info_sql_query_3 (str): query
        grant_sde_access_to_database_sql_query_3 (str): query
        grant_sde_access_to_schema_info_sql_query_4 (str): query
        grant_sde_access_to_schema_info_sql_query_5 (str): query
        grant_sde_access_to_database_sql_query_4 (str): query
        grant_sde_access_to_schema_info_sql_query_6 (str): query
        grant_sde_access_to_schema_info_sql_query_7 (str): query
        grant_jds_access_to_database_sql_query_1 (str): query
        grant_jds_access_to_schema_info_sql_query_1 (str): query
        grant_jds_access_to_schema_info_sql_query_2 (str): query
        grant_jds_access_to_database_sql_query_2 (str): query
        grant_jds_access_to_schema_info_sql_query_3 (str): query
        grant_jds_access_to_schema_info_sql_query_4 (str): query
        grant_jds_access_to_database_sql_query_3 (str): query
        grant_jds_access_to_schema_info_sql_query_5 (str): query
        grant_jds_access_to_schema_info_sql_query_6 (str): query
        grant_sds_access_to_database_sql_query_1 (str): query
        grant_sds_access_to_schema_info_sql_query_1 (str): query
        grant_sds_access_to_schema_info_sql_query_2 (str): query
        grant_sds_access_to_database_sql_query_2 (str): query
        grant_sds_access_to_schema_info_sql_query_3 (str): query
        grant_sds_access_to_schema_info_sql_query_4 (str): query
        grant_sds_access_to_database_sql_query_3 (str): query
        grant_sds_access_to_schema_info_sql_query_5 (str): query
        grant_sds_access_to_schema_info_sql_query_6 (str): query
    """
    try:
        # ================================================== GRANT DATABASE AND INFO SCHEMA ACCESS =======================================

        root_logger.info(
            f"=========================================== GRANT DATABASE AND INFO SCHEMA ACCESS ======================================="
        )
        root_logger.info(
            f"======================================================================================================"
        )
        root_logger.info(f"")
        root_logger.info(f"")
        __setup_db_schema_access_da(
            cursor_dwh,
            grant_jda_access_to_database_sql_query,
            dwh_db,
            grant_jda_access_to_schema_info_sql_query,
            grant_sda_access_to_database_sql_query,
            grant_sda_access_to_schema_info_sql_query,
            dwh_reporting_schema,
        )

        __setup_db_schema_access_de(
            cursor_raw,
            cursor_stg,
            cursor_semantic,
            cursor_dwh,
            raw_db,
            staging_db,
            semantic_db,
            dwh_db,
            raw_main_schema,
            staging_dev_schema,
            staging_prod_schema,
            semantic_dev_schema,
            semantic_prod_schema,
            dwh_live_schema,
            dwh_reporting_schema,
            grant_jde_access_to_database_sql_query_1,
            grant_jde_access_to_schema_info_sql_query_1,
            grant_jde_access_to_database_sql_query_2,
            grant_jde_access_to_schema_info_sql_query_2,
            grant_jde_access_to_schema_info_sql_query_3,
            grant_jde_access_to_database_sql_query_3,
            grant_jde_access_to_schema_info_sql_query_4,
            grant_jde_access_to_schema_info_sql_query_5,
            grant_jde_access_to_database_sql_query_4,
            grant_jde_access_to_schema_info_sql_query_6,
            grant_jde_access_to_schema_info_sql_query_7,
            grant_sde_access_to_database_sql_query_1,
            grant_sde_access_to_schema_info_sql_query_1,
            grant_sde_access_to_database_sql_query_2,
            grant_sde_access_to_schema_info_sql_query_2,
            grant_sde_access_to_schema_info_sql_query_3,
            grant_sde_access_to_database_sql_query_3,
            grant_sde_access_to_schema_info_sql_query_4,
            grant_sde_access_to_schema_info_sql_query_5,
            grant_sde_access_to_database_sql_query_4,
            grant_sde_access_to_schema_info_sql_query_6,
            grant_sde_access_to_schema_info_sql_query_7,
        )
        __setup_db_schema_access_ds(
            cursor_dwh,
            cursor_stg,
            cursor_semantic,
            dwh_db,
            staging_db,
            semantic_db,
            staging_dev_schema,
            staging_prod_schema,
            semantic_dev_schema,
            semantic_prod_schema,
            dwh_live_schema,
            dwh_reporting_schema,
            grant_jds_access_to_database_sql_query_1,
            grant_jds_access_to_schema_info_sql_query_1,
            grant_jds_access_to_schema_info_sql_query_2,
            grant_jds_access_to_database_sql_query_2,
            grant_jds_access_to_schema_info_sql_query_3,
            grant_jds_access_to_schema_info_sql_query_4,
            grant_jds_access_to_database_sql_query_3,
            grant_jds_access_to_schema_info_sql_query_5,
            grant_jds_access_to_schema_info_sql_query_6,
            grant_sds_access_to_database_sql_query_1,
            grant_sds_access_to_schema_info_sql_query_1,
            grant_sds_access_to_schema_info_sql_query_2,
            grant_sds_access_to_database_sql_query_2,
            grant_sds_access_to_schema_info_sql_query_3,
            grant_sds_access_to_schema_info_sql_query_4,
            grant_sds_access_to_database_sql_query_3,
            grant_sds_access_to_schema_info_sql_query_5,
            grant_sds_access_to_schema_info_sql_query_6,
        )

    except psycopg2.Error as e:
        root_logger.error(e)


def __grant_table_ownership(
    cursor_dwh: Any,
    view_name: str,
    view_name2: str,
    grant_ownership_rights_to_sde: str,
    grant_ownership_rights_to_sde2: str,
    dwh_db: str,
    dwh_reporting_schema: str,
):
    """Perform grant sql statement

    Args:
        cursor_dwh (Any): postgresql cursor
        view_name (str): view name
        view_name2 (str): view name
        grant_ownership_rights_to_sde (str): query
        grant_ownership_rights_to_sde2 (str): query
        dwh_db (str): db name
        dwh_reporting_schema (str): db schema
    """
    try:
        root_logger.info(
            f"=========================================== GRANT TABLE OWNERSHIP RIGHTS TO ROLES ======================================="
        )
        root_logger.info(
            f"======================================================================================================"
        )
        root_logger.info(f"")
        root_logger.info(f"")

        cursor_dwh.execute(grant_ownership_rights_to_sde)
        cursor_dwh.execute(grant_ownership_rights_to_sde2)
        # postgres_connection.commit()
        root_logger.info(
            f"""Successfully granted 'senior_data_analyst' ownership of '{view_name}' table in '{dwh_db}.{dwh_reporting_schema}' schema  """
        )
        root_logger.info(
            f"""Successfully granted 'senior_data_analyst' ownership of '{view_name2}' table in '{dwh_db}.{dwh_reporting_schema}' schema  """
        )
        root_logger.info(f"===========================================")
        root_logger.info(f"")
        root_logger.info(f"")
        root_logger.info(f"")
        root_logger.info(f"")

    except psycopg2.Error as e:
        root_logger.error(e)


def __grant_privileges(
    cursor_dwh: Any,
    grant_privileges_for_jda_sql_query: str,
    grant_privileges_for_sda_sql_query: str,
    dwh_db: str,
    dwh_reporting_schema: str,
):
    """Perform grant to roles

    Args:
        cursor_dwh (Any): postgresql cursor
        grant_privileges_for_jda_sql_query (str): query
        grant_privileges_for_sda_sql_query (str): query
        dwh_db (str): db name
        dwh_reporting_schema (str): schema name
    """
    try:
        root_logger.info(
            f"=========================================== GRANT PRIVILEGES TO ROLES ======================================="
        )
        root_logger.info(
            f"======================================================================================================"
        )
        root_logger.info(f"")
        root_logger.info(f"")

        cursor_dwh.execute(grant_privileges_for_jda_sql_query)
        # postgres_connection.commit()
        root_logger.info(
            f"""Successfully granted privileges to 'junior_data_analyst' on all tables in '{dwh_db}.{dwh_reporting_schema}' schema.  """
        )
        root_logger.info(f"===========================================")
        root_logger.info(f"")
        root_logger.info(f"")
        root_logger.info(f"")
        root_logger.info(f"")

        cursor_dwh.execute(grant_privileges_for_sda_sql_query)
        # postgres_connection.commit()
        root_logger.info(
            f"""Successfully granted privileges to 'senior_data_analyst' on all tables in '{dwh_db}.{dwh_reporting_schema}' schema.  """
        )
        root_logger.info(f"===========================================")
        root_logger.info(f"")
        root_logger.info(f"")
        root_logger.info(f"")
        root_logger.info(f"")

    except psycopg2.Error as e:
        root_logger.error(e)


def __set_up_access_control() -> None:
    """create reporting view"""
    root_logger.info("Beginning dwh_create_indexes process...")
    try:
        postgres_connection_dwh, cursor_dwh = get_cursor(db_type, data_warehouse_layer)
        path = os.path.abspath("/opt/airflow/dags/db_credentials.ini")
        config = configparser.ConfigParser()
        config.read(path)
        # create vars
        raw_db = config["db_credentials"]["RAW_DB"]
        staging_db = config["db_credentials"]["STAGING_DB"]
        semantic_db = config["db_credentials"]["SEMANTIC_DB"]
        dwh_db = config["db_credentials"]["DWH_DB"]
        custom_roles = [
            "junior_data_analyst",
            "senior_data_analyst",
            "junior_data_engineer",
            "senior_data_engineer",
            "junior_data_scientist",
            "senior_data_scientist",
        ]

        raw_main_schema = "main"

        staging_dev_schema = "dev"
        staging_prod_schema = "prod"

        semantic_dev_schema = "dev"
        semantic_prod_schema = "prod"

        dwh_reporting_schema = "reporting"
        dwh_live_schema = "live"

        db_schemas = {
            "raw_db": [raw_main_schema],
            "staging_db": [staging_dev_schema, staging_prod_schema],
            "semantic_db": [semantic_dev_schema, semantic_prod_schema],
            "dwh_db": [dwh_live_schema, dwh_reporting_schema],
        }
        role_databases = {
            "junior_data_analyst": [dwh_db],
            "senior_data_analyst": [dwh_db],
            "junior_data_engineer": [raw_db, staging_db, semantic_db, dwh_db],
            "senior_data_engineer": [raw_db, staging_db, semantic_db, dwh_db],
            "junior_data_scientist": [staging_db, semantic_db, dwh_db],
            "senior_data_scientist": [staging_db, semantic_db, dwh_db],
        }

        # For granting access to the DWH databases

        ## A. Data analysts
        grant_jda_access_to_database_sql_query = (
            f""" GRANT CONNECT ON DATABASE {dwh_db} TO junior_data_analyst; """
        )
        grant_sda_access_to_database_sql_query = (
            f""" GRANT CONNECT ON DATABASE {dwh_db} TO senior_data_analyst; """
        )

        ## B. Data engineers
        grant_jde_access_to_database_sql_query_1 = (
            f""" GRANT CONNECT ON DATABASE {raw_db} TO junior_data_engineer; """
        )
        grant_jde_access_to_database_sql_query_2 = (
            f""" GRANT CONNECT ON DATABASE {staging_db} TO junior_data_engineer; """
        )
        grant_jde_access_to_database_sql_query_3 = (
            f""" GRANT CONNECT ON DATABASE {semantic_db} TO junior_data_engineer; """
        )
        grant_jde_access_to_database_sql_query_4 = (
            f""" GRANT CONNECT ON DATABASE {dwh_db} TO junior_data_engineer; """
        )

        grant_sde_access_to_database_sql_query_1 = (
            f""" GRANT CONNECT ON DATABASE {raw_db} TO senior_data_engineer; """
        )
        grant_sde_access_to_database_sql_query_2 = (
            f""" GRANT CONNECT ON DATABASE {staging_db} TO senior_data_engineer; """
        )
        grant_sde_access_to_database_sql_query_3 = (
            f""" GRANT CONNECT ON DATABASE {semantic_db} TO senior_data_engineer; """
        )
        grant_sde_access_to_database_sql_query_4 = (
            f""" GRANT CONNECT ON DATABASE {dwh_db} TO senior_data_engineer; """
        )

        ## C. Data scientists
        grant_jds_access_to_database_sql_query_1 = (
            f""" GRANT CONNECT ON DATABASE {staging_db} TO junior_data_scientist; """
        )
        grant_jds_access_to_database_sql_query_2 = (
            f""" GRANT CONNECT ON DATABASE {semantic_db} TO junior_data_scientist; """
        )
        grant_jds_access_to_database_sql_query_3 = (
            f""" GRANT CONNECT ON DATABASE {dwh_db} TO junior_data_scientist; """
        )

        grant_sds_access_to_database_sql_query_1 = (
            f""" GRANT CONNECT ON DATABASE {staging_db} TO senior_data_scientist; """
        )
        grant_sds_access_to_database_sql_query_2 = (
            f""" GRANT CONNECT ON DATABASE {semantic_db} TO senior_data_scientist; """
        )
        grant_sds_access_to_database_sql_query_3 = (
            f""" GRANT CONNECT ON DATABASE {dwh_db} TO senior_data_scientist; """
        )

        # For granting access to viewing metadata on objects within the specified schema

        ## A. Data analysts
        grant_jda_access_to_schema_info_sql_query = f""" GRANT USAGE ON SCHEMA {dwh_reporting_schema} TO junior_data_analyst   ; """
        grant_sda_access_to_schema_info_sql_query = f""" GRANT USAGE ON SCHEMA {dwh_reporting_schema} TO senior_data_analyst   ; """

        ## B. Data engineers
        grant_jde_access_to_schema_info_sql_query_1 = f""" GRANT USAGE ON SCHEMA {raw_main_schema} TO junior_data_engineer        ;  """
        grant_jde_access_to_schema_info_sql_query_2 = f""" GRANT USAGE ON SCHEMA {staging_dev_schema} TO junior_data_engineer     ;  """
        grant_jde_access_to_schema_info_sql_query_3 = f""" GRANT USAGE ON SCHEMA {staging_prod_schema} TO junior_data_engineer     ; """
        grant_jde_access_to_schema_info_sql_query_4 = f""" GRANT USAGE ON SCHEMA {semantic_dev_schema} TO junior_data_engineer     ; """
        grant_jde_access_to_schema_info_sql_query_5 = f""" GRANT USAGE ON SCHEMA {semantic_prod_schema} TO junior_data_engineer    ; """
        grant_jde_access_to_schema_info_sql_query_6 = f""" GRANT USAGE ON SCHEMA {dwh_live_schema} TO junior_data_engineer         ; """
        grant_jde_access_to_schema_info_sql_query_7 = f""" GRANT USAGE ON SCHEMA {dwh_reporting_schema} TO junior_data_engineer    ; """

        grant_sde_access_to_schema_info_sql_query_1 = f""" GRANT USAGE ON SCHEMA {raw_main_schema} TO senior_data_engineer         ; """
        grant_sde_access_to_schema_info_sql_query_2 = f""" GRANT USAGE ON SCHEMA {staging_dev_schema} TO senior_data_engineer     ;  """
        grant_sde_access_to_schema_info_sql_query_3 = f""" GRANT USAGE ON SCHEMA {staging_prod_schema} TO senior_data_engineer    ;  """
        grant_sde_access_to_schema_info_sql_query_4 = f""" GRANT USAGE ON SCHEMA {semantic_dev_schema} TO senior_data_engineer    ;  """
        grant_sde_access_to_schema_info_sql_query_5 = f""" GRANT USAGE ON SCHEMA {semantic_prod_schema} TO senior_data_engineer   ;  """
        grant_sde_access_to_schema_info_sql_query_6 = f""" GRANT USAGE ON SCHEMA {dwh_live_schema} TO senior_data_engineer        ;  """
        grant_sde_access_to_schema_info_sql_query_7 = f""" GRANT USAGE ON SCHEMA {dwh_reporting_schema} TO senior_data_engineer   ;  """

        ## C. Data scientists
        grant_jds_access_to_schema_info_sql_query_1 = f""" GRANT USAGE ON SCHEMA {staging_dev_schema} TO junior_data_scientist     ;  """
        grant_jds_access_to_schema_info_sql_query_2 = f""" GRANT USAGE ON SCHEMA {staging_prod_schema} TO junior_data_scientist    ;  """
        grant_jds_access_to_schema_info_sql_query_3 = f""" GRANT USAGE ON SCHEMA {semantic_dev_schema} TO junior_data_scientist    ;  """
        grant_jds_access_to_schema_info_sql_query_4 = f""" GRANT USAGE ON SCHEMA {semantic_prod_schema} TO junior_data_scientist  ;   """
        grant_jds_access_to_schema_info_sql_query_5 = f""" GRANT USAGE ON SCHEMA {dwh_live_schema} TO junior_data_scientist        ;  """
        grant_jds_access_to_schema_info_sql_query_6 = f""" GRANT USAGE ON SCHEMA {dwh_reporting_schema} TO junior_data_scientist   ;  """

        grant_sds_access_to_schema_info_sql_query_1 = f""" GRANT USAGE ON SCHEMA {staging_dev_schema} TO senior_data_scientist      ; """
        grant_sds_access_to_schema_info_sql_query_2 = f""" GRANT USAGE ON SCHEMA {staging_prod_schema} TO senior_data_scientist    ;  """
        grant_sds_access_to_schema_info_sql_query_3 = f""" GRANT USAGE ON SCHEMA {semantic_dev_schema} TO senior_data_scientist    ;  """
        grant_sds_access_to_schema_info_sql_query_4 = f""" GRANT USAGE ON SCHEMA {semantic_prod_schema} TO senior_data_scientist    ; """
        grant_sds_access_to_schema_info_sql_query_5 = f""" GRANT USAGE ON SCHEMA {dwh_live_schema} TO senior_data_scientist         ; """
        grant_sds_access_to_schema_info_sql_query_6 = f""" GRANT USAGE ON SCHEMA {dwh_reporting_schema} TO senior_data_scientist   ;  """

        # For granting table ownership to specific roles
        view_name = "latest_updates_on_top1_client"
        grant_ownership_rights_to_sde = f""" ALTER TABLE {dwh_reporting_schema}.{view_name} OWNER TO senior_data_analyst ; """
        view_name2 = "no_updates_per_client"
        grant_ownership_rights_to_sde2 = f""" ALTER TABLE {dwh_reporting_schema}.{view_name2} OWNER TO senior_data_analyst ; """

        # For granting privileges to roles

        grant_privileges_for_jda_sql_query = f"GRANT SELECT ON ALL TABLES IN SCHEMA {dwh_reporting_schema} TO junior_data_analyst"
        grant_privileges_for_sda_sql_query = f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {dwh_reporting_schema} TO senior_data_analyst"

        postgres_connection_raw, cursor_raw = get_cursor(raw_db, "raw")
        postgres_connection_stg, cursor_stg = get_cursor(staging_db, "staging")
        postgres_connection_semantic, cursor_semantic = get_cursor(
            semantic_db, "semantic"
        )

        __create_roles(cursor_dwh, role_databases, db_schemas, postgres_connection_dwh)
        __grant_database_access(
            cursor_dwh,
            cursor_raw,
            cursor_stg,
            cursor_semantic,
            dwh_db,
            raw_db,
            staging_db,
            semantic_db,
            raw_main_schema,
            dwh_reporting_schema,
            staging_dev_schema,
            staging_prod_schema,
            semantic_dev_schema,
            semantic_prod_schema,
            dwh_live_schema,
            grant_jda_access_to_database_sql_query,
            grant_jda_access_to_schema_info_sql_query,
            grant_sda_access_to_database_sql_query,
            grant_sda_access_to_schema_info_sql_query,
            grant_jde_access_to_database_sql_query_1,
            grant_jde_access_to_schema_info_sql_query_1,
            grant_jde_access_to_database_sql_query_2,
            grant_jde_access_to_schema_info_sql_query_2,
            grant_jde_access_to_schema_info_sql_query_3,
            grant_jde_access_to_database_sql_query_3,
            grant_jde_access_to_schema_info_sql_query_4,
            grant_jde_access_to_schema_info_sql_query_5,
            grant_jde_access_to_database_sql_query_4,
            grant_jde_access_to_schema_info_sql_query_6,
            grant_jde_access_to_schema_info_sql_query_7,
            grant_sde_access_to_database_sql_query_1,
            grant_sde_access_to_schema_info_sql_query_1,
            grant_sde_access_to_database_sql_query_2,
            grant_sde_access_to_schema_info_sql_query_2,
            grant_sde_access_to_schema_info_sql_query_3,
            grant_sde_access_to_database_sql_query_3,
            grant_sde_access_to_schema_info_sql_query_4,
            grant_sde_access_to_schema_info_sql_query_5,
            grant_sde_access_to_database_sql_query_4,
            grant_sde_access_to_schema_info_sql_query_6,
            grant_sde_access_to_schema_info_sql_query_7,
            grant_jds_access_to_database_sql_query_1,
            grant_jds_access_to_schema_info_sql_query_1,
            grant_jds_access_to_schema_info_sql_query_2,
            grant_jds_access_to_database_sql_query_2,
            grant_jds_access_to_schema_info_sql_query_3,
            grant_jds_access_to_schema_info_sql_query_4,
            grant_jds_access_to_database_sql_query_3,
            grant_jds_access_to_schema_info_sql_query_5,
            grant_jds_access_to_schema_info_sql_query_6,
            grant_sds_access_to_database_sql_query_1,
            grant_sds_access_to_schema_info_sql_query_1,
            grant_sds_access_to_schema_info_sql_query_2,
            grant_sds_access_to_database_sql_query_2,
            grant_sds_access_to_schema_info_sql_query_3,
            grant_sds_access_to_schema_info_sql_query_4,
            grant_sds_access_to_database_sql_query_3,
            grant_sds_access_to_schema_info_sql_query_5,
            grant_sds_access_to_schema_info_sql_query_6,
        )
        __grant_table_ownership(
            cursor_dwh,
            view_name,
            view_name2,
            grant_ownership_rights_to_sde,
            grant_ownership_rights_to_sde2,
            dwh_db,
            dwh_reporting_schema,
        )
        __grant_privileges(
            cursor_dwh,
            grant_privileges_for_jda_sql_query,
            grant_privileges_for_sda_sql_query,
            dwh_db,
            dwh_reporting_schema,
        )

    except Exception as e:
        root_logger.error(e)
    finally:
        # Close the cursor if it exists
        if cursor_dwh is not None:
            cursor_dwh.close()
            root_logger.debug("")
            root_logger.debug("Cursor closed successfully.")

        # Close the database connection to Postgres if it exists
        if postgres_connection_dwh is not None:
            postgres_connection_dwh.close()
            # root_logger.debug("")
            root_logger.debug("Session connected to Postgres database closed.")

        if cursor_raw is not None:
            cursor_raw.close()
            root_logger.debug("")
            root_logger.debug("Cursor closed successfully.")

        # Close the database connection to Postgres if it exists
        if postgres_connection_raw is not None:
            postgres_connection_raw.close()
            # root_logger.debug("")
            root_logger.debug("Session connected to Postgres database closed.")

        if cursor_stg is not None:
            cursor_stg.close()
            root_logger.debug("")
            root_logger.debug("Cursor closed successfully.")

        # Close the database connection to Postgres if it exists
        if postgres_connection_stg is not None:
            postgres_connection_stg.close()
            # root_logger.debug("")
            root_logger.debug("Session connected to Postgres database closed.")

        if cursor_semantic is not None:
            cursor_semantic.close()
            root_logger.debug("")
            root_logger.debug("Cursor closed successfully.")

        # Close the database connection to Postgres if it exists
        if postgres_connection_semantic is not None:
            postgres_connection_semantic.close()
            # root_logger.debug("")
            root_logger.debug("Session connected to Postgres database closed.")
    root_logger.info("Ending dwh_create_indexes process...")


def perform_dwh_access_control() -> None:
    """Create permissions and access to roles"""
    __set_up_access_control()
