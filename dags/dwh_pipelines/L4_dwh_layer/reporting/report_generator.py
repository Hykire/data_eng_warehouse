import logging

import pandas as pd
import plotly.express as px
from geopandas import GeoDataFrame
from shapely.geometry import Point
from sqlalchemy import create_engine
from utils import get_constans

# Set up root root_logger
root_logger = logging.getLogger(__name__)

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
    active_schema_name="reporting",
    src_table_name="",
    table_name="",
    data_warehouse_layer="DWH - UAL",
)
# Set up root root_logger
root_logger = logging.getLogger(__name__)


def create_plots() -> None:
    """Perform creation of reports using plotly -> html files"""
    root_logger.info("Beginning create_plots process...")
    try:
        sql_query_1 = (
            f"""select * from {active_schema_name}.no_updates_per_client limit 10; """
        )
        sql_alchemy_engine = create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{database}"
        )
        sql_query_2 = f"""
                select * from {active_schema_name}.latest_updates_on_top1_client;
            """
        sql_alchemy_engine = create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{database}"
        )
        no_updates_per_client_df = pd.read_sql(sql_query_1, con=sql_alchemy_engine)

        fig = px.histogram(
            no_updates_per_client_df,
            x="n_events",
            y="data_id",
            marginal="box",
            color="data_id",
            hover_data=no_updates_per_client_df.columns,
        )
        t_min = no_updates_per_client_df.min_t.min()
        t_max = no_updates_per_client_df.max_t.max()
        fig.update_layout(
            title_text=f"Top 10 clients with most events between {t_min} and {t_max}",
            template="plotly_dark",
        )
        fig.show()
        fig.write_html(
            "/opt/airflow/dags/dwh_pipelines/L4_dwh_layer/reporting/no_updates_per_client.html"
        )
        #### second graph
        latest_movements_df = pd.read_sql(sql_query_2, con=sql_alchemy_engine)
        data = {}
        lst_points = []
        lst_id = []
        for _, row in latest_movements_df.iterrows():
            lst_id.append(row["data_id"])
            lst_points.append(Point(row["longitude"], row["latitude"]))
        data = {"id": lst_id, "coordinates": lst_points}
        df = pd.DataFrame.from_dict(data)
        new_df = GeoDataFrame(df, crs="EPSG:4326", geometry=df.coordinates)
        fig = px.scatter_geo(
            new_df, lat=new_df.geometry.y, lon=new_df.geometry.x, hover_name="id"
        )
        fig.update_layout(
            title_text=f"Latest 1h movement from id {lst_id[0]}",
            template="plotly_dark",
        )
        fig.write_html(
            "/opt/airflow/dags/dwh_pipelines/L4_dwh_layer/reporting/latest_updates_on_top1_client.html"
        )
    except Exception as e:
        root_logger.error(e)

    root_logger.info("Ending create_plots process...")
