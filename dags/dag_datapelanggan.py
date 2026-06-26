import logging
import os
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

import sys
sys.path.insert(0, "/opt/airflow/plugins")
from etl_pipeline_factory import ETLPipelineFactory

# ── Config ────────────────────────────────────────────────────────────────────
DB_URL      = os.getenv(
    "DW_CONN_URL",
    "postgresql+psycopg2://dwuser:dwpassword@postgres_dw:5432/transjakarta_dw"
)
PG_CONN_ID  = "transjakarta_dw"   # Airflow Connection ID untuk PostgresOperator
DATA_DIR    = "/opt/airflow/data/input"
OUTPUT_DIR  = "/opt/airflow/data/output"

logger = logging.getLogger(__name__)


# EXTRACT
# Setelah extract, data di-load ke PostgreSQL staging agar semua transform berikutnya dikerjakan database

def task_extract(**context):
    logger.info("=" * 60)
    logger.info("[EXTRACT] Start — load sumber ke PostgreSQL staging")
    logger.info("=" * 60)

    run_id  = context["run_id"]
    factory = ETLPipelineFactory(DB_URL, DATA_DIR, OUTPUT_DIR)

    # 1. Extract + load transaksi ke Bronze
    df_bus   = factory.get_bus_extractor().extract()
    df_halte = factory.get_halte_extractor().extract()
    factory.get_loader_bronze().load(df_bus, df_halte, run_id)
    logger.info(f"[EXTRACT] Bronze: {len(df_bus)+len(df_halte)} rows -> stg_transaksi_raw")

    # 2. Extract + load CSV ke PostgreSQL
    ref    = factory.get_reference_extractor()
    loader = factory.get_reference_loader()
    loader.load_routes(ref.extract_routes())
    loader.load_realisasi(ref.extract_realisasi_bus())
    loader.load_shelter_corridor(ref.extract_shelter_corridor())

    logger.info("[EXTRACT] Semua data ada di PostgreSQL — siap di-transform via SQL")


def task_export_csv(**context):
    logger.info("[EXPORT CSV] Exporting Gold tables to CSV ...")
    factory = ETLPipelineFactory(DB_URL, DATA_DIR, OUTPUT_DIR)
    factory.get_loader_gold_csv().export_all()
    logger.info("[EXPORT CSV] Done")


# DAG DEFINITION

default_args = {
    "owner"           : "transjakarta-de",
    "depends_on_past" : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id       = "dag_datapelanggan",
    description  = "ELT Pipeline Transjakarta — SQL Pushdown, Medallion Architecture",
    schedule     = "0 0 * * *",    # 07:00 WIB = 00:00 UTC
    start_date   = pendulum.datetime(2025, 7, 1, tz="Asia/Jakarta"),
    catchup      = False,
    default_args = default_args,
    tags         = ["transjakarta", "elt", "medallion", "sql-pushdown"],
    template_searchpath = ["/opt/airflow/sql"],   # cari SQL files di sini
) as dag:

    extract = PythonOperator(
        task_id         = "extract",
        python_callable = task_extract,
        provide_context = True,
    )

    with TaskGroup(group_id="transform") as transform:

        transform_silver = PostgresOperator(
            task_id           = "silver",
            postgres_conn_id  = PG_CONN_ID,
            sql               = "03_transform_silver.sql",   # file di sql/
        )

        transform_gold = PostgresOperator(
            task_id           = "gold",
            postgres_conn_id  = PG_CONN_ID,
            sql               = "04_transform_gold.sql",
        )

        transform_silver >> transform_gold

    with TaskGroup(group_id="load") as load:

        export_csv = PythonOperator(
            task_id         = "export_csv",
            python_callable = task_export_csv,
            provide_context = True,
        )

    # ── Pipeline flow ─────────────────────────────────────────────
    extract >> transform >> load
