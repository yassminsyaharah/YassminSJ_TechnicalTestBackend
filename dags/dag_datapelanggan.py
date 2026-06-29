import logging
import pendulum
import sqlalchemy
from datetime import timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import sys
sys.path.insert(0, "/opt/airflow/plugins")
from etl_pipeline_factory import ETLPipelineFactory

DB_URL     = "postgresql+psycopg2://dwuser:dwpassword@postgres_dw:5432/transjakarta_dw"
DATA_DIR   = "/opt/airflow/data/input"
OUTPUT_DIR = "/opt/airflow/data/output"
SQL_DIR    = "/opt/airflow/sql"
logger = logging.getLogger(__name__)


def task_extract(**context):
    logger.info("[EXTRACT] Start")
    run_id  = context["run_id"]
    factory = ETLPipelineFactory(DB_URL, DATA_DIR, OUTPUT_DIR)
    df_bus   = factory.get_bus_extractor().extract()
    df_halte = factory.get_halte_extractor().extract()
    factory.get_loader_bronze().load(df_bus, df_halte, run_id)
    ref    = factory.get_reference_extractor()
    loader = factory.get_reference_loader()
    loader.load_routes(ref.extract_routes())
    loader.load_realisasi(ref.extract_realisasi_bus())
    loader.load_shelter_corridor(ref.extract_shelter_corridor())
    logger.info("[EXTRACT] Done")


def task_transform_silver(**context):
    run_id = context["run_id"]
    sql = Path(f"{SQL_DIR}/03_transform_silver.sql").read_text()
    sql = sql.replace("'{{ run_id }}'", f"'{run_id}'")
    engine = sqlalchemy.create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(sql))
    logger.info("[SILVER] Done")


def task_transform_gold(**context):
    run_id = context["run_id"]
    sql = Path(f"{SQL_DIR}/04_transform_gold.sql").read_text()
    sql = sql.replace("'{{ run_id }}'", f"'{run_id}'")
    engine = sqlalchemy.create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(sql))
    logger.info("[GOLD] Done")


def task_export_csv(**context):
    logger.info("[EXPORT CSV] Start")
    factory = ETLPipelineFactory(DB_URL, DATA_DIR, OUTPUT_DIR)
    factory.get_loader_gold_csv().export_all()
    logger.info("[EXPORT CSV] Done")


default_args = {
    "owner": "transjakarta-de",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id       = "dag_datapelanggan",
    description  = "ELT Pipeline Transjakarta",
    schedule     = "0 0 * * *",
    start_date   = pendulum.datetime(2025, 7, 1, tz="Asia/Jakarta"),
    catchup      = False,
    default_args = default_args,
    tags         = ["transjakarta", "elt", "medallion"],
) as dag:

    extract = PythonOperator(
        task_id         = "extract",
        python_callable = task_extract,
        provide_context = True,
    )

    with TaskGroup(group_id="transform") as transform:
        silver = PythonOperator(
            task_id         = "silver",
            python_callable = task_transform_silver,
            provide_context = True,
        )
        gold = PythonOperator(
            task_id         = "gold",
            python_callable = task_transform_gold,
            provide_context = True,
        )
        silver >> gold

    with TaskGroup(group_id="load") as load:
        export_csv = PythonOperator(
            task_id         = "export_csv",
            python_callable = task_export_csv,
            provide_context = True,
        )

    extract >> transform >> load