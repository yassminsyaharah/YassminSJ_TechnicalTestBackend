import pandas as pd
import sqlalchemy
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SEED] %(message)s")
logger = logging.getLogger(__name__)

DW_CONN_DEFAULT_HOST = "postgresql+psycopg2://dwuser:dwpassword@localhost:5433/transjakarta_dw"
DB_URL = os.getenv("DW_CONN", DW_CONN_DEFAULT_HOST)
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "input")


def get_engine():
    return sqlalchemy.create_engine(DB_URL)


def seed_table(engine, csv_filename: str, table_name: str, dtype_map: dict):
    path = os.path.join(DATA_DIR, csv_filename)
    logger.info(f"Reading {csv_filename} ...")
    df = pd.read_csv(path)

    # Konversi tipe data
    for col, dtype in dtype_map.items():
        if col in df.columns:
            if dtype == "datetime":
                df[col] = pd.to_datetime(df[col])
            elif dtype == "bool":
                df[col] = df[col].astype(bool)
            elif dtype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    logger.info(f"Loading {len(df)} rows → {table_name} ...")

    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {table_name}"))
        df.to_sql(table_name, conn, if_exists="append", index=False)

    logger.info(f"{table_name} seeded ({len(df)} rows)")


def main():
    engine = get_engine()

    dtype_bus = {
        "waktu_transaksi": "datetime",
        "insert_on_dtm": "datetime",
        "gate_in_boo": "bool",
        "free_service_boo": "bool",
        "balance_before_int": "int",
        "fare_int": "int",
        "balance_after_int": "int",
    }

    dtype_halte = dtype_bus.copy()  # struktur kolom sama

    seed_table(engine, "dummy_transaksi_bus.csv",   "dummy_transaksi_bus",   dtype_bus)
    seed_table(engine, "dummy_transaksi_halte.csv", "dummy_transaksi_halte", dtype_halte)

    logger.info("=== Seeding complete ===")


if __name__ == "__main__":
    main()
