import logging
import pandas as pd
import sqlalchemy
from abc import ABC, abstractmethod
from datetime import datetime

logger = logging.getLogger(__name__)


# EXTRACTORS  

class TransaksiExtractor(ABC):
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        pass


class BusExtractor(TransaksiExtractor):
    """Extract transaksi bus dari PostgreSQL source."""

    def extract(self) -> pd.DataFrame:
        logger.info("[BusExtractor] Extracting dummy_transaksi_bus ...")
        df = pd.read_sql("SELECT * FROM dummy_transaksi_bus", self.engine)
        df["sumber"]     = "BUS"
        df["lokasi_var"] = df["no_body_var"]   # raw value, dipakai sbg join key ke stg_realisasi_bus
        df = df.drop(columns=["armada_id_var", "no_body_var"], errors="ignore")
        logger.info(f"[BusExtractor] {len(df)} rows")
        return df


class HalteExtractor(TransaksiExtractor):
    """Extract transaksi halte dari PostgreSQL source."""

    def extract(self) -> pd.DataFrame:
        logger.info("[HalteExtractor] Extracting dummy_transaksi_halte ...")
        df = pd.read_sql("SELECT * FROM dummy_transaksi_halte", self.engine)
        df["sumber"]     = "HALTE"
        df["lokasi_var"] = df["shelter_name_var"]
        df = df.drop(columns=["terminal_name_var", "shelter_name_var"], errors="ignore")
        logger.info(f"[HalteExtractor] {len(df)} rows")
        return df


class ReferenceExtractor:
    """Extract data referensi dari CSV"""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    def extract_routes(self) -> pd.DataFrame:
        df = pd.read_csv(f"{self.data_dir}/dummy_routes.csv")
        df["route_code"] = df["route_code"].astype(str).str.strip()
        logger.info(f"[ReferenceExtractor] routes: {len(df)} rows")
        return df

    def extract_shelter_corridor(self) -> pd.DataFrame:
        df = pd.read_csv(f"{self.data_dir}/dummy_shelter_corridor.csv")
        df["shelter_name_var"] = df["shelter_name_var"].astype(str).str.strip()
        df["corridor_code"]    = df["corridor_code"].astype(str).str.strip()
        logger.info(f"[ReferenceExtractor] shelter_corridor: {len(df)} rows")
        return df

    def extract_realisasi_bus(self) -> pd.DataFrame:
        df = pd.read_csv(f"{self.data_dir}/dummy_realisasi_bus.csv")
        df["tanggal_realisasi"] = pd.to_datetime(df["tanggal_realisasi"]).dt.date
        df["bus_body_no"]       = df["bus_body_no"].astype(str).str.strip()
        df = df[["tanggal_realisasi", "bus_body_no", "rute_realisasi"]]
        logger.info(f"[ReferenceExtractor] realisasi_bus: {len(df)} rows")
        return df


# LOADERS

class LoaderBronze:
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def load(self, df_bus: pd.DataFrame, df_halte: pd.DataFrame, etl_run_id: str):
        logger.info("[LoaderBronze] Loading raw data to stg_transaksi_raw ...")

        df = pd.concat([df_bus, df_halte], ignore_index=True)
        df["waktu_transaksi"] = pd.to_datetime(df["waktu_transaksi"])
        df["tanggal"]         = df["waktu_transaksi"].dt.date
        df["etl_run_id"]      = etl_run_id
        df["etl_loaded_at"]   = datetime.utcnow()

        bronze_cols = [
            "uuid", "sumber", "waktu_transaksi", "tanggal",
            "lokasi_var", "card_number_var", "card_type_var",
            "balance_before_int", "fare_int", "balance_after_int",
            "transcode_txt", "gate_in_boo", "p_latitude_flo", "p_longitude_flo",
            "status_var", "free_service_boo", "insert_on_dtm",
            "etl_run_id", "etl_loaded_at",
        ]
        df = df[[c for c in bronze_cols if c in df.columns]]

        with self.engine.begin() as conn:
            conn.execute(
                sqlalchemy.text("DELETE FROM stg_transaksi_raw WHERE etl_run_id = :run_id"),
                {"run_id": etl_run_id},
            )
            df.to_sql("stg_transaksi_raw", conn, if_exists="append", index=False)

        logger.info(f"[LoaderBronze] {len(df)} rows -> stg_transaksi_raw (run_id={etl_run_id})")


class ReferenceLoader:

    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def load_routes(self, df: pd.DataFrame):
        df.to_sql("ref_routes", self.engine, if_exists="replace", index=False)
        logger.info(f"[ReferenceLoader] ref_routes: {len(df)} rows")

    def load_shelter_corridor(self, df: pd.DataFrame):
        df.to_sql("ref_shelter_corridor", self.engine, if_exists="replace", index=False)
        logger.info(f"[ReferenceLoader] ref_shelter_corridor: {len(df)} rows")

    def load_realisasi(self, df: pd.DataFrame):
        df.to_sql("stg_realisasi_bus", self.engine, if_exists="replace", index=False)
        logger.info(f"[ReferenceLoader] stg_realisasi_bus: {len(df)} rows")


class LoaderGoldCSV:

    def __init__(self, engine: sqlalchemy.engine.Engine, output_dir: str):
        self.engine     = engine
        self.output_dir = output_dir

    def export_all(self):
        tables = {
            "output_by_card_type.csv": "SELECT * FROM cube_by_card_type ORDER BY tanggal, card_type_var",
            "output_by_route.csv"    : "SELECT * FROM cube_by_route     ORDER BY tanggal, route_code",
            "output_by_tarif.csv"    : "SELECT * FROM cube_by_tarif     ORDER BY tanggal, fare_int",
        }
        for filename, query in tables.items():
            df = pd.read_sql(query, self.engine)
            path = f"{self.output_dir}/{filename}"
            df.to_csv(path, index=False)
            logger.info(f"[LoaderGoldCSV] {filename}: {len(df)} rows -> {path}")


# FACTORY

class ETLPipelineFactory:

    def __init__(self, db_url: str, data_dir: str, output_dir: str):
        self.engine     = sqlalchemy.create_engine(db_url)
        self.data_dir   = data_dir
        self.output_dir = output_dir

    def get_bus_extractor(self)       -> BusExtractor:       return BusExtractor(self.engine)
    def get_halte_extractor(self)     -> HalteExtractor:     return HalteExtractor(self.engine)
    def get_reference_extractor(self) -> ReferenceExtractor: return ReferenceExtractor(self.data_dir)
    def get_loader_bronze(self)       -> LoaderBronze:       return LoaderBronze(self.engine)
    def get_reference_loader(self)    -> ReferenceLoader:    return ReferenceLoader(self.engine)
    def get_loader_gold_csv(self)     -> LoaderGoldCSV:      return LoaderGoldCSV(self.engine, self.output_dir)
