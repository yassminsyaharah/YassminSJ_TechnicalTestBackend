
-- STAGING REFERENSI : CSV yang di-load saat Extract
CREATE TABLE IF NOT EXISTS stg_realisasi_bus (
    tanggal_realisasi   DATE,
    bus_body_no         VARCHAR(20),   -- used as JOIN key
    rute_realisasi      VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS ref_routes (
    route_code  VARCHAR(10) PRIMARY KEY,
    route_name  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS ref_shelter_corridor (
    shelter_name_var VARCHAR(100) PRIMARY KEY,
    corridor_code    VARCHAR(10),
    corridor_name    VARCHAR(100)
);


-- BRONZE / L0 : Raw union dari bus + halte

CREATE TABLE IF NOT EXISTS stg_transaksi_raw (
    uuid               VARCHAR(36),
    sumber             VARCHAR(10),
    waktu_transaksi    TIMESTAMP,
    tanggal            DATE,
    lokasi_var         VARCHAR(100),
    card_number_var    VARCHAR(20),
    card_type_var      VARCHAR(30),
    balance_before_int INTEGER,
    fare_int           INTEGER,
    balance_after_int  INTEGER,
    transcode_txt      VARCHAR(20),
    gate_in_boo        BOOLEAN,
    p_latitude_flo     FLOAT,
    p_longitude_flo    FLOAT,
    status_var         VARCHAR(5),
    free_service_boo   BOOLEAN,
    insert_on_dtm      TIMESTAMP,
    etl_run_id         VARCHAR(50),
    etl_loaded_at      TIMESTAMP DEFAULT NOW()
);


-- SILVER / L1 : Cleaned, deduplicated, standardized

CREATE TABLE IF NOT EXISTS stg_transaksi_clean (
    uuid                    VARCHAR(36) PRIMARY KEY,
    sumber                  VARCHAR(10),
    waktu_transaksi         TIMESTAMP,
    tanggal                 DATE,
    lokasi_var              VARCHAR(100),
    lokasi_standardized     VARCHAR(20),  
    card_number_var         VARCHAR(20),
    card_type_var           VARCHAR(30),
    balance_before_int      INTEGER,
    fare_int                INTEGER,
    balance_after_int       INTEGER,
    gate_in_boo             BOOLEAN,
    status_var              VARCHAR(5),
    is_pelanggan            BOOLEAN,
    free_service_boo        BOOLEAN,
    etl_run_id              VARCHAR(50),
    etl_loaded_at           TIMESTAMP DEFAULT NOW()
);


-- GOLD / L2 : Cube aggregations

CREATE TABLE IF NOT EXISTS cube_by_card_type (
    tanggal             DATE,
    card_type_var       VARCHAR(30),
    gate_in_boo         BOOLEAN,
    jumlah_pelanggan    INTEGER,
    total_amount        BIGINT,
    etl_run_id          VARCHAR(50),
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tanggal, card_type_var, gate_in_boo)
);

CREATE TABLE IF NOT EXISTS cube_by_route (
    tanggal             DATE,
    route_code          VARCHAR(20),
    route_name          VARCHAR(100),
    gate_in_boo         BOOLEAN,
    jumlah_pelanggan    INTEGER,
    total_amount        BIGINT,
    etl_run_id          VARCHAR(50),
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tanggal, route_code, gate_in_boo)
);

CREATE TABLE IF NOT EXISTS cube_by_tarif (
    tanggal             DATE,
    fare_int            INTEGER,
    gate_in_boo         BOOLEAN,
    jumlah_pelanggan    INTEGER,
    total_amount        BIGINT,
    etl_run_id          VARCHAR(50),
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tanggal, fare_int, gate_in_boo)
);
