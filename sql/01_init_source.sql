CREATE TABLE IF NOT EXISTS dummy_transaksi_bus (
    uuid               VARCHAR(36) PRIMARY KEY,
    waktu_transaksi    TIMESTAMP,
    armada_id_var      VARCHAR(20),
    no_body_var        VARCHAR(20),
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
    insert_on_dtm      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dummy_transaksi_halte (
    uuid               VARCHAR(36) PRIMARY KEY,
    waktu_transaksi    TIMESTAMP,
    shelter_name_var   VARCHAR(100),
    terminal_name_var  VARCHAR(100),
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
    insert_on_dtm      TIMESTAMP
);
