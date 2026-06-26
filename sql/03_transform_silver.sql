-- Yang dilakukan:
--   1. Deduplication by uuid (DISTINCT ON)
--   2. Standardisasi no_body_var ke format XXX-000 (SQL regex)
--   3. Flag is_pelanggan (status_var = 'S')
--   4. Upsert ke stg_transaksi_clean

INSERT INTO stg_transaksi_clean (
    uuid,
    sumber,
    waktu_transaksi,
    tanggal,
    lokasi_var,
    lokasi_standardized,
    card_number_var,
    card_type_var,
    balance_before_int,
    fare_int,
    balance_after_int,
    gate_in_boo,
    status_var,
    is_pelanggan,
    free_service_boo,
    etl_run_id,
    etl_loaded_at
)
SELECT
    uuid,
    sumber,
    waktu_transaksi,
    tanggal,
    lokasi_var,

    CASE
        WHEN sumber = 'BUS' THEN
            CONCAT(
                UPPER(LEFT(REGEXP_REPLACE(lokasi_var, '[^A-Za-z]', '', 'g'), 3)),
                '-',
                LPAD(LEFT(REGEXP_REPLACE(lokasi_var, '[^0-9]', '', 'g'), 3), 3, '0')
            )
        ELSE lokasi_var
    END AS lokasi_standardized,

    card_number_var,
    card_type_var,
    balance_before_int,
    fare_int,
    balance_after_int,
    gate_in_boo,
    status_var,
    (status_var = 'S')          AS is_pelanggan,
    free_service_boo,
    '{{ run_id }}'              AS etl_run_id,
    NOW()                       AS etl_loaded_at

FROM (
    SELECT DISTINCT ON (uuid) *
    FROM stg_transaksi_raw
    WHERE etl_run_id = '{{ run_id }}'
    ORDER BY uuid, insert_on_dtm ASC
) deduplicated

ON CONFLICT (uuid) DO UPDATE SET
    lokasi_standardized = EXCLUDED.lokasi_standardized,
    is_pelanggan        = EXCLUDED.is_pelanggan,
    etl_run_id          = EXCLUDED.etl_run_id,
    etl_loaded_at       = EXCLUDED.etl_loaded_at;
