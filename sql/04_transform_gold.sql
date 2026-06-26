
-- ── CUBE 1: by Card Type ─────────────────────────────────────────────────────
INSERT INTO cube_by_card_type (
    tanggal, card_type_var, gate_in_boo,
    jumlah_pelanggan, total_amount,
    etl_run_id, etl_loaded_at
)
SELECT
    tanggal,
    card_type_var,
    gate_in_boo,
    COUNT(uuid)        AS jumlah_pelanggan,
    SUM(fare_int)      AS total_amount,
    '{{ run_id }}'     AS etl_run_id,
    NOW()              AS etl_loaded_at
FROM stg_transaksi_clean
WHERE is_pelanggan = TRUE
GROUP BY tanggal, card_type_var, gate_in_boo

ON CONFLICT (tanggal, card_type_var, gate_in_boo) DO UPDATE SET
    jumlah_pelanggan = EXCLUDED.jumlah_pelanggan,
    total_amount     = EXCLUDED.total_amount,
    etl_run_id       = EXCLUDED.etl_run_id,
    etl_loaded_at    = EXCLUDED.etl_loaded_at;


--  CUBE 2: by Route
-- BUS:
--   stg_transaksi_clean (sumber=BUS)
--     JOIN stg_realisasi_bus ON lokasi_var = bus_body_no   
--     JOIN ref_routes ON route_code = rute_realisasi
-- HALTE:
--   stg_transaksi_clean (sumber=HALTE)
--     JOIN ref_shelter_corridor ON lokasi_var = shelter_name_var
--     JOIN ref_routes ON route_code = corridor_code
INSERT INTO cube_by_route (
    tanggal, route_code, route_name, gate_in_boo,
    jumlah_pelanggan, total_amount,
    etl_run_id, etl_loaded_at
)
SELECT
    tanggal, route_code, route_name, gate_in_boo,
    SUM(jumlah_pelanggan) AS jumlah_pelanggan,
    SUM(total_amount)     AS total_amount,
    '{{ run_id }}'        AS etl_run_id,
    NOW()                 AS etl_loaded_at
FROM (
    SELECT
        t.tanggal,
        r.route_code,
        r.route_name,
        t.gate_in_boo,
        COUNT(t.uuid)   AS jumlah_pelanggan,
        SUM(t.fare_int) AS total_amount
    FROM stg_transaksi_clean t
    JOIN stg_realisasi_bus rb
        ON t.lokasi_var = rb.bus_body_no
    JOIN ref_routes r
        ON r.route_code = rb.rute_realisasi
    WHERE t.is_pelanggan = TRUE
      AND t.sumber = 'BUS'
    GROUP BY t.tanggal, r.route_code, r.route_name, t.gate_in_boo

    UNION ALL

    SELECT
        t.tanggal,
        r.route_code,
        r.route_name,
        t.gate_in_boo,
        COUNT(t.uuid)   AS jumlah_pelanggan,
        SUM(t.fare_int) AS total_amount
    FROM stg_transaksi_clean t
    JOIN ref_shelter_corridor sc
        ON t.lokasi_var = sc.shelter_name_var
    JOIN ref_routes r
        ON r.route_code = sc.corridor_code
    WHERE t.is_pelanggan = TRUE
      AND t.sumber = 'HALTE'
    GROUP BY t.tanggal, r.route_code, r.route_name, t.gate_in_boo
) combined
GROUP BY tanggal, route_code, route_name, gate_in_boo

ON CONFLICT (tanggal, route_code, gate_in_boo) DO UPDATE SET
    route_name       = EXCLUDED.route_name,
    jumlah_pelanggan = EXCLUDED.jumlah_pelanggan,
    total_amount     = EXCLUDED.total_amount,
    etl_run_id       = EXCLUDED.etl_run_id,
    etl_loaded_at    = EXCLUDED.etl_loaded_at;


INSERT INTO cube_by_tarif (
    tanggal, fare_int, gate_in_boo,
    jumlah_pelanggan, total_amount,
    etl_run_id, etl_loaded_at
)
SELECT
    tanggal,
    fare_int,
    gate_in_boo,
    COUNT(uuid)        AS jumlah_pelanggan,
    SUM(fare_int)      AS total_amount,
    '{{ run_id }}'     AS etl_run_id,
    NOW()              AS etl_loaded_at
FROM stg_transaksi_clean
WHERE is_pelanggan = TRUE
GROUP BY tanggal, fare_int, gate_in_boo

ON CONFLICT (tanggal, fare_int, gate_in_boo) DO UPDATE SET
    jumlah_pelanggan = EXCLUDED.jumlah_pelanggan,
    total_amount     = EXCLUDED.total_amount,
    etl_run_id       = EXCLUDED.etl_run_id,
    etl_loaded_at    = EXCLUDED.etl_loaded_at;
