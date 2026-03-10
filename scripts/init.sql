-- ============================================================
-- ObRail Europe – PostgreSQL Schema Init – Couche Gold
-- Exécuté AVANT le chargement JDBC (pas d'index ici → bulk insert rapide)
-- Les index, vues et ANALYZE sont créés dans post_load.sql
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- 1. TABLES GOLD
-- ============================================================

-- ------------------------------------------------------------
-- 1.1 GOLD_ROUTES  (train + avion agglomérés)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS gold_routes CASCADE;

CREATE TABLE gold_routes (
    source                   TEXT,
    trip_id                  TEXT,
    mode                     TEXT            NOT NULL,

    -- Voyage
    destination              TEXT,
    trip_short_name          TEXT,
    agency_name              TEXT,
    agency_timezone          TEXT,
    service_id               TEXT,
    route_id                 TEXT,
    route_type               TEXT,
    route_short_name         TEXT,
    route_long_name          TEXT,

    -- Départ
    departure_station        TEXT,
    departure_city           TEXT,
    departure_country        CHAR(2)         NOT NULL DEFAULT 'XX',
    departure_time           TEXT,
    departure_parent_station TEXT,

    -- Arrivée
    arrival_station          TEXT,
    arrival_city             TEXT,
    arrival_country          CHAR(2),
    arrival_time             TEXT,
    arrival_parent_station   TEXT,

    -- Service
    service_start_date       TEXT,
    service_end_date         TEXT,
    days_of_week             CHAR(7),

    -- Métriques
    is_night_train           BOOLEAN,
    distance_km              DOUBLE PRECISION,
    co2_per_pkm              DOUBLE PRECISION,
    emissions_co2            DOUBLE PRECISION

) PARTITION BY LIST (departure_country);

-- ------------------------------------------------------------
-- 1.2 GOLD_COMPARE_BEST  (comparaison train vs avion par O/D)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS gold_compare_best CASCADE;

CREATE TABLE gold_compare_best (
    source                      TEXT,
    trip_id                     TEXT,

    -- O/D
    departure_station           TEXT,
    departure_city              TEXT,
    departure_country           CHAR(2)          NOT NULL DEFAULT 'XX',
    arrival_station             TEXT,
    arrival_city                TEXT,
    arrival_country             CHAR(2),
    departure_parent_station    TEXT,
    arrival_parent_station      TEXT,
    days_of_week                CHAR(7),

    -- Métriques train (NULL si aucun train sur ce corridor)
    train_duration_min          DOUBLE PRECISION,
    train_distance_km           DOUBLE PRECISION,
    train_emissions_co2         DOUBLE PRECISION,

    -- Métriques avion (NULL si aucun vol sur ce corridor)
    flight_candidate_id         TEXT,
    flight_correspondence_count SMALLINT,
    flight_duration_min         DOUBLE PRECISION,
    flight_distance_km          DOUBLE PRECISION,
    flight_emissions_co2        DOUBLE PRECISION,

    -- Mode le plus écologique (émissions CO2 ↓, puis durée ↓)
    best_mode                   TEXT             NOT NULL

) PARTITION BY LIST (departure_country);

-- ============================================================
-- 2. PARTITIONS
-- ============================================================

DO $$
DECLARE
    countries TEXT[] := ARRAY[
        'FR','DE','CH','IT','ES','AT','BE','NL','PL','CZ',
        'SK','HU','RO','PT','SE','NO','DK','FI','GB','LU',
        'BG','HR','EE','GR','IE','LV','LT','MT','SI'
    ];
    c TEXT;
BEGIN
    FOREACH c IN ARRAY countries LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold_routes_%s
             PARTITION OF gold_routes FOR VALUES IN (%L)', c, c
        );
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold_compare_best_%s
             PARTITION OF gold_compare_best FOR VALUES IN (%L)', c, c
        );
    END LOOP;
END $$;

-- Partitions DEFAULT (pays non listés)
CREATE TABLE IF NOT EXISTS gold_routes_XX
    PARTITION OF gold_routes DEFAULT;

CREATE TABLE IF NOT EXISTS gold_compare_best_XX
    PARTITION OF gold_compare_best DEFAULT;

-- ============================================================
-- 3. COMMENTAIRES
-- ============================================================

COMMENT ON TABLE gold_routes IS
    'Couche gold ObRail – routes train + avion agglomérées. Partitionnée par departure_country.';

COMMENT ON TABLE gold_compare_best IS
    'Comparaison train vs avion par paire O/D – métriques côte à côte, best_mode = le plus écologique.';
