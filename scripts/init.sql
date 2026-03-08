-- ============================================================
-- ObRail Europe – PostgreSQL Init Script – Couche Gold
-- Tables : gold_routes, gold_compare_candidates, gold_compare_best
-- Partitionnement : par departure_country (CHAR 2)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- recherche textuelle partielle

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
    mode                     TEXT            NOT NULL,         -- 'train' | 'flight'

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
    departure_time           TEXT,           -- HH:MM:SS (stocké en TEXT, GTFS peut dépasser 24h)
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
    days_of_week             CHAR(7),        -- masque binaire Lun-Dim (ex: "1111100")

    -- Métriques
    is_night_train           BOOLEAN,
    distance_km              DOUBLE PRECISION,
    co2_per_pkm              DOUBLE PRECISION,
    emissions_co2            DOUBLE PRECISION

) PARTITION BY LIST (departure_country);

-- Variantes par mode (non partitionnées, vues simples)
DROP TABLE IF EXISTS gold_routes_train CASCADE;
CREATE TABLE gold_routes_train (LIKE gold_routes);

DROP TABLE IF EXISTS gold_routes_flight CASCADE;
CREATE TABLE gold_routes_flight (LIKE gold_routes);

-- ------------------------------------------------------------
-- 1.2 GOLD_COMPARE_CANDIDATES  (tous les candidats comparaison)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS gold_compare_candidates CASCADE;

CREATE TABLE gold_compare_candidates (
    source                   TEXT,
    trip_id                  TEXT,

    -- O/D (référence train)
    departure_station        TEXT,
    departure_city           TEXT,
    departure_country        CHAR(2)         NOT NULL DEFAULT 'XX',
    arrival_station          TEXT,
    arrival_city             TEXT,
    arrival_country          CHAR(2),
    departure_parent_station TEXT,
    arrival_parent_station   TEXT,
    days_of_week             CHAR(7),

    -- Candidat
    candidate_mode           TEXT            NOT NULL,  -- 'train' | 'flight'
    candidate_id             TEXT,
    correspondence_count     SMALLINT        NOT NULL DEFAULT 0,
    duration_min             DOUBLE PRECISION,
    comparison_distance_km   DOUBLE PRECISION,
    comparison_emissions_co2 DOUBLE PRECISION

) PARTITION BY LIST (departure_country);

-- ------------------------------------------------------------
-- 1.3 GOLD_COMPARE_BEST  (meilleure option par O/D)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS gold_compare_best CASCADE;

CREATE TABLE gold_compare_best (
    source                   TEXT,
    trip_id                  TEXT,

    departure_station        TEXT,
    departure_city           TEXT,
    departure_country        CHAR(2)         NOT NULL DEFAULT 'XX',
    arrival_station          TEXT,
    arrival_city             TEXT,
    arrival_country          CHAR(2),
    departure_parent_station TEXT,
    arrival_parent_station   TEXT,
    days_of_week             CHAR(7),

    candidate_mode           TEXT            NOT NULL,
    candidate_id             TEXT,
    correspondence_count     SMALLINT        NOT NULL DEFAULT 0,
    duration_min             DOUBLE PRECISION,
    comparison_distance_km   DOUBLE PRECISION,
    comparison_emissions_co2 DOUBLE PRECISION

) PARTITION BY LIST (departure_country);

-- ============================================================
-- 2. PARTITIONS (les 3 tables partitionnées partagent le même schéma)
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
        -- gold_routes
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold_routes_%s
             PARTITION OF gold_routes FOR VALUES IN (%L)',
            c, c
        );
        -- gold_compare_candidates
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold_compare_candidates_%s
             PARTITION OF gold_compare_candidates FOR VALUES IN (%L)',
            c, c
        );
        -- gold_compare_best
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold_compare_best_%s
             PARTITION OF gold_compare_best FOR VALUES IN (%L)',
            c, c
        );
    END LOOP;
END $$;

-- Partitions DEFAULT (fallback pour les pays non listés)
CREATE TABLE IF NOT EXISTS gold_routes_XX
    PARTITION OF gold_routes DEFAULT;

CREATE TABLE IF NOT EXISTS gold_compare_candidates_XX
    PARTITION OF gold_compare_candidates DEFAULT;

CREATE TABLE IF NOT EXISTS gold_compare_best_XX
    PARTITION OF gold_compare_best DEFAULT;

-- ============================================================
-- 3. INDEX – alignés sur les endpoints FastAPI
-- ============================================================

-- -----------------------------------------------------------------
-- GET /routes/search  — pays départ + ville O/D + mode
-- -----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_routes_od
    ON gold_routes (departure_country, departure_city, arrival_city);

CREATE INDEX IF NOT EXISTS idx_routes_mode_country
    ON gold_routes (mode, departure_country);

-- -----------------------------------------------------------------
-- GET /routes/night-trains  — trains de nuit par pays
-- -----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_routes_night
    ON gold_routes (departure_country, is_night_train)
    WHERE is_night_train = TRUE;

-- -----------------------------------------------------------------
-- GET /routes/by-country  — trafic transfrontalier
-- -----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_routes_country_pair
    ON gold_routes (departure_country, arrival_country);

-- -----------------------------------------------------------------
-- GET /routes/co2  — émissions par pays/mode
-- -----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_routes_co2
    ON gold_routes (departure_country, mode, emissions_co2);

-- -----------------------------------------------------------------
-- GET /routes/{trip_id}  — lookup direct
-- -----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_routes_trip_id
    ON gold_routes (trip_id);

-- -----------------------------------------------------------------
-- GET /routes/compare  — comparaison par O/D
-- -----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_cmp_candidates_od
    ON gold_compare_candidates (departure_country, departure_city, arrival_city);

CREATE INDEX IF NOT EXISTS idx_cmp_best_od
    ON gold_compare_best (departure_country, departure_city, arrival_city);

CREATE INDEX IF NOT EXISTS idx_cmp_best_mode
    ON gold_compare_best (candidate_mode, departure_country);

-- -----------------------------------------------------------------
-- Recherche full-text sur les noms de villes (trgm)
-- -----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_routes_dep_city_trgm
    ON gold_routes USING GIN (departure_city gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_routes_arr_city_trgm
    ON gold_routes USING GIN (arrival_city gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_cmp_best_dep_city_trgm
    ON gold_compare_best USING GIN (departure_city gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_cmp_best_arr_city_trgm
    ON gold_compare_best USING GIN (arrival_city gin_trgm_ops);

-- ============================================================
-- 4. VUES – raccourcis API
-- ============================================================

-- Vue : routes train uniquement
CREATE OR REPLACE VIEW v_routes_train AS
    SELECT * FROM gold_routes WHERE mode = 'train';

-- Vue : routes avion uniquement
CREATE OR REPLACE VIEW v_routes_flight AS
    SELECT * FROM gold_routes WHERE mode = 'flight';

-- Vue : meilleure option écologique par O/D
--       (la plus faible émission CO2 parmi les candidats comparaison)
CREATE OR REPLACE VIEW v_best_green_option AS
    SELECT DISTINCT ON (departure_city, departure_country, arrival_city, arrival_country)
        departure_city,
        departure_country,
        arrival_city,
        arrival_country,
        candidate_mode     AS best_mode,
        comparison_emissions_co2 AS min_co2,
        duration_min,
        comparison_distance_km AS distance_km
    FROM gold_compare_best
    ORDER BY departure_city, departure_country, arrival_city, arrival_country,
             comparison_emissions_co2 ASC NULLS LAST;

-- Vue : trains de nuit disponibles (avec pays départ/arrivée)
CREATE OR REPLACE VIEW v_night_trains AS
    SELECT
        trip_id, source, agency_name,
        departure_station, departure_city, departure_country, departure_time,
        arrival_station,   arrival_city,   arrival_country,   arrival_time,
        days_of_week,
        distance_km, emissions_co2
    FROM gold_routes
    WHERE mode = 'train'
      AND is_night_train = TRUE;

-- ============================================================
-- 5. COMMENTAIRES
-- ============================================================

COMMENT ON TABLE gold_routes IS
    'Couche gold ObRail – routes train + avion agglomérées. Partitionnée par departure_country.';

COMMENT ON TABLE gold_compare_candidates IS
    'Tous les candidats de comparaison train vs avion pour chaque O/D train.';

COMMENT ON TABLE gold_compare_best IS
    'Meilleure option (train ou avion) par paire O/D selon durée puis émissions.';

COMMENT ON VIEW v_routes_train IS 'Filtre gold_routes sur mode=train.';
COMMENT ON VIEW v_routes_flight IS 'Filtre gold_routes sur mode=flight.';
COMMENT ON VIEW v_best_green_option IS 'Option la plus verte (CO2 min) par O/D.';
COMMENT ON VIEW v_night_trains IS 'Trains de nuit européens actifs.';
