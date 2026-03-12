-- ============================================================
-- ObRail Europe – Post-load : Index & ANALYZE
-- Exécuté APRÈS le chargement JDBC (données déjà en base).
--
-- Tous les index utilisent IF NOT EXISTS → idempotent (safe sur re-run).
-- La table est partitionnée par departure_country : les index sur la
-- table parent sont propagés automatiquement à toutes les partitions.
-- ============================================================

-- Optimisations de session pour accélérer la création d'index
-- Allocation de 10GB de RAM et jusqu'à 12 cœurs en parallèle
SET maintenance_work_mem = '10GB';
SET max_parallel_maintenance_workers = 12;
SET max_parallel_workers = 12;
SET synchronous_commit = off;

-- pg_trgm est déjà activé par init.sql, mais on le réaffirme pour
-- permettre l'exécution de ce script de façon autonome.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- 1. INDEX BTREE – gold_routes
-- ============================================================

-- Recherche par ville (endpoints les plus fréquents : /cities, /stations, /search)
CREATE INDEX IF NOT EXISTS idx_routes_dep_city
    ON gold_routes (departure_city);

CREATE INDEX IF NOT EXISTS idx_routes_arr_city
    ON gold_routes (arrival_city);

-- Filtres combinés fréquents (mode + nuit, mode + pays de départ)
CREATE INDEX IF NOT EXISTS idx_routes_mode_night
    ON gold_routes (mode, is_night_train);

CREATE INDEX IF NOT EXISTS idx_routes_mode_depcountry
    ON gold_routes (mode, departure_country);

-- Endpoint /carbon/factors (/carbon/facteur) : groupement des facteurs CO2
-- par pays/mode/nuit avec comptage des trajets concernés.
CREATE INDEX IF NOT EXISTS idx_routes_factors_by_country_mode_night
    ON gold_routes (departure_country, mode, is_night_train, co2_per_pkm)
    WHERE co2_per_pkm IS NOT NULL;

-- Lookup direct par trip_id (/routes/{trip_id}, /carbon/trip/{trip_id})
-- Index composite (trip_id, source) : les deux filtres sont toujours utilisés ensemble
CREATE INDEX IF NOT EXISTS idx_routes_trip_id_source
    ON gold_routes (trip_id, source);

-- ============================================================
-- 2. INDEX BTREE – gold_compare_best
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_compare_dep_city
    ON gold_compare_best (departure_city);

CREATE INDEX IF NOT EXISTS idx_compare_arr_city
    ON gold_compare_best (arrival_city);

-- Filtre /compare?best_mode=train|flight, /carbon/ranking
CREATE INDEX IF NOT EXISTS idx_compare_best_mode
    ON gold_compare_best (best_mode);

-- Jointure avec gold_routes via trip_id
CREATE INDEX IF NOT EXISTS idx_compare_trip_id
    ON gold_compare_best (trip_id);

-- ============================================================
-- 3. INDEX GIN TRIGRAM – recherches ILIKE (autocomplétion)
-- Ces index accélèrent tous les ILIKE '%…%' sur les colonnes texte.
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_routes_dep_city_trgm
    ON gold_routes USING gin (departure_city gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_routes_arr_city_trgm
    ON gold_routes USING gin (arrival_city gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_routes_dep_station_trgm
    ON gold_routes USING gin (departure_station gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_routes_arr_station_trgm
    ON gold_routes USING gin (arrival_station gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_routes_agency_trgm
    ON gold_routes USING gin (agency_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_compare_dep_city_trgm
    ON gold_compare_best USING gin (departure_city gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_compare_arr_city_trgm
    ON gold_compare_best USING gin (arrival_city gin_trgm_ops);

-- ============================================================
-- 4. ANALYZE – met à jour les statistiques du planificateur
-- À exécuter après chaque chargement bulk pour des plans optimaux.
-- ============================================================

ANALYZE gold_routes;
ANALYZE gold_compare_best;
