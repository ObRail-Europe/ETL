"""
Endpoints Qualité des Données (section 7 de la spec).

8 GET endpoints pour les métriques de complétude, couverture et qualité.
"""

from fastapi import APIRouter, Depends, Query

from ..database import get_db
from ..dependencies import pagination_params
from ..utils.query_helpers import execute_query, execute_single, execute_paginated_with_count

router = APIRouter()


# ── 7.1 GET /quality/completeness ─────────────────────────────────────────────

@router.get("/quality/completeness")
def quality_completeness(conn=Depends(get_db)):
    """Taux de complétude de chaque colonne de gold_routes."""
    # raw SQL: spec section 7.1
    query = """
        SELECT
            COUNT(*) AS total_rows,
            ROUND(100.0 * COUNT(source)               / COUNT(*), 2) AS source_pct,
            ROUND(100.0 * COUNT(trip_id)              / COUNT(*), 2) AS trip_id_pct,
            ROUND(100.0 * COUNT(destination)          / COUNT(*), 2) AS destination_pct,
            ROUND(100.0 * COUNT(trip_short_name)      / COUNT(*), 2) AS trip_short_name_pct,
            ROUND(100.0 * COUNT(agency_name)          / COUNT(*), 2) AS agency_name_pct,
            ROUND(100.0 * COUNT(agency_timezone)      / COUNT(*), 2) AS agency_timezone_pct,
            ROUND(100.0 * COUNT(service_id)           / COUNT(*), 2) AS service_id_pct,
            ROUND(100.0 * COUNT(route_id)             / COUNT(*), 2) AS route_id_pct,
            ROUND(100.0 * COUNT(route_type)           / COUNT(*), 2) AS route_type_pct,
            ROUND(100.0 * COUNT(route_short_name)     / COUNT(*), 2) AS route_short_name_pct,
            ROUND(100.0 * COUNT(route_long_name)      / COUNT(*), 2) AS route_long_name_pct,
            ROUND(100.0 * COUNT(departure_station)    / COUNT(*), 2) AS departure_station_pct,
            ROUND(100.0 * COUNT(departure_city)       / COUNT(*), 2) AS departure_city_pct,
            ROUND(100.0 * COUNT(departure_time)       / COUNT(*), 2) AS departure_time_pct,
            ROUND(100.0 * COUNT(departure_parent_station) / COUNT(*), 2) AS departure_parent_station_pct,
            ROUND(100.0 * COUNT(arrival_station)      / COUNT(*), 2) AS arrival_station_pct,
            ROUND(100.0 * COUNT(arrival_city)         / COUNT(*), 2) AS arrival_city_pct,
            ROUND(100.0 * COUNT(arrival_country)      / COUNT(*), 2) AS arrival_country_pct,
            ROUND(100.0 * COUNT(arrival_time)         / COUNT(*), 2) AS arrival_time_pct,
            ROUND(100.0 * COUNT(arrival_parent_station) / COUNT(*), 2) AS arrival_parent_station_pct,
            ROUND(100.0 * COUNT(service_start_date)   / COUNT(*), 2) AS service_start_date_pct,
            ROUND(100.0 * COUNT(service_end_date)     / COUNT(*), 2) AS service_end_date_pct,
            ROUND(100.0 * COUNT(days_of_week)         / COUNT(*), 2) AS days_of_week_pct,
            ROUND(100.0 * COUNT(is_night_train)       / COUNT(*), 2) AS is_night_train_pct,
            ROUND(100.0 * COUNT(distance_km)          / COUNT(*), 2) AS distance_km_pct,
            ROUND(100.0 * COUNT(co2_per_pkm)          / COUNT(*), 2) AS co2_per_pkm_pct,
            ROUND(100.0 * COUNT(emissions_co2)        / COUNT(*), 2) AS emissions_co2_pct
        FROM gold_routes
    """
    row = execute_single(conn, query)
    return {"status": "ok", "data": row}


# ── 7.2 GET /quality/completeness/by-country ──────────────────────────────────

@router.get("/quality/completeness/by-country")
def quality_completeness_by_country(conn=Depends(get_db)):
    """Complétude ventilée par pays de départ."""
    # raw SQL: spec section 7.2
    query = """
        SELECT
            departure_country,
            COUNT(*) AS total_rows,
            ROUND(100.0 * COUNT(departure_city)    / COUNT(*), 2) AS departure_city_pct,
            ROUND(100.0 * COUNT(arrival_city)      / COUNT(*), 2) AS arrival_city_pct,
            ROUND(100.0 * COUNT(arrival_country)   / COUNT(*), 2) AS arrival_country_pct,
            ROUND(100.0 * COUNT(days_of_week)      / COUNT(*), 2) AS days_of_week_pct,
            ROUND(100.0 * COUNT(distance_km)       / COUNT(*), 2) AS distance_km_pct,
            ROUND(100.0 * COUNT(emissions_co2)     / COUNT(*), 2) AS emissions_co2_pct,
            ROUND(100.0 * COUNT(is_night_train)    / COUNT(*), 2) AS is_night_train_pct,
            ROUND(100.0 * COUNT(trip_short_name)   / COUNT(*), 2) AS trip_short_name_pct
        FROM gold_routes
        WHERE mode = 'train'
        GROUP BY departure_country
        ORDER BY departure_country
    """
    rows = execute_query(conn, query)
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 7.3 GET /quality/coverage/countries ───────────────────────────────────────

@router.get("/quality/coverage/countries")
def quality_coverage_countries(conn=Depends(get_db)):
    """Représentation des pays dans les données."""
    # raw SQL: spec section 7.3
    query = """
        WITH country_stats AS (
            SELECT
                departure_country,
                COUNT(*)                                          AS total_routes,
                COUNT(CASE WHEN mode = 'train' THEN 1 END)       AS train_routes,
                COUNT(CASE WHEN mode = 'flight' THEN 1 END)      AS flight_routes,
                COUNT(CASE WHEN is_night_train = true THEN 1 END) AS night_routes,
                COUNT(CASE WHEN is_night_train = false AND mode = 'train' THEN 1 END) AS day_routes,
                COUNT(DISTINCT agency_name)                       AS nb_agencies,
                COUNT(DISTINCT departure_city)                    AS nb_dep_cities,
                COUNT(DISTINCT arrival_country)                   AS nb_connected_countries
            FROM gold_routes
            GROUP BY departure_country
        )
        SELECT *,
            ROUND(100.0 * night_routes / NULLIF(train_routes, 0), 1) AS night_pct,
            ROUND(100.0 * flight_routes / NULLIF(total_routes, 0), 1) AS flight_pct
        FROM country_stats
        ORDER BY total_routes DESC
    """
    rows = execute_query(conn, query)
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 7.4 GET /quality/coverage/cities ──────────────────────────────────────────

@router.get("/quality/coverage/cities")
def quality_coverage_cities(
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    role: str = Query("both", pattern="^(departure|arrival|both)$"),
    top_n: int = Query(50, ge=1, le=500),
    conn=Depends(get_db),
):
    """Villes les plus présentes dans la base."""
    # raw SQL: spec section 7.4
    # Les noms de colonnes sont des littéraux statiques — jamais dérivés de
    # l'input utilisateur — ce qui prévient toute injection d'identifiant SQL.
    params: list = []
    country_filter = ""
    if departure_country:
        params.append(departure_country.upper())
    params.append(top_n)

    if role == "arrival":
        if departure_country:
            country_filter = "AND arrival_country = %s"
        query = f"""
            SELECT
                arrival_city AS city,
                arrival_country AS country,
                COUNT(*) AS total_routes,
                COUNT(CASE WHEN mode = 'train' AND is_night_train = false THEN 1 END) AS day_train,
                COUNT(CASE WHEN mode = 'train' AND is_night_train = true  THEN 1 END) AS night_train,
                COUNT(CASE WHEN mode = 'flight' THEN 1 END)                           AS flight,
                COUNT(DISTINCT arrival_country) AS nb_destination_countries
            FROM gold_routes
            WHERE arrival_city IS NOT NULL
              {country_filter}
            GROUP BY arrival_city, arrival_country
            ORDER BY total_routes DESC
            LIMIT %s
        """
    else:
        if departure_country:
            country_filter = "AND departure_country = %s"
        query = f"""
            SELECT
                departure_city AS city,
                departure_country AS country,
                COUNT(*) AS total_routes,
                COUNT(CASE WHEN mode = 'train' AND is_night_train = false THEN 1 END) AS day_train,
                COUNT(CASE WHEN mode = 'train' AND is_night_train = true  THEN 1 END) AS night_train,
                COUNT(CASE WHEN mode = 'flight' THEN 1 END)                           AS flight,
                COUNT(DISTINCT arrival_country) AS nb_destination_countries
            FROM gold_routes
            WHERE departure_city IS NOT NULL
              {country_filter}
            GROUP BY departure_city, departure_country
            ORDER BY total_routes DESC
            LIMIT %s
        """
    rows = execute_query(conn, query, params)
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 7.5 GET /quality/schedules ────────────────────────────────────────────────

@router.get("/quality/schedules")
def quality_schedules(conn=Depends(get_db)):
    """Analyse de la couverture des jours de service."""
    # raw SQL: spec section 7.5
    query = """
        SELECT
            departure_country,
            COUNT(CASE WHEN days_of_week = '1111100' THEN 1 END) AS weekday_only,
            COUNT(CASE WHEN days_of_week = '0000011' THEN 1 END) AS weekend_only,
            COUNT(CASE WHEN days_of_week = '1111111' THEN 1 END) AS daily,
            COUNT(CASE WHEN days_of_week IS NULL THEN 1 END)     AS no_schedule,
            COUNT(CASE WHEN days_of_week NOT IN ('1111100','0000011','1111111')
                       AND days_of_week IS NOT NULL THEN 1 END)  AS other_pattern,
            COUNT(*) AS total,
            ROUND(100.0 * COUNT(CASE WHEN days_of_week IS NULL THEN 1 END) / COUNT(*), 2) AS null_schedule_pct
        FROM gold_routes
        WHERE mode = 'train'
        GROUP BY departure_country
        ORDER BY null_schedule_pct DESC
    """
    rows = execute_query(conn, query)
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 7.6 GET /quality/compare-coverage ─────────────────────────────────────────

@router.get("/quality/compare-coverage")
def quality_compare_coverage(conn=Depends(get_db)):
    """Taux de couverture de la comparaison train/avion."""
    # raw SQL: spec section 7.6
    query = """
        WITH train_routes AS (
            SELECT
                departure_country,
                COUNT(DISTINCT trip_id) AS total_train_trips
            FROM gold_routes
            WHERE mode = 'train'
            GROUP BY departure_country
        ),
        compared AS (
            SELECT
                departure_country,
                COUNT(*) AS compared_trips,
                COUNT(CASE WHEN best_mode = 'train'  THEN 1 END) AS train_wins,
                COUNT(CASE WHEN best_mode = 'flight' THEN 1 END) AS flight_wins,
                COUNT(CASE WHEN flight_candidate_id IS NOT NULL THEN 1 END) AS with_flight_equiv
            FROM gold_compare_best
            GROUP BY departure_country
        )
        SELECT
            t.departure_country,
            t.total_train_trips,
            COALESCE(c.compared_trips, 0) AS compared_trips,
            ROUND(100.0 * COALESCE(c.compared_trips, 0) / NULLIF(t.total_train_trips, 0), 2) AS compare_coverage_pct,
            COALESCE(c.with_flight_equiv, 0) AS with_flight_equiv,
            COALESCE(c.train_wins, 0) AS train_wins,
            COALESCE(c.flight_wins, 0) AS flight_wins,
            ROUND(100.0 * COALESCE(c.train_wins, 0) / NULLIF(c.compared_trips, 0), 1) AS train_win_pct
        FROM train_routes t
        LEFT JOIN compared c USING (departure_country)
        ORDER BY t.departure_country
    """
    rows = execute_query(conn, query)
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 7.7 GET /quality/day-night-balance ────────────────────────────────────────

@router.get("/quality/day-night-balance")
def quality_day_night_balance(conn=Depends(get_db)):
    """Équilibre entre offre jour et nuit par pays et par corridors."""
    # raw SQL: spec section 7.7
    query = """
        WITH corridor_stats AS (
            SELECT
                departure_country,
                arrival_country,
                COUNT(CASE WHEN is_night_train = false THEN 1 END) AS day_count,
                COUNT(CASE WHEN is_night_train = true  THEN 1 END) AS night_count,
                COUNT(*) AS total
            FROM gold_routes
            WHERE mode = 'train'
              AND arrival_country IS NOT NULL
              AND departure_country != arrival_country
            GROUP BY departure_country, arrival_country
        )
        SELECT *,
            ROUND(100.0 * night_count / NULLIF(total, 0), 1) AS night_share_pct,
            CASE
                WHEN night_count = 0 THEN 'day_only'
                WHEN day_count = 0   THEN 'night_only'
                ELSE 'mixed'
            END AS service_type
        FROM corridor_stats
        ORDER BY total DESC
    """
    rows = execute_query(conn, query)
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 7.8 GET /quality/summary ──────────────────────────────────────────────────

@router.get("/quality/summary")
def quality_summary(conn=Depends(get_db)):
    """Tableau de bord synthétique qualité."""
    # raw SQL: spec section 7.8
    query = """
        SELECT
            (SELECT COUNT(*) FROM gold_routes) AS total_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train') AS total_train,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'flight') AS total_flight,
            (SELECT COUNT(*) FROM gold_compare_best) AS total_comparisons,

            (SELECT COUNT(DISTINCT departure_country) FROM gold_routes) AS countries_departure,
            (SELECT COUNT(DISTINCT arrival_country) FROM gold_routes WHERE arrival_country IS NOT NULL) AS countries_arrival,
            (SELECT COUNT(DISTINCT departure_city) FROM gold_routes WHERE departure_city IS NOT NULL) AS unique_dep_cities,
            (SELECT COUNT(DISTINCT arrival_city) FROM gold_routes WHERE arrival_city IS NOT NULL) AS unique_arr_cities,
            (SELECT COUNT(DISTINCT agency_name) FROM gold_routes WHERE agency_name IS NOT NULL) AS unique_agencies,

            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train = true) AS night_train_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train = false) AS day_train_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode = 'train' AND is_night_train IS NULL) AS unclassified_routes,

            (SELECT ROUND(100.0 * COUNT(distance_km) / COUNT(*), 2) FROM gold_routes) AS distance_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(emissions_co2) / COUNT(*), 2) FROM gold_routes) AS emissions_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(days_of_week) / COUNT(*), 2) FROM gold_routes) AS schedule_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(departure_city) / COUNT(*), 2) FROM gold_routes) AS dep_city_completeness_pct,
            (SELECT ROUND(100.0 * COUNT(arrival_city) / COUNT(*), 2) FROM gold_routes) AS arr_city_completeness_pct,

            (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode = 'train') AS train_wins_total,
            (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode = 'flight') AS flight_wins_total
    """
    row = execute_single(conn, query)
    return {"status": "ok", "data": row}
