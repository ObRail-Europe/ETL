"""
Section 7 – Qualité des données
GET /quality/completeness, /quality/completeness/by-country
GET /quality/coverage/countries, /quality/coverage/cities
GET /quality/schedules, /quality/compare-coverage
GET /quality/day-night-balance, /quality/summary

Section 8 – Statistiques & Agrégations
GET /stats/operators, /stats/distances, /stats/emissions-by-distance
"""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db

router = APIRouter(tags=["Qualité & Statistiques"])


async def _fetchall_dict(db: AsyncSession, sql: str, params: dict) -> list[dict]:
    result = await db.execute(text(sql), params)
    return [dict(r) for r in result.mappings().fetchall()]


async def _fetchone_dict(db: AsyncSession, sql: str, params: dict) -> dict:
    result = await db.execute(text(sql), params)
    row = result.mappings().fetchone()
    return dict(row) if row else {}


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 – QUALITÉ
# ═══════════════════════════════════════════════════════════════════════════════

# ── 7.1 GET /quality/completeness ────────────────────────────────────────────

@router.get("/quality/completeness", summary="Complétude globale des colonnes")
async def quality_completeness(db: AsyncSession = Depends(get_db)):
    sql = """
        SELECT
            COUNT(*) AS total_rows,
            ROUND(100.0 * COUNT(source)               / COUNT(*), 2) AS source_pct,
            ROUND(100.0 * COUNT(trip_id)              / COUNT(*), 2) AS trip_id_pct,
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
            ROUND(100.0 * COUNT(arrival_parent_station)  / COUNT(*), 2) AS arrival_parent_station_pct,
            ROUND(100.0 * COUNT(service_start_date)   / COUNT(*), 2) AS service_start_date_pct,
            ROUND(100.0 * COUNT(service_end_date)     / COUNT(*), 2) AS service_end_date_pct,
            ROUND(100.0 * COUNT(days_of_week)         / COUNT(*), 2) AS days_of_week_pct,
            ROUND(100.0 * COUNT(is_night_train)       / COUNT(*), 2) AS is_night_train_pct,
            ROUND(100.0 * COUNT(distance_km)          / COUNT(*), 2) AS distance_km_pct,
            ROUND(100.0 * COUNT(co2_per_pkm)          / COUNT(*), 2) AS co2_per_pkm_pct,
            ROUND(100.0 * COUNT(emissions_co2)        / COUNT(*), 2) AS emissions_co2_pct
        FROM gold_routes
    """
    return await _fetchone_dict(db, sql, {})


# ── 7.2 GET /quality/completeness/by-country ─────────────────────────────────

@router.get("/quality/completeness/by-country", summary="Complétude par pays de départ")
async def quality_completeness_by_country(db: AsyncSession = Depends(get_db)):
    sql = """
        SELECT
            departure_country,
            COUNT(*) AS total_rows,
            ROUND(100.0 * COUNT(departure_city)  / COUNT(*), 2) AS departure_city_pct,
            ROUND(100.0 * COUNT(arrival_city)    / COUNT(*), 2) AS arrival_city_pct,
            ROUND(100.0 * COUNT(arrival_country) / COUNT(*), 2) AS arrival_country_pct,
            ROUND(100.0 * COUNT(days_of_week)    / COUNT(*), 2) AS days_of_week_pct,
            ROUND(100.0 * COUNT(distance_km)     / COUNT(*), 2) AS distance_km_pct,
            ROUND(100.0 * COUNT(emissions_co2)   / COUNT(*), 2) AS emissions_co2_pct,
            ROUND(100.0 * COUNT(is_night_train)  / COUNT(*), 2) AS is_night_train_pct,
            ROUND(100.0 * COUNT(trip_short_name) / COUNT(*), 2) AS trip_short_name_pct
        FROM gold_routes
        WHERE mode = 'train'
        GROUP BY departure_country
        ORDER BY departure_country
    """
    data = await _fetchall_dict(db, sql, {})
    return {"status": "ok", "count": len(data), "data": data}


# ── 7.3 GET /quality/coverage/countries ──────────────────────────────────────

@router.get("/quality/coverage/countries", summary="Représentation des pays dans la base")
async def quality_coverage_countries(db: AsyncSession = Depends(get_db)):
    sql = """
        WITH cs AS (
            SELECT
                departure_country,
                COUNT(*)                                           AS total_routes,
                COUNT(CASE WHEN mode='train'  THEN 1 END)          AS train_routes,
                COUNT(CASE WHEN mode='flight' THEN 1 END)          AS flight_routes,
                COUNT(CASE WHEN is_night_train THEN 1 END)         AS night_routes,
                COUNT(CASE WHEN NOT is_night_train AND mode='train' THEN 1 END) AS day_routes,
                COUNT(DISTINCT agency_name)                        AS nb_agencies,
                COUNT(DISTINCT departure_city)                     AS nb_dep_cities,
                COUNT(DISTINCT arrival_country)                    AS nb_connected_countries
            FROM gold_routes GROUP BY departure_country
        )
        SELECT *,
            ROUND(100.0 * night_routes  / NULLIF(train_routes,  0), 1) AS night_pct,
            ROUND(100.0 * flight_routes / NULLIF(total_routes, 0),  1) AS flight_pct
        FROM cs ORDER BY total_routes DESC
    """
    data = await _fetchall_dict(db, sql, {})
    return {"status": "ok", "count": len(data), "data": data}


# ── 7.4 GET /quality/coverage/cities ─────────────────────────────────────────

@router.get("/quality/coverage/cities", summary="Couverture des villes (top N)")
async def quality_coverage_cities(
    departure_country: Optional[str] = Query(None),
    role: str = Query("departure", pattern="^(departure|arrival|both)$"),
    top_n: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    params = {"dep_country": departure_country, "top_n": top_n}
    if role == "departure":
        sql = """
            SELECT departure_city AS city, departure_country AS country,
                COUNT(*) AS total_routes,
                COUNT(CASE WHEN mode='train' AND NOT is_night_train THEN 1 END) AS day_train,
                COUNT(CASE WHEN mode='train' AND     is_night_train THEN 1 END) AS night_train,
                COUNT(CASE WHEN mode='flight' THEN 1 END)                        AS flight,
                COUNT(DISTINCT arrival_country) AS nb_destination_countries
            FROM gold_routes
            WHERE departure_city IS NOT NULL
              AND (:dep_country IS NULL OR departure_country = :dep_country)
            GROUP BY departure_city, departure_country
            ORDER BY total_routes DESC LIMIT :top_n
        """
    else:
        sql = """
            SELECT arrival_city AS city, arrival_country AS country,
                COUNT(*) AS total_routes,
                COUNT(CASE WHEN mode='train' AND NOT is_night_train THEN 1 END) AS day_train,
                COUNT(CASE WHEN mode='train' AND     is_night_train THEN 1 END) AS night_train,
                COUNT(CASE WHEN mode='flight' THEN 1 END)                        AS flight,
                COUNT(DISTINCT departure_country) AS nb_origin_countries
            FROM gold_routes
            WHERE arrival_city IS NOT NULL
              AND (:dep_country IS NULL OR arrival_country = :dep_country)
            GROUP BY arrival_city, arrival_country
            ORDER BY total_routes DESC LIMIT :top_n
        """
    data = await _fetchall_dict(db, sql, params)
    return {"status": "ok", "count": len(data), "data": data}


# ── 7.5 GET /quality/schedules ────────────────────────────────────────────────

@router.get("/quality/schedules", summary="Analyse des patterns horaires")
async def quality_schedules(db: AsyncSession = Depends(get_db)):
    sql = """
        SELECT
            departure_country,
            COUNT(CASE WHEN days_of_week = '1111100' THEN 1 END) AS weekday_only,
            COUNT(CASE WHEN days_of_week = '0000011' THEN 1 END) AS weekend_only,
            COUNT(CASE WHEN days_of_week = '1111111' THEN 1 END) AS daily,
            COUNT(CASE WHEN days_of_week IS NULL THEN 1 END)     AS no_schedule,
            COUNT(CASE WHEN days_of_week NOT IN ('1111100','0000011','1111111')
                          AND days_of_week IS NOT NULL THEN 1 END) AS other_pattern,
            COUNT(*) AS total,
            ROUND(100.0 * COUNT(CASE WHEN days_of_week IS NULL THEN 1 END) / COUNT(*), 2) AS null_schedule_pct
        FROM gold_routes WHERE mode = 'train'
        GROUP BY departure_country ORDER BY null_schedule_pct DESC
    """
    data = await _fetchall_dict(db, sql, {})
    return {"status": "ok", "count": len(data), "data": data}


# ── 7.6 GET /quality/compare-coverage ────────────────────────────────────────

@router.get("/quality/compare-coverage", summary="Couverture comparaison train/avion")
async def quality_compare_coverage(db: AsyncSession = Depends(get_db)):
    sql = """
        WITH tr AS (
            SELECT departure_country, COUNT(DISTINCT trip_id) AS total_train_trips
            FROM gold_routes WHERE mode='train' GROUP BY departure_country
        ),
        cp AS (
            SELECT departure_country,
                COUNT(*) AS compared_trips,
                COUNT(CASE WHEN best_mode='train'  THEN 1 END) AS train_wins,
                COUNT(CASE WHEN best_mode='flight' THEN 1 END) AS flight_wins,
                COUNT(CASE WHEN flight_candidate_id IS NOT NULL THEN 1 END) AS with_flight_equiv
            FROM gold_compare_best GROUP BY departure_country
        )
        SELECT
            t.departure_country,
            t.total_train_trips,
            COALESCE(c.compared_trips, 0) AS compared_trips,
            ROUND(100.0 * COALESCE(c.compared_trips,0) / NULLIF(t.total_train_trips,0), 2) AS compare_coverage_pct,
            COALESCE(c.with_flight_equiv, 0) AS with_flight_equiv,
            COALESCE(c.train_wins,  0) AS train_wins,
            COALESCE(c.flight_wins, 0) AS flight_wins,
            ROUND(100.0 * COALESCE(c.train_wins,0) / NULLIF(c.compared_trips,0), 1) AS train_win_pct
        FROM tr t LEFT JOIN cp c USING (departure_country)
        ORDER BY t.departure_country
    """
    data = await _fetchall_dict(db, sql, {})
    return {"status": "ok", "count": len(data), "data": data}


# ── 7.7 GET /quality/day-night-balance ───────────────────────────────────────

@router.get("/quality/day-night-balance", summary="Équilibre jour/nuit par corridor")
async def quality_daynight_balance(db: AsyncSession = Depends(get_db)):
    sql = """
        WITH cs AS (
            SELECT
                departure_country, arrival_country,
                COUNT(CASE WHEN NOT is_night_train THEN 1 END) AS day_count,
                COUNT(CASE WHEN     is_night_train THEN 1 END) AS night_count,
                COUNT(*) AS total
            FROM gold_routes
            WHERE mode='train' AND arrival_country IS NOT NULL
              AND departure_country != arrival_country
            GROUP BY departure_country, arrival_country
        )
        SELECT *,
            ROUND(100.0 * night_count / NULLIF(total, 0), 1) AS night_share_pct,
            CASE WHEN night_count=0 THEN 'day_only'
                 WHEN day_count=0   THEN 'night_only'
                 ELSE 'mixed' END AS service_type
        FROM cs ORDER BY total DESC
    """
    data = await _fetchall_dict(db, sql, {})
    return {"status": "ok", "count": len(data), "data": data}


# ── 7.8 GET /quality/summary ──────────────────────────────────────────────────

@router.get("/quality/summary", summary="Dashboard qualité synthétique")
async def quality_summary(db: AsyncSession = Depends(get_db)):
    sql = """
        SELECT
            (SELECT COUNT(*) FROM gold_routes)                                         AS total_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode='train')                      AS total_train,
            (SELECT COUNT(*) FROM gold_routes WHERE mode='flight')                     AS total_flight,
            (SELECT COUNT(*) FROM gold_compare_best)                                   AS total_comparisons,
            (SELECT COUNT(DISTINCT departure_country) FROM gold_routes)                AS countries_departure,
            (SELECT COUNT(DISTINCT arrival_country)   FROM gold_routes WHERE arrival_country IS NOT NULL) AS countries_arrival,
            (SELECT COUNT(DISTINCT departure_city)    FROM gold_routes WHERE departure_city  IS NOT NULL) AS unique_dep_cities,
            (SELECT COUNT(DISTINCT arrival_city)      FROM gold_routes WHERE arrival_city    IS NOT NULL) AS unique_arr_cities,
            (SELECT COUNT(DISTINCT agency_name)       FROM gold_routes WHERE agency_name     IS NOT NULL) AS unique_agencies,
            (SELECT COUNT(*) FROM gold_routes WHERE mode='train' AND is_night_train)   AS night_train_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode='train' AND NOT is_night_train) AS day_train_routes,
            (SELECT COUNT(*) FROM gold_routes WHERE mode='train' AND is_night_train IS NULL) AS unclassified_routes,
            (SELECT ROUND(100.0*COUNT(distance_km)/COUNT(*),2) FROM gold_routes)       AS distance_completeness_pct,
            (SELECT ROUND(100.0*COUNT(emissions_co2)/COUNT(*),2) FROM gold_routes)     AS emissions_completeness_pct,
            (SELECT ROUND(100.0*COUNT(days_of_week)/COUNT(*),2) FROM gold_routes)      AS schedule_completeness_pct,
            (SELECT ROUND(100.0*COUNT(departure_city)/COUNT(*),2) FROM gold_routes)    AS dep_city_completeness_pct,
            (SELECT ROUND(100.0*COUNT(arrival_city)/COUNT(*),2) FROM gold_routes)      AS arr_city_completeness_pct,
            (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode='train')           AS train_wins_total,
            (SELECT COUNT(*) FROM gold_compare_best WHERE best_mode='flight')          AS flight_wins_total
    """
    return await _fetchone_dict(db, sql, {})


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 – STATISTIQUES
# ═══════════════════════════════════════════════════════════════════════════════

# ── 8.1 GET /stats/operators ─────────────────────────────────────────────────

@router.get("/stats/operators", summary="Classement des opérateurs ferroviaires")
async def stats_operators(db: AsyncSession = Depends(get_db)):
    sql = """
        SELECT
            agency_name,
            COUNT(*)                            AS total_routes,
            COUNT(DISTINCT departure_country)   AS countries_served,
            COUNT(DISTINCT departure_city)      AS dep_cities,
            COUNT(DISTINCT arrival_city)        AS arr_cities,
            COUNT(CASE WHEN is_night_train THEN 1 END) AS night_routes,
            ROUND(AVG(distance_km)::numeric,  1) AS avg_distance_km,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
            ROUND(MIN(distance_km)::numeric,  1) AS min_distance_km,
            ROUND(MAX(distance_km)::numeric,  1) AS max_distance_km
        FROM gold_routes
        WHERE mode = 'train' AND agency_name IS NOT NULL
        GROUP BY agency_name
        ORDER BY total_routes DESC
    """
    data = await _fetchall_dict(db, sql, {})
    return {"status": "ok", "count": len(data), "data": data}


# ── 8.2 GET /stats/distances ─────────────────────────────────────────────────

@router.get("/stats/distances", summary="Distribution des distances par mode")
async def stats_distances(
    mode: Optional[str] = Query(None),
    departure_country: Optional[str] = Query(None),
    bucket_size: int = Query(100, ge=10, le=1000),
    db: AsyncSession = Depends(get_db),
):
    params = {"mode": mode, "dep_country": departure_country, "bucket": bucket_size}
    sql = """
        SELECT
            mode, is_night_train,
            FLOOR(distance_km / :bucket) * :bucket AS distance_bucket,
            COUNT(*) AS nb_routes,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2
        FROM gold_routes
        WHERE distance_km IS NOT NULL
          AND (:mode        IS NULL OR mode              = :mode)
          AND (:dep_country IS NULL OR departure_country = :dep_country)
        GROUP BY mode, is_night_train, distance_bucket
        ORDER BY mode, is_night_train, distance_bucket
    """
    data = await _fetchall_dict(db, sql, params)
    return {"status": "ok", "count": len(data), "data": data}


# ── 8.3 GET /stats/emissions-by-distance ─────────────────────────────────────

@router.get("/stats/emissions-by-distance", summary="Émissions moyennes par tranche de distance")
async def stats_emissions_by_distance(db: AsyncSession = Depends(get_db)):
    sql = """
        SELECT
            mode,
            FLOOR(distance_km / 100) * 100       AS distance_range_start,
            FLOOR(distance_km / 100) * 100 + 99  AS distance_range_end,
            COUNT(*) AS nb_routes,
            ROUND(AVG(co2_per_pkm)::numeric,   2) AS avg_co2_per_pkm,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_total_emissions
        FROM gold_routes
        WHERE distance_km IS NOT NULL AND emissions_co2 IS NOT NULL
        GROUP BY mode, FLOOR(distance_km / 100)
        ORDER BY mode, distance_range_start
    """
    data = await _fetchall_dict(db, sql, {})
    return {"status": "ok", "count": len(data), "data": data}
