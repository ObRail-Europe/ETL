"""
Endpoints Référentiel — Villes, Gares, Aéroports (section 2 de la spec).

6 GET endpoints dérivant les données depuis gold_routes avec SELECT DISTINCT.
"""

from fastapi import APIRouter, Depends, Query

from ..database import get_db
from ..dependencies import pagination_params
from ..utils.query_helpers import (
    execute_query, execute_paginated_with_count, WhereBuilder,
)

router = APIRouter()


# ── 2.1 GET /cities ──────────────────────────────────────────────────────────

@router.get("/cities")
def list_cities(
    country: str | None = Query(None, min_length=2, max_length=2),
    search: str | None = Query(None, min_length=1, max_length=100),
    has_station: bool | None = Query(None),
    has_airport: bool | None = Query(None),
    pagination: dict = Depends(pagination_params),
    conn=Depends(get_db),
):
    """Liste des villes disponibles dans la base."""
    # raw SQL: spec section 2.1
    wb = WhereBuilder()
    if country:
        wb.add_raw("country_code = %s", [country.upper()])
    if search:
        wb.add_ilike("city_name", search)
    if has_station is not None:
        wb.add_raw("(nb_stations > 0) = %s", [has_station])
    if has_airport is not None:
        wb.add_raw("(flight_routes > 0) = %s", [has_airport])

    where = wb.build()

    data_query = f"""
        WITH city_data AS (
            SELECT
                departure_city AS city_name,
                departure_country AS country_code,
                COUNT(CASE WHEN mode = 'train' THEN 1 END) AS train_routes,
                COUNT(CASE WHEN mode = 'flight' THEN 1 END) AS flight_routes,
                COUNT(DISTINCT departure_station) AS nb_stations,
                COUNT(DISTINCT CASE WHEN mode = 'train' AND is_night_train = true THEN trip_id END) AS night_routes
            FROM gold_routes
            WHERE departure_city IS NOT NULL
            GROUP BY departure_city, departure_country
        )
        SELECT *,
            (nb_stations > 0) AS has_station,
            (flight_routes > 0) AS has_airport
        FROM city_data
        WHERE {where}
        ORDER BY city_name
        LIMIT %s OFFSET %s
    """

    count_query = f"""
        WITH city_data AS (
            SELECT
                departure_city AS city_name,
                departure_country AS country_code,
                COUNT(CASE WHEN mode = 'train' THEN 1 END) AS train_routes,
                COUNT(CASE WHEN mode = 'flight' THEN 1 END) AS flight_routes,
                COUNT(DISTINCT departure_station) AS nb_stations
            FROM gold_routes
            WHERE departure_city IS NOT NULL
            GROUP BY departure_city, departure_country
        )
        SELECT COUNT(*) AS total FROM city_data WHERE {where}
    """

    return execute_paginated_with_count(
        conn, data_query, count_query,
        wb.params, wb.params,
        pagination["page"], pagination["page_size"],
    )


# ── 2.2 GET /cities/{country_code} ───────────────────────────────────────────

@router.get("/cities/{country_code}")
def cities_by_country(
    country_code: str,
    conn=Depends(get_db),
):
    """Liste des villes d'un pays spécifique, avec métriques."""
    # raw SQL: spec section 2.2
    # partition pruning: departure_country
    query = """
        SELECT
            departure_city AS city_name,
            COUNT(*) AS total_routes,
            COUNT(CASE WHEN mode = 'train' THEN 1 END) AS train_routes,
            COUNT(CASE WHEN mode = 'flight' THEN 1 END) AS flight_routes,
            COUNT(DISTINCT departure_station) AS nb_stations,
            COUNT(DISTINCT arrival_country) AS connected_countries
        FROM gold_routes
        WHERE departure_country = %s
          AND departure_city IS NOT NULL
        GROUP BY departure_city
        ORDER BY total_routes DESC
    """
    rows = execute_query(conn, query, [country_code.upper()])
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 2.3 GET /stations ────────────────────────────────────────────────────────

@router.get("/stations")
def list_stations(
    city: str | None = Query(None, max_length=100),
    country: str | None = Query(None, min_length=2, max_length=2),
    search: str | None = Query(None, min_length=1, max_length=100),
    pagination: dict = Depends(pagination_params),
    conn=Depends(get_db),
):
    """Liste des gares ferroviaires."""
    # raw SQL: spec section 2.3
    wb = WhereBuilder()
    wb.add_raw("mode = 'train'")
    wb.add_raw("departure_station IS NOT NULL")
    wb.add_ilike("departure_city", city)
    # partition pruning: departure_country
    if country:
        wb.add_exact("departure_country", country.upper())
    wb.add_ilike("departure_station", search)

    where = wb.build()

    data_query = f"""
        SELECT
            departure_station AS station_name,
            departure_city AS city_name,
            departure_country AS country_code,
            departure_parent_station AS parent_station,
            COUNT(*) AS nb_departures,
            COUNT(DISTINCT arrival_city) AS destinations_served,
            COUNT(CASE WHEN is_night_train = true THEN 1 END) AS night_departures
        FROM gold_routes
        WHERE {where}
        GROUP BY departure_station, departure_city, departure_country, departure_parent_station
        ORDER BY nb_departures DESC
        LIMIT %s OFFSET %s
    """

    count_query = f"""
        SELECT COUNT(*) AS total FROM (
            SELECT 1
            FROM gold_routes
            WHERE {where}
            GROUP BY departure_station, departure_city, departure_country, departure_parent_station
        ) sub
    """

    return execute_paginated_with_count(
        conn, data_query, count_query,
        wb.params, wb.params,
        pagination["page"], pagination["page_size"],
    )


# ── 2.4 GET /stations/{country_code}/{city} ──────────────────────────────────

@router.get("/stations/{country_code}/{city}")
def stations_by_city(
    country_code: str,
    city: str,
    conn=Depends(get_db),
):
    """Toutes les gares d'une ville donnée."""
    # raw SQL: spec section 2.4
    # partition pruning: departure_country
    query = """
        SELECT
            departure_station AS station_name,
            departure_parent_station AS parent_station,
            COUNT(*) AS nb_departures,
            COUNT(DISTINCT arrival_city) AS destinations_served,
            COUNT(DISTINCT agency_name) AS operators,
            ARRAY_AGG(DISTINCT agency_name) FILTER (WHERE agency_name IS NOT NULL) AS operator_list,
            COUNT(CASE WHEN is_night_train = true THEN 1 END) AS night_departures,
            COUNT(CASE WHEN is_night_train = false THEN 1 END) AS day_departures
        FROM gold_routes
        WHERE mode = 'train'
          AND departure_country = %s
          AND departure_city ILIKE '%%' || %s || '%%'
          AND departure_station IS NOT NULL
        GROUP BY departure_station, departure_parent_station
        ORDER BY nb_departures DESC
    """
    rows = execute_query(conn, query, [country_code.upper(), city])
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 2.5 GET /airports ────────────────────────────────────────────────────────

@router.get("/airports")
def list_airports(
    city: str | None = Query(None, max_length=100),
    country: str | None = Query(None, min_length=2, max_length=2),
    search: str | None = Query(None, min_length=1, max_length=100),
    pagination: dict = Depends(pagination_params),
    conn=Depends(get_db),
):
    """Liste des aéroports disponibles."""
    # raw SQL: spec section 2.5
    wb = WhereBuilder()
    wb.add_raw("mode = 'flight'")
    wb.add_raw("departure_station IS NOT NULL")
    wb.add_ilike("departure_city", city)
    # partition pruning: departure_country
    if country:
        wb.add_exact("departure_country", country.upper())
    wb.add_ilike("departure_station", search)

    where = wb.build()

    data_query = f"""
        SELECT
            departure_station AS airport_name,
            departure_city AS city_name,
            departure_country AS country_code,
            COUNT(*) AS nb_flights,
            COUNT(DISTINCT arrival_city) AS destinations_served,
            COUNT(DISTINCT arrival_country) AS countries_served
        FROM gold_routes
        WHERE {where}
        GROUP BY departure_station, departure_city, departure_country
        ORDER BY nb_flights DESC
        LIMIT %s OFFSET %s
    """

    count_query = f"""
        SELECT COUNT(*) AS total FROM (
            SELECT 1
            FROM gold_routes
            WHERE {where}
            GROUP BY departure_station, departure_city, departure_country
        ) sub
    """

    return execute_paginated_with_count(
        conn, data_query, count_query,
        wb.params, wb.params,
        pagination["page"], pagination["page_size"],
    )


# ── 2.6 GET /airports/{country_code}/{city} ──────────────────────────────────

@router.get("/airports/{country_code}/{city}")
def airports_by_city(
    country_code: str,
    city: str,
    conn=Depends(get_db),
):
    """Tous les aéroports d'une ville donnée."""
    # raw SQL: spec section 2.6
    # partition pruning: departure_country
    query = """
        SELECT
            departure_station AS airport_name,
            COUNT(*) AS nb_flights,
            COUNT(DISTINCT arrival_city) AS destinations_served,
            COUNT(DISTINCT arrival_country) AS countries_served,
            ROUND(AVG(distance_km)::numeric, 1) AS avg_flight_distance_km
        FROM gold_routes
        WHERE mode = 'flight'
          AND departure_country = %s
          AND departure_city ILIKE '%%' || %s || '%%'
          AND departure_station IS NOT NULL
        GROUP BY departure_station
        ORDER BY nb_flights DESC
    """
    rows = execute_query(conn, query, [country_code.upper(), city])
    return {"status": "ok", "count": len(rows), "data": rows}
