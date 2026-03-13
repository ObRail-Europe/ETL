"""
Endpoints Analyse Jour / Nuit (section 6 de la spec).

5 GET endpoints pour la comparaison trains de jour vs trains de nuit.
"""

from fastapi import APIRouter, Depends, Query

from ..database import get_db
from ..dependencies import pagination_params
from ..utils.query_helpers import (
    execute_query, WhereBuilder,
    execute_paginated_with_count, safe_order_by, safe_sort_direction,
    escape_like,
)

router = APIRouter()


@router.get("/analysis/day-night/coverage")
def day_night_coverage(
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    conn=Depends(get_db),
):
    """Couverture des trains de jour vs nuit par pays."""
    # Cette vue agrégée permet de comparer rapidement l'offre jour/nuit
    # par pays sans retraitement applicatif.
    wb = WhereBuilder()
    wb.add_raw("mode = 'train'")
    # Le filtre pays est appliqué tôt pour réduire le coût de l'agrégation.
    if departure_country:
        wb.add_exact("departure_country", departure_country.upper())

    where = wb.build()
    query = f"""
        SELECT
            departure_country,
            is_night_train,
            COUNT(*)                          AS nb_routes,
            COUNT(DISTINCT agency_name)       AS nb_agencies,
            COUNT(DISTINCT departure_city)    AS nb_dep_cities,
            COUNT(DISTINCT arrival_city)      AS nb_arr_cities,
            COUNT(DISTINCT arrival_country)   AS nb_destination_countries,
            ROUND(AVG(distance_km)::numeric, 1)     AS avg_distance_km,
            ROUND(AVG(emissions_co2)::numeric, 1)   AS avg_emissions_co2
        FROM gold_routes
        WHERE {where}
        GROUP BY departure_country, is_night_train
        ORDER BY departure_country, is_night_train
    """
    rows = execute_query(conn, query, wb.params)
    return {"status": "ok", "count": len(rows), "data": rows}


@router.get("/analysis/day-night/emissions")
def day_night_emissions(
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    conn=Depends(get_db),
):
    """Comparaison des émissions CO₂ jour vs nuit par pays."""
    # On garde une agrégation SQL directe pour comparer jour/nuit avec les
    # mêmes hypothèses de calcul par pays.
    wb = WhereBuilder()
    wb.add_raw("mode = 'train'")
    if departure_country:
        wb.add_exact("departure_country", departure_country.upper())

    where = wb.build()
    query = f"""
        SELECT
            departure_country,
            ROUND(AVG(CASE WHEN is_night_train = false THEN co2_per_pkm END)::numeric, 2) AS avg_co2_pkm_day,
            ROUND(AVG(CASE WHEN is_night_train = true  THEN co2_per_pkm END)::numeric, 2) AS avg_co2_pkm_night,
            ROUND(AVG(CASE WHEN is_night_train = false THEN emissions_co2 END)::numeric, 1) AS avg_total_co2_day,
            ROUND(AVG(CASE WHEN is_night_train = true  THEN emissions_co2 END)::numeric, 1) AS avg_total_co2_night,
            ROUND(AVG(CASE WHEN is_night_train = false THEN distance_km END)::numeric, 1) AS avg_dist_day,
            ROUND(AVG(CASE WHEN is_night_train = true  THEN distance_km END)::numeric, 1) AS avg_dist_night
        FROM gold_routes
        WHERE {where}
        GROUP BY departure_country
        HAVING COUNT(CASE WHEN is_night_train = true THEN 1 END) > 0
        ORDER BY departure_country
    """
    rows = execute_query(conn, query, wb.params)
    return {"status": "ok", "count": len(rows), "data": rows}


@router.get("/analysis/day-night/compare")
def day_night_compare(
    origin: str = Query(..., min_length=1, max_length=100),
    destination: str = Query(..., min_length=1, max_length=100),
    conn=Depends(get_db),
):
    """Comparaison jour vs nuit pour une paire O/D spécifique."""
    # Cette requête sert à comparer une paire O/D précise en séparant clairement
    # les options jour et nuit.
    query = """
        SELECT
            is_night_train,
            COUNT(*) AS nb_options,
            ROUND(AVG(distance_km)::numeric, 1) AS avg_distance_km,
            ROUND(AVG(co2_per_pkm)::numeric, 2) AS avg_co2_per_pkm,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
            ROUND(MIN(emissions_co2)::numeric, 1) AS min_emissions_co2,
            ROUND(MAX(emissions_co2)::numeric, 1) AS max_emissions_co2,
            MIN(departure_time) AS earliest_departure,
            MAX(departure_time) AS latest_departure,
            ARRAY_AGG(DISTINCT agency_name) FILTER (WHERE agency_name IS NOT NULL) AS operators
        FROM gold_routes
        WHERE mode = 'train'
          AND (departure_city ILIKE '%%' || %s || '%%')
          AND (arrival_city ILIKE '%%' || %s || '%%')
        GROUP BY is_night_train
    """
    rows = execute_query(conn, query, [escape_like(origin), escape_like(destination)])

    result = {"origin": origin, "destination": destination}
    for row in rows:
        key = "night_train" if row.get("is_night_train") else "day_train"
        result[key] = row

    # Le meilleur mode est choisi sur la moyenne d'émissions quand les deux
    # classes sont disponibles.
    day = result.get("day_train", {})
    night = result.get("night_train", {})
    if day and night:
        day_co2 = day.get("avg_emissions_co2") or float("inf")
        night_co2 = night.get("avg_emissions_co2") or float("inf")
        result["best"] = "day" if day_co2 <= night_co2 else "night"

    return result


@router.get("/analysis/day-night/routes")
def day_night_routes(
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    arrival_country: str | None = Query(None, min_length=2, max_length=2),
    is_night_train: bool | None = Query(None),
    agency_name: str | None = Query(None, max_length=100),
    min_distance_km: float | None = Query(None, ge=0),
    pagination: dict = Depends(pagination_params),
    conn=Depends(get_db),
):
    """Liste des routes avec classification jour/nuit."""
    # Endpoint orienté exploration détaillée, avec pagination pour rester stable
    # même sur de gros volumes.
    wb = WhereBuilder()
    wb.add_raw("mode = 'train'")
    # On pousse le filtre pays dans le WHERE pour bénéficier du pruning.
    if departure_country:
        wb.add_exact("departure_country", departure_country.upper())
    if arrival_country:
        wb.add_exact("arrival_country", arrival_country.upper())
    wb.add_bool("is_night_train", is_night_train)
    wb.add_ilike("agency_name", agency_name)
    wb.add_gte("distance_km", min_distance_km)

    where = wb.build()

    data_query = f"""
        SELECT
            departure_city,
            departure_country,
            arrival_city,
            arrival_country,
            agency_name,
            route_long_name,
            is_night_train,
            days_of_week,
            departure_time,
            arrival_time,
            distance_km,
            co2_per_pkm,
            emissions_co2
        FROM gold_routes
        WHERE {where}
        ORDER BY departure_country, is_night_train, departure_time
        LIMIT %s OFFSET %s
    """

    count_query = f"SELECT COUNT(*) AS total FROM gold_routes WHERE {where}"

    return execute_paginated_with_count(
        conn, data_query, count_query,
        wb.params, wb.params,
        pagination["page"], pagination["page_size"],
    )


@router.get("/analysis/day-night/summary")
def day_night_summary(conn=Depends(get_db)):
    """Résumé agrégé jour/nuit au niveau européen."""
    # Résumé global conçu pour alimenter les KPIs de synthèse sans calcul côté
    # client.
    query = """
        SELECT
            is_night_train,
            COUNT(*)                                    AS total_routes,
            COUNT(DISTINCT departure_country)           AS countries_served,
            COUNT(DISTINCT agency_name)                 AS operators,
            COUNT(DISTINCT departure_city || '-' || arrival_city) AS unique_od_pairs,
            ROUND(AVG(distance_km)::numeric, 1)         AS avg_distance_km,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY distance_km)::numeric, 1) AS median_distance_km,
            ROUND(AVG(co2_per_pkm)::numeric, 2)         AS avg_co2_per_pkm,
            ROUND(AVG(emissions_co2)::numeric, 1)       AS avg_emissions_co2,
            ROUND(SUM(emissions_co2)::numeric, 0)       AS total_emissions_co2
        FROM gold_routes
        WHERE mode = 'train'
        GROUP BY is_night_train
    """
    rows = execute_query(conn, query)
    return {"status": "ok", "count": len(rows), "data": rows}
