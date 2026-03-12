"""
Endpoints Statistiques & Agrégations (section 8 de la spec).

3 GET endpoints pour les classements opérateurs, distributions et émissions.
"""

from fastapi import APIRouter, Depends, Query

from ..database import get_db
from ..utils.query_helpers import execute_query, WhereBuilder

router = APIRouter()


# ── 8.1 GET /stats/operators ──────────────────────────────────────────────────

@router.get("/stats/operators")
def stats_operators(conn=Depends(get_db)):
    """Classement des opérateurs ferroviaires par volume et couverture."""
    # raw SQL: spec section 8.1
    query = """
        SELECT
            agency_name,
            COUNT(*) AS total_routes,
            COUNT(DISTINCT departure_country) AS countries_served,
            COUNT(DISTINCT departure_city)    AS dep_cities,
            COUNT(DISTINCT arrival_city)      AS arr_cities,
            COUNT(CASE WHEN is_night_train = true THEN 1 END) AS night_routes,
            ROUND(AVG(distance_km)::numeric, 1) AS avg_distance_km,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
            ROUND(MIN(distance_km)::numeric, 1) AS min_distance_km,
            ROUND(MAX(distance_km)::numeric, 1) AS max_distance_km
        FROM gold_routes
        WHERE mode = 'train'
          AND agency_name IS NOT NULL
        GROUP BY agency_name
        ORDER BY total_routes DESC
    """
    rows = execute_query(conn, query)
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 8.2 GET /stats/distances ─────────────────────────────────────────────────

@router.get("/stats/distances")
def stats_distances(
    mode: str | None = Query(None, pattern="^(train|flight)$"),
    departure_country: str | None = Query(None, min_length=2, max_length=2),
    bucket_size: int = Query(100, ge=1, le=1000),
    conn=Depends(get_db),
):
    """Distribution des distances par mode et type jour/nuit."""
    # raw SQL: spec section 8.2
    wb = WhereBuilder()
    wb.add_raw("distance_km IS NOT NULL")
    wb.add_exact("mode", mode)
    # partition pruning: departure_country
    if departure_country:
        wb.add_exact("departure_country", departure_country.upper())

    where = wb.build()
    params = wb.params + [bucket_size, bucket_size]

    query = f"""
        SELECT
            mode,
            is_night_train,
            FLOOR(distance_km / %s) * %s AS distance_bucket,
            COUNT(*) AS nb_routes,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2
        FROM gold_routes
        WHERE {where}
        GROUP BY mode, is_night_train, distance_bucket
        ORDER BY mode, is_night_train, distance_bucket
    """
    rows = execute_query(conn, query, params)
    return {"status": "ok", "count": len(rows), "data": rows}


# ── 8.3 GET /stats/emissions-by-distance ──────────────────────────────────────

@router.get("/stats/emissions-by-distance")
def stats_emissions_by_distance(conn=Depends(get_db)):
    """Émissions moyennes par tranche de distance."""
    # raw SQL: spec section 8.3
    query = """
        SELECT
            mode,
            FLOOR(distance_km / 100) * 100 AS distance_range_start,
            FLOOR(distance_km / 100) * 100 + 99 AS distance_range_end,
            COUNT(*) AS nb_routes,
            ROUND(AVG(co2_per_pkm)::numeric, 2) AS avg_co2_per_pkm,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_total_emissions
        FROM gold_routes
        WHERE distance_km IS NOT NULL
          AND emissions_co2 IS NOT NULL
        GROUP BY mode, FLOOR(distance_km / 100)
        ORDER BY mode, distance_range_start
    """
    rows = execute_query(conn, query)
    return {"status": "ok", "count": len(rows), "data": rows}
