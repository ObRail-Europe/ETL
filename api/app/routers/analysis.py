"""
Section 6 – Analyse Jour / Nuit
GET /analysis/day-night/coverage
GET /analysis/day-night/emissions
GET /analysis/day-night/compare
GET /analysis/day-night/routes
GET /analysis/day-night/summary
"""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.config import settings

router = APIRouter(prefix="/analysis/day-night", tags=["Analyse Jour / Nuit"])


async def _fetchall_dict(db: AsyncSession, sql: str, params: dict) -> list[dict]:
    result = await db.execute(text(sql), params)
    return [dict(r) for r in result.mappings().fetchall()]


# ── 6.1 GET /analysis/day-night/coverage ─────────────────────────────────────

@router.get("/coverage", summary="Couverture trains jour/nuit par pays")
async def daynight_coverage(
    departure_country: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT
            departure_country,
            is_night_train,
            COUNT(*)                            AS nb_routes,
            COUNT(DISTINCT agency_name)         AS nb_agencies,
            COUNT(DISTINCT departure_city)      AS nb_dep_cities,
            COUNT(DISTINCT arrival_city)        AS nb_arr_cities,
            COUNT(DISTINCT arrival_country)     AS nb_destination_countries,
            ROUND(AVG(distance_km)::numeric,  1) AS avg_distance_km,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2
        FROM gold_routes
        WHERE mode = 'train'
          AND (:dep_country IS NULL OR departure_country = :dep_country)
        GROUP BY departure_country, is_night_train
        ORDER BY departure_country, is_night_train
    """
    data = await _fetchall_dict(db, sql, {"dep_country": departure_country})
    return {"status": "ok", "count": len(data), "data": data}


# ── 6.2 GET /analysis/day-night/emissions ────────────────────────────────────

@router.get("/emissions", summary="Comparaison émissions CO₂ jour vs nuit par pays")
async def daynight_emissions(
    departure_country: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT
            departure_country,
            ROUND(AVG(CASE WHEN NOT is_night_train THEN co2_per_pkm  END)::numeric, 2) AS avg_co2_pkm_day,
            ROUND(AVG(CASE WHEN     is_night_train THEN co2_per_pkm  END)::numeric, 2) AS avg_co2_pkm_night,
            ROUND(AVG(CASE WHEN NOT is_night_train THEN emissions_co2 END)::numeric,1) AS avg_total_co2_day,
            ROUND(AVG(CASE WHEN     is_night_train THEN emissions_co2 END)::numeric,1) AS avg_total_co2_night,
            ROUND(AVG(CASE WHEN NOT is_night_train THEN distance_km  END)::numeric, 1) AS avg_dist_day,
            ROUND(AVG(CASE WHEN     is_night_train THEN distance_km  END)::numeric, 1) AS avg_dist_night
        FROM gold_routes
        WHERE mode = 'train'
          AND (:dep_country IS NULL OR departure_country = :dep_country)
        GROUP BY departure_country
        HAVING COUNT(CASE WHEN is_night_train THEN 1 END) > 0
        ORDER BY departure_country
    """
    data = await _fetchall_dict(db, sql, {"dep_country": departure_country})
    return {"status": "ok", "count": len(data), "data": data}


# ── 6.3 GET /analysis/day-night/compare ──────────────────────────────────────

@router.get("/compare", summary="Comparaison jour/nuit pour une paire O/D")
async def daynight_compare(
    origin: str = Query(...),
    destination: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT
            is_night_train,
            COUNT(*)                            AS nb_options,
            ROUND(AVG(distance_km)::numeric,  1) AS avg_distance_km,
            ROUND(AVG(co2_per_pkm)::numeric,  2) AS avg_co2_per_pkm,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
            ROUND(MIN(emissions_co2)::numeric, 1) AS min_emissions_co2,
            ROUND(MAX(emissions_co2)::numeric, 1) AS max_emissions_co2,
            MIN(departure_time) AS earliest_departure,
            MAX(departure_time) AS latest_departure,
            ARRAY_AGG(DISTINCT agency_name) FILTER (WHERE agency_name IS NOT NULL) AS operators
        FROM gold_routes
        WHERE mode = 'train'
          AND departure_city ILIKE :origin
          AND arrival_city   ILIKE :destination
        GROUP BY is_night_train
    """
    rows = await _fetchall_dict(db, sql, {
        "origin": f"%{origin}%",
        "destination": f"%{destination}%",
    })

    day   = next((r for r in rows if r["is_night_train"] is False), None)
    night = next((r for r in rows if r["is_night_train"] is True),  None)

    # détermine le meilleur mode (moins d'émissions)
    best = None
    if day and night:
        d_co2 = float(day["avg_emissions_co2"] or 0)
        n_co2 = float(night["avg_emissions_co2"] or 0)
        best = "day" if d_co2 <= n_co2 else "night"
    elif day:
        best = "day"
    elif night:
        best = "night"

    return {
        "origin": origin,
        "destination": destination,
        "day_train": day,
        "night_train": night,
        "best": best,
    }


# ── 6.4 GET /analysis/day-night/routes ──────────────────────────────────────

@router.get("/routes", summary="Liste des routes classifiées jour/nuit")
async def daynight_routes(
    departure_country: Optional[str] = Query(None),
    arrival_country: Optional[str] = Query(None),
    is_night_train: Optional[bool] = Query(None),
    agency_name: Optional[str] = Query(None),
    min_distance_km: Optional[float] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    db: AsyncSession = Depends(get_db),
):
    params = {
        "dep_country": departure_country,
        "arr_country": arrival_country,
        "is_night": is_night_train,
        "agency": f"%{agency_name}%" if agency_name else None,
        "min_dist": min_distance_km,
        "limit": page_size,
        "offset": (page - 1) * page_size,
    }
    sql = """
        SELECT
            departure_city, departure_country,
            arrival_city,   arrival_country,
            agency_name, route_long_name,
            is_night_train, days_of_week,
            departure_time, arrival_time,
            distance_km, co2_per_pkm, emissions_co2
        FROM gold_routes
        WHERE mode = 'train'
          AND (:dep_country IS NULL OR departure_country = :dep_country)
          AND (:arr_country IS NULL OR arrival_country   = :arr_country)
          AND (:is_night    IS NULL OR is_night_train    = :is_night)
          AND (:agency      IS NULL OR agency_name       ILIKE :agency)
          AND (:min_dist    IS NULL OR distance_km       >= :min_dist)
        ORDER BY departure_country, is_night_train, departure_time
        LIMIT :limit OFFSET :offset
    """
    data = await _fetchall_dict(db, sql, params)
    return {"status": "ok", "count": len(data), "data": data}


# ── 6.5 GET /analysis/day-night/summary ──────────────────────────────────────

@router.get("/summary", summary="Résumé européen jour/nuit")
async def daynight_summary(db: AsyncSession = Depends(get_db)):
    sql = """
        SELECT
            is_night_train,
            COUNT(*)                            AS total_routes,
            COUNT(DISTINCT departure_country)   AS countries_served,
            COUNT(DISTINCT agency_name)         AS operators,
            COUNT(DISTINCT departure_city || '-' || arrival_city) AS unique_od_pairs,
            ROUND(AVG(distance_km)::numeric,  1) AS avg_distance_km,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY distance_km)::numeric, 1) AS median_distance_km,
            ROUND(AVG(co2_per_pkm)::numeric,  2) AS avg_co2_per_pkm,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
            ROUND(SUM(emissions_co2)::numeric, 0) AS total_emissions_co2
        FROM gold_routes
        WHERE mode = 'train'
        GROUP BY is_night_train
    """
    data = await _fetchall_dict(db, sql, {})
    return {"status": "ok", "count": len(data), "data": data}
