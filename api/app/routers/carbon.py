"""
Section 5 – Émissions Carbone
GET /carbon/trip/{trip_id}
GET /carbon/estimate
GET /carbon/ranking
GET /carbon/factors
"""

from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.config import settings
from app.core.schemas import safe_sort_col, CARBON_RANKING_SORTABLE

router = APIRouter(prefix="/carbon", tags=["Émissions Carbone"])


async def _fetchall_dict(db: AsyncSession, sql: str, params: dict) -> list[dict]:
    result = await db.execute(text(sql), params)
    return [dict(r) for r in result.mappings().fetchall()]


# ── 5.1 GET /carbon/trip/{trip_id} ───────────────────────────────────────────

@router.get("/trip/{trip_id}", summary="Bilan carbone d'un trajet")
async def carbon_trip(
    trip_id: str,
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT trip_id, mode, departure_city, arrival_city,
               departure_country, arrival_country,
               distance_km, co2_per_pkm, emissions_co2, is_night_train
        FROM gold_routes
        WHERE trip_id = :trip_id
        LIMIT 1
    """
    rows = await _fetchall_dict(db, sql, {"trip_id": trip_id})
    if not rows:
        raise HTTPException(status_code=404, detail=f"Trajet '{trip_id}' introuvable.")
    return rows[0]


# ── 5.2 GET /carbon/estimate ─────────────────────────────────────────────────

@router.get("/estimate", summary="Estimation CO₂ pour une paire O/D")
async def carbon_estimate(
    origin: str = Query(...),
    destination: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT
            mode,
            COUNT(*) AS nb_options,
            ROUND(AVG(distance_km)::numeric,  1) AS avg_distance_km,
            ROUND(AVG(co2_per_pkm)::numeric,  2) AS avg_co2_per_pkm,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
            ROUND(MIN(emissions_co2)::numeric, 1) AS min_emissions_co2,
            ROUND(MAX(emissions_co2)::numeric, 1) AS max_emissions_co2
        FROM gold_routes
        WHERE departure_city ILIKE :origin
          AND arrival_city   ILIKE :destination
        GROUP BY mode
    """
    rows = await _fetchall_dict(db, sql, {
        "origin": f"%{origin}%",
        "destination": f"%{destination}%",
    })

    # calcul économie CO₂ côté applicatif
    train = next((r for r in rows if r["mode"] == "train"),  None)
    flight = next((r for r in rows if r["mode"] == "flight"), None)
    co2_saving_pct = None
    if train and flight and flight["avg_emissions_co2"] and float(flight["avg_emissions_co2"]) > 0:
        co2_saving_pct = round(
            (1 - float(train["avg_emissions_co2"]) / float(flight["avg_emissions_co2"])) * 100, 1
        )

    return {
        "origin": origin,
        "destination": destination,
        "comparison": rows,
        "co2_saving_pct": co2_saving_pct,
    }


# ── 5.3 GET /carbon/ranking ──────────────────────────────────────────────────

@router.get("/ranking", summary="Classement paires O/D par économie CO₂")
async def carbon_ranking(
    departure_country: Optional[str] = Query(None),
    min_distance_km: Optional[float] = Query(None),
    sort_by: str = Query("co2_saving_pct"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    db: AsyncSession = Depends(get_db),
):
    col = safe_sort_col(sort_by, CARBON_RANKING_SORTABLE, "co2_saving_pct")
    order = "DESC" if sort_order == "desc" else "ASC"

    params = {
        "dep_country": departure_country,
        "min_dist": min_distance_km,
        "limit": page_size,
        "offset": (page - 1) * page_size,
    }

    base = """
        SELECT
            departure_city, departure_country,
            arrival_city,   arrival_country,
            train_distance_km, train_emissions_co2,
            flight_distance_km, flight_emissions_co2,
            ROUND((1 - train_emissions_co2 / NULLIF(flight_emissions_co2, 0)) * 100, 1) AS co2_saving_pct,
            best_mode
        FROM gold_compare_best
        WHERE flight_emissions_co2 IS NOT NULL
          AND train_emissions_co2  IS NOT NULL
          AND flight_emissions_co2 > 0
          AND (:dep_country IS NULL OR departure_country = :dep_country)
          AND (:min_dist    IS NULL OR train_distance_km >= :min_dist)
    """

    result = await db.execute(text(f"SELECT COUNT(*) FROM ({base}) sub"), params)
    total = result.scalar_one()
    data = await _fetchall_dict(db, f"{base} ORDER BY {col} {order} LIMIT :limit OFFSET :offset", params)

    return {"status": "ok", "count": len(data), "total": total,
            "page": page, "page_size": page_size, "data": data}


# ── 5.4 GET /carbon/factors ──────────────────────────────────────────────────

@router.get("/factors", summary="Facteurs d'émission CO₂ (transparence)")
async def carbon_factors(
    country: Optional[str] = Query(None),
    mode: Optional[str] = Query(None),
    is_night_train: Optional[bool] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT
            departure_country   AS country_code,
            mode,
            is_night_train,
            COUNT(*)            AS nb_routes_using_factor,
            ROUND(AVG(co2_per_pkm)::numeric,    4) AS avg_co2_per_pkm,
            ROUND(MIN(co2_per_pkm)::numeric,    4) AS min_co2_per_pkm,
            ROUND(MAX(co2_per_pkm)::numeric,    4) AS max_co2_per_pkm,
            ROUND(STDDEV(co2_per_pkm)::numeric, 4) AS stddev_co2_per_pkm,
            COUNT(DISTINCT co2_per_pkm)            AS distinct_factors
        FROM gold_routes
        WHERE co2_per_pkm IS NOT NULL
          AND (:country  IS NULL OR departure_country = :country)
          AND (:mode     IS NULL OR mode              = :mode)
          AND (:is_night IS NULL OR is_night_train    = :is_night)
        GROUP BY departure_country, mode, is_night_train
        ORDER BY departure_country, mode, is_night_train
    """
    data = await _fetchall_dict(db, sql, {
        "country": country, "mode": mode, "is_night": is_night_train
    })
    return {"status": "ok", "count": len(data), "data": data}
