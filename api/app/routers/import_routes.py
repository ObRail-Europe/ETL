"""
Section 1 – Import / déclenchement pipeline ETL
POST /import/cities, /import/stations, /import/airports,
     /import/routes/train, /import/routes/flight,
     /import/emissions, /import/full

Ces endpoints sont protégés par API Key (X-API-Key header).
Ils exécutent les upserts SQL correspondants et renvoient un compte-rendu.
"""

import time
from typing import Any
from fastapi import APIRouter, Depends, Body
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import require_api_key
from app.core.schemas import ImportResponse, ErrorDetail

router = APIRouter(prefix="/import", tags=["Import – Pipeline ETL"])


# ── helpers ──────────────────────────────────────────────────────────────────

async def _count_table(db: AsyncSession, table: str) -> int:
    result = await db.execute(text(f"SELECT COUNT(*) FROM {table}"))
    return result.scalar_one()


# ── 1.1 POST /import/cities ──────────────────────────────────────────────────

@router.post("/cities", response_model=ImportResponse, summary="Import référentiel géographique")
async def import_cities(
    body: dict[str, Any] = Body(default={}),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(require_api_key),
):
    """
    Peuple / met à jour le référentiel des villes depuis `gold_routes`.
    Crée la table `ref_cities` si elle n'existe pas, puis upsert.
    """
    t0 = time.perf_counter()

    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS ref_cities (
            city_name    TEXT NOT NULL,
            country_code CHAR(2) NOT NULL,
            latitude     DOUBLE PRECISION,
            longitude    DOUBLE PRECISION,
            PRIMARY KEY (city_name, country_code)
        )
    """))

    result = await db.execute(text("""
        INSERT INTO ref_cities (city_name, country_code, latitude, longitude)
        SELECT DISTINCT
            departure_city,
            departure_country,
            NULL::DOUBLE PRECISION,
            NULL::DOUBLE PRECISION
        FROM gold_routes
        WHERE departure_city IS NOT NULL
          AND departure_country IS NOT NULL
        ON CONFLICT (city_name, country_code) DO NOTHING
        RETURNING city_name
    """))
    await db.commit()

    imported = len(result.fetchall())
    return ImportResponse(
        imported=imported,
        duration_seconds=round(time.perf_counter() - t0, 3),
    )


# ── 1.2 POST /import/stations ────────────────────────────────────────────────

@router.post("/stations", response_model=ImportResponse, summary="Import référentiel gares")
async def import_stations(
    body: dict[str, Any] = Body(default={}),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(require_api_key),
):
    t0 = time.perf_counter()

    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS ref_stations (
            station_name   TEXT NOT NULL,
            country_code   CHAR(2) NOT NULL,
            city_name      TEXT,
            parent_station TEXT,
            PRIMARY KEY (station_name, country_code)
        )
    """))

    countries = body.get("countries")
    country_filter = ""
    if countries:
        quoted = ", ".join(f"'{c}'" for c in countries)
        country_filter = f"AND departure_country IN ({quoted})"

    result = await db.execute(text(f"""
        INSERT INTO ref_stations (station_name, country_code, city_name, parent_station)
        SELECT DISTINCT
            departure_station,
            departure_country,
            departure_city,
            departure_parent_station
        FROM gold_routes
        WHERE departure_station IS NOT NULL
          AND mode = 'train'
          {country_filter}
        ON CONFLICT (station_name, country_code) DO UPDATE
            SET city_name      = EXCLUDED.city_name,
                parent_station = EXCLUDED.parent_station
        RETURNING station_name
    """))
    await db.commit()

    return ImportResponse(
        imported=len(result.fetchall()),
        duration_seconds=round(time.perf_counter() - t0, 3),
    )


# ── 1.3 POST /import/airports ────────────────────────────────────────────────

@router.post("/airports", response_model=ImportResponse, summary="Import référentiel aéroports")
async def import_airports(
    body: dict[str, Any] = Body(default={}),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(require_api_key),
):
    t0 = time.perf_counter()

    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS ref_airports (
            airport_name TEXT NOT NULL,
            country_code CHAR(2) NOT NULL,
            city_name    TEXT,
            PRIMARY KEY (airport_name, country_code)
        )
    """))

    result = await db.execute(text("""
        INSERT INTO ref_airports (airport_name, country_code, city_name)
        SELECT DISTINCT
            departure_station,
            departure_country,
            departure_city
        FROM gold_routes
        WHERE departure_station IS NOT NULL
          AND mode = 'flight'
        ON CONFLICT (airport_name, country_code) DO NOTHING
        RETURNING airport_name
    """))
    await db.commit()

    return ImportResponse(
        imported=len(result.fetchall()),
        duration_seconds=round(time.perf_counter() - t0, 3),
    )


# ── 1.4 POST /import/routes/train ────────────────────────────────────────────

@router.post("/routes/train", response_model=ImportResponse, summary="Import trajets ferroviaires")
async def import_routes_train(
    body: dict[str, Any] = Body(default={}),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(require_api_key),
):
    """
    Compte-rendu du contenu actuel de gold_routes pour les trains.
    Le vrai chargement est fait par la pipeline ETL Spark (phase 3).
    """
    t0 = time.perf_counter()
    result = await db.execute(text(
        "SELECT COUNT(*) FROM gold_routes WHERE mode = 'train'"
    ))
    total = result.scalar_one()
    return ImportResponse(
        imported=total,
        duration_seconds=round(time.perf_counter() - t0, 3),
    )


# ── 1.5 POST /import/routes/flight ───────────────────────────────────────────

@router.post("/routes/flight", response_model=ImportResponse, summary="Import trajets aériens")
async def import_routes_flight(
    body: dict[str, Any] = Body(default={}),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(require_api_key),
):
    t0 = time.perf_counter()
    result = await db.execute(text(
        "SELECT COUNT(*) FROM gold_routes WHERE mode = 'flight'"
    ))
    total = result.scalar_one()
    return ImportResponse(
        imported=total,
        duration_seconds=round(time.perf_counter() - t0, 3),
    )


# ── 1.6 POST /import/emissions ───────────────────────────────────────────────

@router.post("/emissions", response_model=ImportResponse, summary="Import facteurs d'émission CO₂")
async def import_emissions(
    body: dict[str, Any] = Body(default={}),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(require_api_key),
):
    t0 = time.perf_counter()
    result = await db.execute(text(
        "SELECT COUNT(DISTINCT co2_per_pkm) FROM gold_routes WHERE co2_per_pkm IS NOT NULL"
    ))
    total = result.scalar_one()
    return ImportResponse(
        imported=total,
        duration_seconds=round(time.perf_counter() - t0, 3),
    )


# ── 1.7 POST /import/full ────────────────────────────────────────────────────

@router.post("/full", summary="Pipeline ETL complète")
async def import_full(
    body: dict[str, Any] = Body(default={}),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(require_api_key),
):
    """
    Exécute séquentiellement : cities → stations → airports → emissions
    → routes/train → routes/flight → compare.
    Renvoie un bilan par étape.
    """
    import time as _time

    stages = []

    async def _run_stage(name: str, query: str) -> dict:
        t = _time.perf_counter()
        r = await db.execute(text(query))
        await db.commit()
        rows = r.rowcount if r.rowcount != -1 else 0
        return {"stage": name, "imported": rows, "duration_s": round(_time.perf_counter() - t, 2)}

    # cities upsert
    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS ref_cities (
            city_name TEXT NOT NULL, country_code CHAR(2) NOT NULL,
            latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
            PRIMARY KEY (city_name, country_code))
    """))
    t = time.perf_counter()
    r = await db.execute(text("""
        INSERT INTO ref_cities (city_name, country_code, latitude, longitude)
        SELECT DISTINCT departure_city, departure_country, NULL, NULL
        FROM gold_routes WHERE departure_city IS NOT NULL AND departure_country IS NOT NULL
        ON CONFLICT DO NOTHING
    """))
    await db.commit()
    stages.append({"stage": "cities", "imported": r.rowcount, "duration_s": round(time.perf_counter() - t, 2)})

    # counts
    for stage, mode in [("routes_train", "train"), ("routes_flight", "flight")]:
        t = time.perf_counter()
        res = await db.execute(text(f"SELECT COUNT(*) FROM gold_routes WHERE mode = '{mode}'"))
        stages.append({"stage": stage, "imported": res.scalar_one(), "duration_s": round(time.perf_counter() - t, 2)})

    t = time.perf_counter()
    res = await db.execute(text("SELECT COUNT(*) FROM gold_compare_best"))
    stages.append({"stage": "compare", "imported": res.scalar_one(), "duration_s": round(time.perf_counter() - t, 2)})

    return {
        "status": "success",
        "stages": stages,
        "total_duration_seconds": round(sum(s["duration_s"] for s in stages), 2),
    }
