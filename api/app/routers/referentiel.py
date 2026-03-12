"""
Section 2 – Référentiel : Villes, Gares, Aéroports
GET /cities, /cities/{country_code}
GET /stations, /stations/{country_code}/{city}
GET /airports, /airports/{country_code}/{city}
"""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.config import settings

router = APIRouter(tags=["Référentiel – Villes, Gares, Aéroports"])


# ── helpers ───────────────────────────────────────────────────────────────────

async def _fetchall_dict(db: AsyncSession, sql: str, params: dict) -> list[dict]:
    result = await db.execute(text(sql), params)
    rows = result.mappings().fetchall()
    return [dict(r) for r in rows]


async def _count(db: AsyncSession, sql: str, params: dict) -> int:
    result = await db.execute(text(sql), params)
    return result.scalar_one()


def _paginated(data: list, total: int, page: int, page_size: int) -> dict:
    return {
        "status": "ok",
        "count": len(data),
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": data,
    }


# ── 2.1 GET /cities ───────────────────────────────────────────────────────────

@router.get("/cities", summary="Liste des villes disponibles")
async def list_cities(
    country: Optional[str] = Query(None, description="Filtre pays (alpha-2)"),
    search: Optional[str] = Query(None, description="Recherche par nom"),
    has_station: Optional[bool] = Query(None),
    has_airport: Optional[bool] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    db: AsyncSession = Depends(get_db),
):
    params: dict = {
        "country": country, "search": f"%{search}%" if search else None,
        "has_station": has_station, "has_airport": has_airport,
        "limit": page_size, "offset": (page - 1) * page_size,
    }

    base_cte = """
        WITH city_data AS (
            SELECT
                departure_city      AS city_name,
                departure_country   AS country_code,
                COUNT(CASE WHEN mode = 'train'  THEN 1 END) AS train_routes,
                COUNT(CASE WHEN mode = 'flight' THEN 1 END) AS flight_routes,
                COUNT(DISTINCT departure_station)            AS nb_stations,
                COUNT(DISTINCT CASE WHEN mode='train' AND is_night_train THEN trip_id END) AS night_routes
            FROM gold_routes
            WHERE departure_city IS NOT NULL
            GROUP BY departure_city, departure_country
        )
        SELECT *, (nb_stations > 0) AS has_station, (flight_routes > 0) AS has_airport
        FROM city_data
        WHERE (:country IS NULL OR country_code = :country)
          AND (:search IS NULL  OR city_name ILIKE :search)
          AND (:has_station IS NULL OR (nb_stations > 0) = :has_station)
          AND (:has_airport IS NULL OR (flight_routes > 0) = :has_airport)
    """

    total = await _count(db, f"SELECT COUNT(*) FROM ({base_cte}) sub", params)
    data = await _fetchall_dict(db, f"{base_cte} ORDER BY city_name LIMIT :limit OFFSET :offset", params)
    return _paginated(data, total, page, page_size)


# ── 2.2 GET /cities/{country_code} ───────────────────────────────────────────

@router.get("/cities/{country_code}", summary="Villes d'un pays")
async def cities_by_country(
    country_code: str,
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT
            departure_city                              AS city_name,
            COUNT(*)                                    AS total_routes,
            COUNT(CASE WHEN mode='train' THEN 1 END)    AS train_routes,
            COUNT(CASE WHEN mode='flight' THEN 1 END)   AS flight_routes,
            COUNT(DISTINCT departure_station)           AS nb_stations,
            COUNT(DISTINCT arrival_country)             AS connected_countries
        FROM gold_routes
        WHERE departure_country = :cc
          AND departure_city IS NOT NULL
        GROUP BY departure_city
        ORDER BY total_routes DESC
    """
    data = await _fetchall_dict(db, sql, {"cc": country_code.upper()})
    return {"status": "ok", "count": len(data), "data": data}


# ── 2.3 GET /stations ────────────────────────────────────────────────────────

@router.get("/stations", summary="Liste des gares ferroviaires")
async def list_stations(
    city: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    db: AsyncSession = Depends(get_db),
):
    params = {
        "city": f"%{city}%" if city else None,
        "country": country,
        "search": f"%{search}%" if search else None,
        "limit": page_size,
        "offset": (page - 1) * page_size,
    }
    where = """
        WHERE mode = 'train' AND departure_station IS NOT NULL
          AND (:city    IS NULL OR departure_city    ILIKE :city)
          AND (:country IS NULL OR departure_country = :country)
          AND (:search  IS NULL OR departure_station ILIKE :search)
    """
    base = f"""
        SELECT
            departure_station       AS station_name,
            departure_city          AS city_name,
            departure_country       AS country_code,
            departure_parent_station AS parent_station,
            COUNT(*)                AS nb_departures,
            COUNT(DISTINCT arrival_city) AS destinations_served,
            COUNT(CASE WHEN is_night_train THEN 1 END) AS night_departures
        FROM gold_routes {where}
        GROUP BY departure_station, departure_city, departure_country, departure_parent_station
    """
    total = await _count(db, f"SELECT COUNT(*) FROM ({base}) sub", params)
    data = await _fetchall_dict(db, f"{base} ORDER BY nb_departures DESC LIMIT :limit OFFSET :offset", params)
    return _paginated(data, total, page, page_size)


# ── 2.4 GET /stations/{country_code}/{city} ──────────────────────────────────

@router.get("/stations/{country_code}/{city}", summary="Gares d'une ville")
async def stations_by_city(
    country_code: str,
    city: str,
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT
            departure_station        AS station_name,
            departure_parent_station AS parent_station,
            COUNT(*)                 AS nb_departures,
            COUNT(DISTINCT arrival_city)   AS destinations_served,
            COUNT(DISTINCT agency_name)    AS operators,
            ARRAY_AGG(DISTINCT agency_name) FILTER (WHERE agency_name IS NOT NULL) AS operator_list,
            COUNT(CASE WHEN is_night_train     THEN 1 END) AS night_departures,
            COUNT(CASE WHEN NOT is_night_train THEN 1 END) AS day_departures
        FROM gold_routes
        WHERE mode = 'train'
          AND departure_country = :cc
          AND departure_city ILIKE :city
          AND departure_station IS NOT NULL
        GROUP BY departure_station, departure_parent_station
        ORDER BY nb_departures DESC
    """
    data = await _fetchall_dict(db, sql, {"cc": country_code.upper(), "city": f"%{city}%"})
    return {"status": "ok", "count": len(data), "data": data}


# ── 2.5 GET /airports ────────────────────────────────────────────────────────

@router.get("/airports", summary="Liste des aéroports")
async def list_airports(
    city: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    db: AsyncSession = Depends(get_db),
):
    params = {
        "city": f"%{city}%" if city else None,
        "country": country,
        "search": f"%{search}%" if search else None,
        "limit": page_size,
        "offset": (page - 1) * page_size,
    }
    where = """
        WHERE mode = 'flight' AND departure_station IS NOT NULL
          AND (:city    IS NULL OR departure_city    ILIKE :city)
          AND (:country IS NULL OR departure_country = :country)
          AND (:search  IS NULL OR departure_station ILIKE :search)
    """
    base = f"""
        SELECT
            departure_station  AS airport_name,
            departure_city     AS city_name,
            departure_country  AS country_code,
            COUNT(*)           AS nb_flights,
            COUNT(DISTINCT arrival_city)    AS destinations_served,
            COUNT(DISTINCT arrival_country) AS countries_served
        FROM gold_routes {where}
        GROUP BY departure_station, departure_city, departure_country
    """
    total = await _count(db, f"SELECT COUNT(*) FROM ({base}) sub", params)
    data = await _fetchall_dict(db, f"{base} ORDER BY nb_flights DESC LIMIT :limit OFFSET :offset", params)
    return _paginated(data, total, page, page_size)


# ── 2.6 GET /airports/{country_code}/{city} ──────────────────────────────────

@router.get("/airports/{country_code}/{city}", summary="Aéroports d'une ville")
async def airports_by_city(
    country_code: str,
    city: str,
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT
            departure_station AS airport_name,
            COUNT(*)          AS nb_flights,
            COUNT(DISTINCT arrival_city)    AS destinations_served,
            COUNT(DISTINCT arrival_country) AS countries_served,
            ROUND(AVG(distance_km)::numeric, 1) AS avg_flight_distance_km
        FROM gold_routes
        WHERE mode = 'flight'
          AND departure_country = :cc
          AND departure_city ILIKE :city
          AND departure_station IS NOT NULL
        GROUP BY departure_station
        ORDER BY nb_flights DESC
    """
    data = await _fetchall_dict(db, sql, {"cc": country_code.upper(), "city": f"%{city}%"})
    return {"status": "ok", "count": len(data), "data": data}
