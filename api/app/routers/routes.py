"""
Section 3 – Consultation & Téléchargement
GET /routes, /routes/download, /compare, /compare/download

Section 4 – Recherche
GET /routes/search, /routes/{trip_id}
"""

import csv
import io
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.config import settings
from app.core.schemas import safe_sort_col, GOLD_ROUTES_SORTABLE, COMPARE_SORTABLE

router = APIRouter(tags=["Consultation & Recherche"])


# ── helpers ───────────────────────────────────────────────────────────────────

async def _fetchall_dict(db: AsyncSession, sql: str, params: dict) -> list[dict]:
    result = await db.execute(text(sql), params)
    return [dict(r) for r in result.mappings().fetchall()]


async def _count_raw(db: AsyncSession, sql: str, params: dict) -> int:
    result = await db.execute(text(sql), params)
    return result.scalar_one()


def _paginated(data, total, page, page_size):
    return {"status": "ok", "count": len(data), "total": total,
            "page": page, "page_size": page_size, "data": data}


def _build_routes_where(params: dict) -> str:
    return """
        WHERE 1=1
          AND (:mode        IS NULL OR mode                = :mode)
          AND (:source      IS NULL OR source              = :source)
          AND (:dep_country IS NULL OR departure_country   = :dep_country)
          AND (:arr_country IS NULL OR arrival_country     = :arr_country)
          AND (:dep_city    IS NULL OR departure_city      ILIKE :dep_city)
          AND (:arr_city    IS NULL OR arrival_city        ILIKE :arr_city)
          AND (:dep_station IS NULL OR departure_station   ILIKE :dep_station)
          AND (:arr_station IS NULL OR arrival_station     ILIKE :arr_station)
          AND (:agency      IS NULL OR agency_name         ILIKE :agency)
          AND (:route_type  IS NULL OR route_type          = :route_type)
          AND (:is_night    IS NULL OR is_night_train      = :is_night)
          AND (:days        IS NULL OR days_of_week        LIKE :days)
          AND (:min_dist    IS NULL OR distance_km         >= :min_dist)
          AND (:max_dist    IS NULL OR distance_km         <= :max_dist)
          AND (:min_co2     IS NULL OR emissions_co2       >= :min_co2)
          AND (:max_co2     IS NULL OR emissions_co2       <= :max_co2)
          AND (:start_after IS NULL OR service_start_date  >= :start_after)
          AND (:end_before  IS NULL OR service_end_date    <= :end_before)
    """


# ── 3.1 GET /routes ──────────────────────────────────────────────────────────

@router.get("/routes", summary="Consultation paginée gold_routes")
async def list_routes(
    mode: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    departure_country: Optional[str] = Query(None),
    arrival_country: Optional[str] = Query(None),
    departure_city: Optional[str] = Query(None),
    arrival_city: Optional[str] = Query(None),
    departure_station: Optional[str] = Query(None),
    arrival_station: Optional[str] = Query(None),
    agency_name: Optional[str] = Query(None),
    route_type: Optional[int] = Query(None),
    is_night_train: Optional[bool] = Query(None),
    days_of_week: Optional[str] = Query(None),
    min_distance_km: Optional[float] = Query(None),
    max_distance_km: Optional[float] = Query(None),
    min_co2: Optional[float] = Query(None),
    max_co2: Optional[float] = Query(None),
    service_start_after: Optional[date] = Query(None),
    service_end_before: Optional[date] = Query(None),
    sort_by: Optional[str] = Query(None),
    sort_order: str = Query("asc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    db: AsyncSession = Depends(get_db),
):
    col = safe_sort_col(sort_by, GOLD_ROUTES_SORTABLE, "departure_country")
    order = "DESC" if sort_order == "desc" else "ASC"

    params = {
        "mode": mode, "source": source,
        "dep_country": departure_country, "arr_country": arrival_country,
        "dep_city": f"%{departure_city}%" if departure_city else None,
        "arr_city": f"%{arrival_city}%" if arrival_city else None,
        "dep_station": f"%{departure_station}%" if departure_station else None,
        "arr_station": f"%{arrival_station}%" if arrival_station else None,
        "agency": f"%{agency_name}%" if agency_name else None,
        "route_type": str(route_type) if route_type is not None else None,
        "is_night": is_night_train,
        "days": days_of_week,
        "min_dist": min_distance_km, "max_dist": max_distance_km,
        "min_co2": min_co2, "max_co2": max_co2,
        "start_after": service_start_after, "end_before": service_end_before,
        "limit": page_size, "offset": (page - 1) * page_size,
    }

    where = _build_routes_where(params)
    total = await _count_raw(db, f"SELECT COUNT(*) FROM gold_routes {where}", params)
    sql = f"SELECT * FROM gold_routes {where} ORDER BY {col} {order} LIMIT :limit OFFSET :offset"
    data = await _fetchall_dict(db, sql, params)
    return _paginated(data, total, page, page_size)


# ── 3.2 GET /routes/download ─────────────────────────────────────────────────

@router.get("/routes/download", summary="Export CSV gold_routes")
async def download_routes(
    mode: Optional[str] = Query(None),
    departure_country: Optional[str] = Query(None),
    arrival_country: Optional[str] = Query(None),
    departure_city: Optional[str] = Query(None),
    arrival_city: Optional[str] = Query(None),
    is_night_train: Optional[bool] = Query(None),
    min_distance_km: Optional[float] = Query(None),
    max_distance_km: Optional[float] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    params = {
        "mode": mode, "source": None,
        "dep_country": departure_country, "arr_country": arrival_country,
        "dep_city": f"%{departure_city}%" if departure_city else None,
        "arr_city": f"%{arrival_city}%" if arrival_city else None,
        "dep_station": None, "arr_station": None, "agency": None,
        "route_type": None, "is_night": is_night_train, "days": None,
        "min_dist": min_distance_km, "max_dist": max_distance_km,
        "min_co2": None, "max_co2": None, "start_after": None, "end_before": None,
        "limit": settings.MAX_CSV_ROWS + 1, "offset": 0,
    }
    where = _build_routes_where(params)
    rows = await _fetchall_dict(db, f"SELECT * FROM gold_routes {where} LIMIT :limit OFFSET :offset", params)

    if len(rows) > settings.MAX_CSV_ROWS:
        raise HTTPException(
            status_code=413,
            detail=f"Export dépasse {settings.MAX_CSV_ROWS} lignes. Affinez vos filtres.",
        )

    def _generate():
        buf = io.StringIO()
        if not rows:
            yield ""
            return
        writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    return StreamingResponse(
        _generate(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=routes_export.csv"},
    )


# ── 3.3 GET /compare ─────────────────────────────────────────────────────────

@router.get("/compare", summary="Consultation paginée gold_compare_best")
async def list_compare(
    departure_city: Optional[str] = Query(None),
    departure_country: Optional[str] = Query(None),
    arrival_city: Optional[str] = Query(None),
    arrival_country: Optional[str] = Query(None),
    best_mode: Optional[str] = Query(None),
    min_train_duration: Optional[float] = Query(None),
    max_train_duration: Optional[float] = Query(None),
    min_flight_duration: Optional[float] = Query(None),
    max_flight_duration: Optional[float] = Query(None),
    days_of_week: Optional[str] = Query(None),
    sort_by: Optional[str] = Query(None),
    sort_order: str = Query("asc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    db: AsyncSession = Depends(get_db),
):
    col = safe_sort_col(sort_by, COMPARE_SORTABLE, "departure_country")
    order = "DESC" if sort_order == "desc" else "ASC"

    params = {
        "dep_city": f"%{departure_city}%" if departure_city else None,
        "dep_country": departure_country,
        "arr_city": f"%{arrival_city}%" if arrival_city else None,
        "arr_country": arrival_country,
        "best_mode": best_mode,
        "min_train_dur": min_train_duration, "max_train_dur": max_train_duration,
        "min_flt_dur": min_flight_duration, "max_flt_dur": max_flight_duration,
        "days": days_of_week,
        "limit": page_size, "offset": (page - 1) * page_size,
    }

    where = """
        WHERE 1=1
          AND (:dep_city    IS NULL OR departure_city   ILIKE :dep_city)
          AND (:dep_country IS NULL OR departure_country = :dep_country)
          AND (:arr_city    IS NULL OR arrival_city     ILIKE :arr_city)
          AND (:arr_country IS NULL OR arrival_country   = :arr_country)
          AND (:best_mode   IS NULL OR best_mode         = :best_mode)
          AND (:min_train_dur IS NULL OR train_duration_min  >= :min_train_dur)
          AND (:max_train_dur IS NULL OR train_duration_min  <= :max_train_dur)
          AND (:min_flt_dur   IS NULL OR flight_duration_min >= :min_flt_dur)
          AND (:max_flt_dur   IS NULL OR flight_duration_min <= :max_flt_dur)
          AND (:days IS NULL OR days_of_week LIKE :days)
    """

    total = await _count_raw(db, f"SELECT COUNT(*) FROM gold_compare_best {where}", params)
    data = await _fetchall_dict(db, f"SELECT * FROM gold_compare_best {where} ORDER BY {col} {order} LIMIT :limit OFFSET :offset", params)
    return _paginated(data, total, page, page_size)


# ── 3.4 GET /compare/download ────────────────────────────────────────────────

@router.get("/compare/download", summary="Export CSV gold_compare_best")
async def download_compare(
    departure_country: Optional[str] = Query(None),
    arrival_country: Optional[str] = Query(None),
    best_mode: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    params = {
        "dep_city": None, "dep_country": departure_country,
        "arr_city": None, "arr_country": arrival_country,
        "best_mode": best_mode,
        "min_train_dur": None, "max_train_dur": None,
        "min_flt_dur": None, "max_flt_dur": None,
        "days": None,
        "limit": settings.MAX_CSV_ROWS + 1, "offset": 0,
    }
    where = """
        WHERE 1=1
          AND (:dep_country IS NULL OR departure_country = :dep_country)
          AND (:arr_country IS NULL OR arrival_country   = :arr_country)
          AND (:best_mode   IS NULL OR best_mode         = :best_mode)
    """
    rows = await _fetchall_dict(db, f"SELECT * FROM gold_compare_best {where} LIMIT :limit OFFSET :offset", params)

    if len(rows) > settings.MAX_CSV_ROWS:
        raise HTTPException(status_code=413, detail="Export trop volumineux. Affinez vos filtres.")

    def _generate():
        buf = io.StringIO()
        if not rows:
            yield ""
            return
        writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    return StreamingResponse(
        _generate(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=compare_export.csv"},
    )


# ── 4.1 GET /routes/search ───────────────────────────────────────────────────

@router.get("/routes/search", summary="Recherche trajet bidirectionnelle")
async def search_routes(
    origin: str = Query(..., description="Ville ou gare d'origine"),
    destination: str = Query(..., description="Ville ou gare de destination"),
    date: Optional[date] = Query(None),
    day_of_week: Optional[int] = Query(None, ge=1, le=7),
    is_night_train: Optional[bool] = Query(None),
    bidirectional: bool = Query(True),
    page: int = Query(1, ge=1),
    page_size: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    db: AsyncSession = Depends(get_db),
):
    params = {
        "origin": f"%{origin}%",
        "destination": f"%{destination}%",
        "date": date,
        "dow": day_of_week,
        "is_night": is_night_train,
        "bidirectional": bidirectional,
        "limit": page_size,
        "offset": (page - 1) * page_size,
    }

    sql = """
        SELECT *, 'outbound' AS direction FROM gold_routes
        WHERE mode = 'train'
          AND (departure_city ILIKE :origin    OR departure_station ILIKE :origin)
          AND (arrival_city   ILIKE :destination OR arrival_station   ILIKE :destination)
          AND (:date IS NULL OR (service_start_date <= :date AND service_end_date >= :date))
          AND (:dow  IS NULL OR SUBSTRING(days_of_week, :dow::int, 1) = '1')
          AND (:is_night IS NULL OR is_night_train = :is_night)

        UNION ALL

        SELECT *, 'return' AS direction FROM gold_routes
        WHERE mode = 'train'
          AND :bidirectional = true
          AND (departure_city ILIKE :destination OR departure_station ILIKE :destination)
          AND (arrival_city   ILIKE :origin      OR arrival_station   ILIKE :origin)
          AND (:date IS NULL OR (service_start_date <= :date AND service_end_date >= :date))
          AND (:dow  IS NULL OR SUBSTRING(days_of_week, :dow::int, 1) = '1')
          AND (:is_night IS NULL OR is_night_train = :is_night)

        ORDER BY direction, departure_time
        LIMIT :limit OFFSET :offset
    """
    data = await _fetchall_dict(db, sql, params)
    return {"status": "ok", "count": len(data), "data": data}


# ── 4.2 GET /routes/{trip_id} ────────────────────────────────────────────────

@router.get("/routes/{trip_id}", summary="Détail d'un trajet")
async def get_route_by_trip_id(
    trip_id: str,
    departure_country: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT * FROM gold_routes
        WHERE trip_id = :trip_id
          AND (:dep_country IS NULL OR departure_country = :dep_country)
    """
    data = await _fetchall_dict(db, sql, {"trip_id": trip_id, "dep_country": departure_country})
    if not data:
        raise HTTPException(status_code=404, detail=f"Trajet '{trip_id}' introuvable.")
    return {"status": "ok", "count": len(data), "data": data}
