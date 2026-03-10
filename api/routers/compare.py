"""
Endpoints /compare — Section 3.3 & 3.4 de la spec
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
import io, csv

from api.database import get_db

router = APIRouter()

COMPARE_SORT_COLS = {"departure_country", "departure_city", "arrival_city", "train_distance_km", "flight_distance_km", "train_emissions_co2", "flight_emissions_co2"}


@router.get("/compare", summary="Consultation paginée gold_compare_best")
def get_compare(
    departure_city: Optional[str] = Query(None),
    departure_country: Optional[str] = Query(None),
    arrival_city: Optional[str] = Query(None),
    arrival_country: Optional[str] = Query(None),
    best_mode: Optional[str] = Query(None, regex="^(train|flight)$"),
    sort_by: str = Query("departure_country"),
    sort_order: str = Query("asc", regex="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=500),
    db: Session = Depends(get_db),
):
    if sort_by not in COMPARE_SORT_COLS:
        raise HTTPException(status_code=422, detail=f"sort_by invalide. Valeurs : {sorted(COMPARE_SORT_COLS)}")

    conditions = ["1=1"]
    params: dict = {}
    if departure_city:
        conditions.append("departure_city ILIKE :dep_city"); params["dep_city"] = f"%{departure_city}%"
    if departure_country:
        conditions.append("departure_country = :dep_country"); params["dep_country"] = departure_country.upper()
    if arrival_city:
        conditions.append("arrival_city ILIKE :arr_city"); params["arr_city"] = f"%{arrival_city}%"
    if arrival_country:
        conditions.append("arrival_country = :arr_country"); params["arr_country"] = arrival_country.upper()
    if best_mode:
        conditions.append("best_mode = :best_mode"); params["best_mode"] = best_mode

    where = " AND ".join(conditions)
    total = db.execute(text(f"SELECT COUNT(*) FROM gold_compare_best WHERE {where}"), params).scalar()
    params["limit"] = page_size
    params["offset"] = (page - 1) * page_size

    rows = db.execute(text(f"""
        SELECT * FROM gold_compare_best WHERE {where}
        ORDER BY {sort_by} {sort_order}
        LIMIT :limit OFFSET :offset
    """), params).mappings().all()

    return {"status": "ok", "count": len(rows), "total": total, "page": page, "page_size": page_size, "data": [dict(r) for r in rows]}


@router.get("/compare/download", summary="Export CSV gold_compare_best")
def download_compare(
    departure_country: Optional[str] = Query(None),
    best_mode: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    conditions = ["1=1"]
    params: dict = {"limit": 500_001}
    if departure_country:
        conditions.append("departure_country = :dep_country"); params["dep_country"] = departure_country.upper()
    if best_mode:
        conditions.append("best_mode = :best_mode"); params["best_mode"] = best_mode

    where = " AND ".join(conditions)
    rows = db.execute(text(f"SELECT * FROM gold_compare_best WHERE {where} LIMIT :limit"), params).mappings().all()

    if len(rows) > 500_000:
        raise HTTPException(status_code=413, detail="Résultat > 500 000 lignes. Affinez vos filtres.")

    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows([dict(r) for r in rows])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=compare_export.csv"})