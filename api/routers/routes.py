"""
Endpoints /routes — Section 3 & 4 de la spec API

Inclut :
- GET /routes              : consultation paginée gold_routes
- GET /routes/search       : recherche bidirectionnelle train
- GET /routes/{trip_id}    : détail d'un trajet
- GET /routes/download     : export CSV (limite 500k lignes)
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
import io
import csv

from api.database import get_db

router = APIRouter()

# Whitelist des colonnes autorisées pour le tri (évite les injections SQL)
SORTABLE_COLUMNS = {
    "departure_country", "departure_city", "arrival_city",
    "distance_km", "emissions_co2", "departure_time", "agency_name",
}


def build_routes_filters(
    mode, source, departure_country, arrival_country,
    departure_city, arrival_city, departure_station, arrival_station,
    agency_name, is_night_train, min_distance_km, max_distance_km,
    min_co2, max_co2,
) -> tuple[str, dict]:
    """Construit les filtres WHERE communs à /routes et /routes/download."""
    conditions = ["1=1"]
    params: dict = {}

    if mode:
        conditions.append("mode = :mode")
        params["mode"] = mode
    if source:
        conditions.append("source = :source")
        params["source"] = source
    if departure_country:
        conditions.append("departure_country = :dep_country")
        params["dep_country"] = departure_country.upper()
    if arrival_country:
        conditions.append("arrival_country = :arr_country")
        params["arr_country"] = arrival_country.upper()
    if departure_city:
        conditions.append("departure_city ILIKE :dep_city")
        params["dep_city"] = f"%{departure_city}%"
    if arrival_city:
        conditions.append("arrival_city ILIKE :arr_city")
        params["arr_city"] = f"%{arrival_city}%"
    if departure_station:
        conditions.append("departure_station ILIKE :dep_station")
        params["dep_station"] = f"%{departure_station}%"
    if arrival_station:
        conditions.append("arrival_station ILIKE :arr_station")
        params["arr_station"] = f"%{arrival_station}%"
    if agency_name:
        conditions.append("agency_name ILIKE :agency")
        params["agency"] = f"%{agency_name}%"
    if is_night_train is not None:
        conditions.append("is_night_train = :is_night")
        params["is_night"] = is_night_train
    if min_distance_km is not None:
        conditions.append("distance_km >= :min_dist")
        params["min_dist"] = min_distance_km
    if max_distance_km is not None:
        conditions.append("distance_km <= :max_dist")
        params["max_dist"] = max_distance_km
    if min_co2 is not None:
        conditions.append("emissions_co2 >= :min_co2")
        params["min_co2"] = min_co2
    if max_co2 is not None:
        conditions.append("emissions_co2 <= :max_co2")
        params["max_co2"] = max_co2

    return " AND ".join(conditions), params


@router.get("/routes", summary="Consultation paginée des routes")
def get_routes(
    mode: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    departure_country: Optional[str] = Query(None),
    arrival_country: Optional[str] = Query(None),
    departure_city: Optional[str] = Query(None),
    arrival_city: Optional[str] = Query(None),
    departure_station: Optional[str] = Query(None),
    arrival_station: Optional[str] = Query(None),
    agency_name: Optional[str] = Query(None),
    is_night_train: Optional[bool] = Query(None),
    min_distance_km: Optional[float] = Query(None),
    max_distance_km: Optional[float] = Query(None),
    min_co2: Optional[float] = Query(None),
    max_co2: Optional[float] = Query(None),
    sort_by: Optional[str] = Query("departure_country", description=f"Colonnes autorisées : {SORTABLE_COLUMNS}"),
    sort_order: str = Query("asc", regex="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=500),
    db: Session = Depends(get_db),
):
    # Valide la colonne de tri (whitelist)
    if sort_by and sort_by not in SORTABLE_COLUMNS:
        raise HTTPException(
            status_code=422,
            detail=f"Colonne de tri invalide. Valeurs autorisées : {sorted(SORTABLE_COLUMNS)}",
        )

    sort_col = sort_by or "departure_country"
    where, params = build_routes_filters(
        mode, source, departure_country, arrival_country,
        departure_city, arrival_city, departure_station, arrival_station,
        agency_name, is_night_train, min_distance_km, max_distance_km,
        min_co2, None,
    )

    total = db.execute(text(f"SELECT COUNT(*) FROM gold_routes WHERE {where}"), params).scalar()

    params["limit"] = page_size
    params["offset"] = (page - 1) * page_size

    rows = db.execute(text(f"""
        SELECT trip_id, mode, source,
               departure_city, departure_country, departure_station, departure_time,
               arrival_city, arrival_country, arrival_station, arrival_time,
               agency_name, is_night_train, distance_km, emissions_co2, co2_per_pkm,
               days_of_week, service_start_date, service_end_date
        FROM gold_routes
        WHERE {where}
        ORDER BY {sort_col} {sort_order}
        LIMIT :limit OFFSET :offset
    """), params).mappings().all()

    return {"status": "ok", "count": len(rows), "total": total, "page": page, "page_size": page_size, "data": [dict(r) for r in rows]}


@router.get("/routes/download", summary="Export CSV de gold_routes")
def download_routes(
    mode: Optional[str] = Query(None),
    departure_country: Optional[str] = Query(None),
    arrival_country: Optional[str] = Query(None),
    departure_city: Optional[str] = Query(None),
    arrival_city: Optional[str] = Query(None),
    is_night_train: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
):
    where, params = build_routes_filters(
        mode, None, departure_country, arrival_country,
        departure_city, arrival_city, None, None, None,
        is_night_train, None, None, None, None,
    )

    # Détecte si on dépasse la limite de 500k lignes
    params["limit"] = 500_001
    rows = db.execute(text(f"SELECT * FROM gold_routes WHERE {where} LIMIT :limit"), params).mappings().all()

    if len(rows) > 500_000:
        raise HTTPException(
            status_code=413,
            detail="Le résultat dépasse 500 000 lignes. Affinez vos filtres (ex: departure_country, mode).",
        )

    # Génère le CSV en mémoire
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows([dict(r) for r in rows])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=routes_export.csv"},
    )


@router.get("/routes/search", summary="Recherche bidirectionnelle de trajets ferroviaires")
def search_routes(
    origin: str = Query(..., description="Ville ou gare d'origine (obligatoire)"),
    destination: str = Query(..., description="Ville ou gare de destination (obligatoire)"),
    is_night_train: Optional[bool] = Query(None),
    bidirectional: bool = Query(True, description="Recherche aussi le trajet retour"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=500),
    db: Session = Depends(get_db),
):
    params = {
        "origin": f"%{origin}%",
        "destination": f"%{destination}%",
        "limit": page_size,
        "offset": (page - 1) * page_size,
    }

    night_filter = ""
    if is_night_train is not None:
        night_filter = "AND is_night_train = :is_night"
        params["is_night"] = is_night_train

    query = text(f"""
        SELECT *, 'aller' AS direction FROM gold_routes
        WHERE mode = 'train'
          AND (departure_city ILIKE :origin OR departure_station ILIKE :origin)
          AND (arrival_city ILIKE :destination OR arrival_station ILIKE :destination)
          {night_filter}

        {'UNION ALL SELECT *, \'retour\' AS direction FROM gold_routes WHERE mode = \'train\' AND (departure_city ILIKE :destination OR departure_station ILIKE :destination) AND (arrival_city ILIKE :origin OR arrival_station ILIKE :origin) ' + night_filter if bidirectional else ''}

        ORDER BY direction, departure_time
        LIMIT :limit OFFSET :offset
    """)

    rows = db.execute(query, params).mappings().all()
    return {"status": "ok", "count": len(rows), "data": [dict(r) for r in rows]}


@router.get("/routes/{trip_id}", summary="Détail complet d'un trajet par trip_id")
def get_route_by_trip_id(
    trip_id: str,
    departure_country: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    params: dict = {"trip_id": trip_id}
    extra = "AND departure_country = :dep_country" if departure_country else ""
    if departure_country:
        params["dep_country"] = departure_country.upper()

    row = db.execute(
        text(f"SELECT * FROM gold_routes WHERE trip_id = :trip_id {extra} LIMIT 1"),
        params,
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"Trajet '{trip_id}' introuvable.")

    return {"status": "ok", "data": dict(row)}