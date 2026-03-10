"""
Endpoints /analysis/day-night — Section 6 de la spec API

- GET /analysis/day-night/coverage  : couverture jour/nuit par pays
- GET /analysis/day-night/emissions : émissions comparées jour vs nuit
- GET /analysis/day-night/compare   : comparaison pour une paire O/D
- GET /analysis/day-night/routes    : liste des routes classifiées
- GET /analysis/day-night/summary   : résumé européen
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional

from api.database import get_db

router = APIRouter()


@router.get("/analysis/day-night/coverage", summary="Couverture trains jour/nuit par pays")
def day_night_coverage(
    departure_country: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    params: dict = {}
    extra = ""
    if departure_country:
        extra = "AND departure_country = :dep_country"
        params["dep_country"] = departure_country.upper()

    rows = db.execute(text(f"""
        SELECT departure_country, is_night_train,
               COUNT(*)                          AS nb_routes,
               COUNT(DISTINCT agency_name)       AS nb_agencies,
               COUNT(DISTINCT departure_city)    AS nb_dep_cities,
               COUNT(DISTINCT arrival_city)      AS nb_arr_cities,
               COUNT(DISTINCT arrival_country)   AS nb_destination_countries,
               ROUND(AVG(distance_km)::numeric, 1)   AS avg_distance_km,
               ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2
        FROM gold_routes
        WHERE mode = 'train' {extra}
        GROUP BY departure_country, is_night_train
        ORDER BY departure_country, is_night_train
    """), params).mappings().all()

    return {"status": "ok", "count": len(rows), "data": [dict(r) for r in rows]}


@router.get("/analysis/day-night/emissions", summary="Comparaison émissions CO₂ jour vs nuit par pays")
def day_night_emissions(
    departure_country: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    params: dict = {}
    extra = ""
    if departure_country:
        extra = "AND departure_country = :dep_country"
        params["dep_country"] = departure_country.upper()

    rows = db.execute(text(f"""
        SELECT departure_country,
               ROUND(AVG(CASE WHEN is_night_train = false THEN co2_per_pkm END)::numeric, 2)   AS avg_co2_pkm_day,
               ROUND(AVG(CASE WHEN is_night_train = true  THEN co2_per_pkm END)::numeric, 2)   AS avg_co2_pkm_night,
               ROUND(AVG(CASE WHEN is_night_train = false THEN emissions_co2 END)::numeric, 1) AS avg_total_co2_day,
               ROUND(AVG(CASE WHEN is_night_train = true  THEN emissions_co2 END)::numeric, 1) AS avg_total_co2_night,
               ROUND(AVG(CASE WHEN is_night_train = false THEN distance_km END)::numeric, 1)   AS avg_dist_day,
               ROUND(AVG(CASE WHEN is_night_train = true  THEN distance_km END)::numeric, 1)   AS avg_dist_night
        FROM gold_routes
        WHERE mode = 'train' {extra}
        GROUP BY departure_country
        HAVING COUNT(CASE WHEN is_night_train = true THEN 1 END) > 0
        ORDER BY departure_country
    """), params).mappings().all()

    return {"status": "ok", "count": len(rows), "data": [dict(r) for r in rows]}


@router.get("/analysis/day-night/compare", summary="Comparaison jour vs nuit pour une paire O/D")
def day_night_compare(
    origin: str = Query(..., description="Ville de départ"),
    destination: str = Query(..., description="Ville d'arrivée"),
    db: Session = Depends(get_db),
):
    rows = db.execute(text("""
        SELECT is_night_train,
               COUNT(*) AS nb_options,
               ROUND(AVG(distance_km)::numeric, 1)   AS avg_distance_km,
               ROUND(AVG(co2_per_pkm)::numeric, 2)   AS avg_co2_per_pkm,
               ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
               ROUND(MIN(emissions_co2)::numeric, 1) AS min_emissions_co2,
               ROUND(MAX(emissions_co2)::numeric, 1) AS max_emissions_co2,
               MIN(departure_time) AS earliest_departure,
               MAX(departure_time) AS latest_departure
        FROM gold_routes
        WHERE mode = 'train'
          AND departure_city ILIKE :origin
          AND arrival_city   ILIKE :destination
        GROUP BY is_night_train
    """), {"origin": f"%{origin}%", "destination": f"%{destination}%"}).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail="Aucun trajet train trouvé pour cette paire O/D.")

    result: dict = {"origin": origin, "destination": destination}
    best_co2 = None
    best_mode_label = None

    for row in rows:
        data = dict(row)
        label = "night_train" if row["is_night_train"] else "day_train"
        result[label] = data
        co2 = row["avg_emissions_co2"]
        if co2 is not None and (best_co2 is None or co2 < best_co2):
            best_co2 = co2
            best_mode_label = "night" if row["is_night_train"] else "day"

    result["best"] = best_mode_label
    return {"status": "ok", "data": result}


@router.get("/analysis/day-night/routes", summary="Liste des routes avec classification jour/nuit")
def day_night_routes(
    departure_country: Optional[str] = Query(None),
    arrival_country: Optional[str] = Query(None),
    is_night_train: Optional[bool] = Query(None),
    agency_name: Optional[str] = Query(None),
    min_distance_km: Optional[float] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=500),
    db: Session = Depends(get_db),
):
    conditions = ["mode = 'train'"]
    params: dict = {}
    if departure_country:
        conditions.append("departure_country = :dep_country"); params["dep_country"] = departure_country.upper()
    if arrival_country:
        conditions.append("arrival_country = :arr_country"); params["arr_country"] = arrival_country.upper()
    if is_night_train is not None:
        conditions.append("is_night_train = :is_night"); params["is_night"] = is_night_train
    if agency_name:
        conditions.append("agency_name ILIKE :agency"); params["agency"] = f"%{agency_name}%"
    if min_distance_km is not None:
        conditions.append("distance_km >= :min_dist"); params["min_dist"] = min_distance_km

    where = " AND ".join(conditions)
    params["limit"] = page_size
    params["offset"] = (page - 1) * page_size

    rows = db.execute(text(f"""
        SELECT departure_city, departure_country, arrival_city, arrival_country,
               agency_name, route_long_name, is_night_train, days_of_week,
               departure_time, arrival_time, distance_km, co2_per_pkm, emissions_co2
        FROM gold_routes WHERE {where}
        ORDER BY departure_country, is_night_train, departure_time
        LIMIT :limit OFFSET :offset
    """), params).mappings().all()

    return {"status": "ok", "count": len(rows), "page": page, "page_size": page_size, "data": [dict(r) for r in rows]}


@router.get("/analysis/day-night/summary", summary="Résumé agrégé jour/nuit au niveau européen")
def day_night_summary(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT
            is_night_train,
            COUNT(*) AS total_routes,
            COUNT(DISTINCT departure_country) AS countries_served,
            COUNT(DISTINCT agency_name)       AS operators,
            COUNT(DISTINCT departure_city || '-' || arrival_city) AS unique_od_pairs,
            ROUND(AVG(distance_km)::numeric, 1)   AS avg_distance_km,
            ROUND(AVG(co2_per_pkm)::numeric, 2)   AS avg_co2_per_pkm,
            ROUND(AVG(emissions_co2)::numeric, 1) AS avg_emissions_co2,
            ROUND(SUM(emissions_co2)::numeric, 0) AS total_emissions_co2
        FROM gold_routes WHERE mode = 'train'
        GROUP BY is_night_train
    """)).mappings().all()

    return {"status": "ok", "data": [dict(r) for r in rows]}