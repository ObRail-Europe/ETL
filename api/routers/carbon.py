"""
Endpoints /carbon — Section 5 de la spec API

- GET /carbon/trip/{trip_id}   : bilan CO₂ d'un trajet
- GET /carbon/estimate         : estimation CO₂ pour une paire O/D
- GET /carbon/ranking          : classement paires par économie CO₂
- GET /carbon/factors          : facteurs d'émission (transparence)
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional

from api.database import get_db

router = APIRouter()


@router.get("/carbon/trip/{trip_id}", summary="Bilan carbone d'un trajet")
def carbon_trip(trip_id: str, db: Session = Depends(get_db)):
    row = db.execute(text("""
        SELECT trip_id, mode, departure_city, arrival_city,
               departure_country, arrival_country,
               distance_km, co2_per_pkm, emissions_co2, is_night_train
        FROM gold_routes WHERE trip_id = :trip_id LIMIT 1
    """), {"trip_id": trip_id}).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"Trajet '{trip_id}' introuvable.")

    return {"status": "ok", "data": dict(row)}


@router.get("/carbon/estimate", summary="Estimation CO₂ train vs avion pour une paire O/D")
def carbon_estimate(
    origin: str = Query(..., description="Ville de départ"),
    destination: str = Query(..., description="Ville d'arrivée"),
    db: Session = Depends(get_db),
):
    rows = db.execute(text("""
        SELECT
            mode,
            COUNT(*) AS nb_options,
            ROUND(AVG(distance_km)::numeric, 1)    AS avg_distance_km,
            ROUND(AVG(co2_per_pkm)::numeric, 2)    AS avg_co2_per_pkm,
            ROUND(AVG(emissions_co2)::numeric, 1)  AS avg_emissions_co2,
            ROUND(MIN(emissions_co2)::numeric, 1)  AS min_emissions_co2,
            ROUND(MAX(emissions_co2)::numeric, 1)  AS max_emissions_co2
        FROM gold_routes
        WHERE departure_city ILIKE :origin
          AND arrival_city   ILIKE :destination
        GROUP BY mode
    """), {"origin": f"%{origin}%", "destination": f"%{destination}%"}).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail="Aucun trajet trouvé pour cette paire O/D.")

    comparison = [dict(r) for r in rows]

    # Calcul de l'économie CO₂ train vs avion côté applicatif
    train = next((r for r in comparison if r["mode"] == "train"), None)
    flight = next((r for r in comparison if r["mode"] == "flight"), None)
    co2_saving_pct = None
    if train and flight and flight["avg_emissions_co2"]:
        co2_saving_pct = round((1 - train["avg_emissions_co2"] / flight["avg_emissions_co2"]) * 100, 1)

    return {
        "status": "ok",
        "origin": origin,
        "destination": destination,
        "comparison": comparison,
        "co2_saving_pct": co2_saving_pct,
    }


@router.get("/carbon/ranking", summary="Classement des paires O/D par économie CO₂")
def carbon_ranking(
    departure_country: Optional[str] = Query(None),
    min_distance_km: Optional[float] = Query(None),
    sort_by: str = Query("co2_saving_pct", pattern="^(co2_saving_pct|train_emissions_co2|flight_emissions_co2)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=500),
    db: Session = Depends(get_db),
):
    conditions = ["flight_emissions_co2 IS NOT NULL", "train_emissions_co2 IS NOT NULL", "flight_emissions_co2 > 0"]
    params: dict = {}
    if departure_country:
        conditions.append("departure_country = :dep_country"); params["dep_country"] = departure_country.upper()
    if min_distance_km is not None:
        conditions.append("train_distance_km >= :min_dist"); params["min_dist"] = min_distance_km

    where = " AND ".join(conditions)
    params["limit"] = page_size
    params["offset"] = (page - 1) * page_size

    rows = db.execute(text(f"""
        SELECT
            departure_city, departure_country, arrival_city, arrival_country,
            train_distance_km, train_emissions_co2, flight_distance_km, flight_emissions_co2,
            ROUND((1 - train_emissions_co2 / NULLIF(flight_emissions_co2, 0)) * 100, 1) AS co2_saving_pct,
            best_mode
        FROM gold_compare_best
        WHERE {where}
        ORDER BY {sort_by} DESC
        LIMIT :limit OFFSET :offset
    """), params).mappings().all()

    return {"status": "ok", "count": len(rows), "page": page, "page_size": page_size, "data": [dict(r) for r in rows]}


@router.get("/carbon/factors", summary="Facteurs d'émission CO₂ (transparence méthodologique)")
def carbon_factors(
    country: Optional[str] = Query(None),
    mode: Optional[str] = Query(None, regex="^(train|flight)$"),
    is_night_train: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
):
    conditions = ["co2_per_pkm IS NOT NULL"]
    params: dict = {}
    if country:
        conditions.append("departure_country = :country"); params["country"] = country.upper()
    if mode:
        conditions.append("mode = :mode"); params["mode"] = mode
    if is_night_train is not None:
        conditions.append("is_night_train = :is_night"); params["is_night"] = is_night_train

    where = " AND ".join(conditions)

    rows = db.execute(text(f"""
        SELECT departure_country AS country_code, mode, is_night_train,
               COUNT(*) AS nb_routes_using_factor,
               ROUND(AVG(co2_per_pkm)::numeric, 4)    AS avg_co2_per_pkm,
               ROUND(MIN(co2_per_pkm)::numeric, 4)    AS min_co2_per_pkm,
               ROUND(MAX(co2_per_pkm)::numeric, 4)    AS max_co2_per_pkm,
               ROUND(STDDEV(co2_per_pkm)::numeric, 4) AS stddev_co2_per_pkm,
               COUNT(DISTINCT co2_per_pkm)            AS distinct_factors
        FROM gold_routes
        WHERE {where}
        GROUP BY departure_country, mode, is_night_train
        ORDER BY departure_country, mode, is_night_train
    """), params).mappings().all()

    return {"status": "ok", "count": len(rows), "data": [dict(r) for r in rows]}