"""
Endpoints /trajets et /trajets/{id}

Ce router gère la consultation des trajets ferroviaires et aériens
depuis la table gold_routes de PostgreSQL.

Concepts clés :
- Query params : paramètres dans l'URL après "?" (?mode=train&page=2)
- Path params : paramètres dans l'URL (/trajets/FR_IC_123)
- Depends(get_db) : FastAPI injecte automatiquement la session DB
- HTTPException : renvoie une erreur HTTP avec le bon code et message
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional

from api.database import get_db
from api.models.schemas import PaginatedResponse

router = APIRouter(tags=["Trajets"])


@router.get(
    "/trajets",
    response_model=PaginatedResponse,
    summary="Liste des trajets",
    description="Consultation paginée des trajets ferroviaires et aériens avec filtres.",
)
def get_trajets(
    # ── Filtres optionnels ────────────────────────────────────────────────────
    mode: Optional[str] = Query(None, description="Mode de transport : 'train' ou 'flight'"),
    departure_country: Optional[str] = Query(None, description="Code pays départ (ex: FR)"),
    arrival_country: Optional[str] = Query(None, description="Code pays arrivée (ex: DE)"),
    departure_city: Optional[str] = Query(None, description="Ville de départ (recherche partielle)"),
    arrival_city: Optional[str] = Query(None, description="Ville d'arrivée (recherche partielle)"),
    is_night_train: Optional[bool] = Query(None, description="true = train de nuit, false = train de jour"),
    agency_name: Optional[str] = Query(None, description="Opérateur ferroviaire (ex: SNCF)"),
    min_distance_km: Optional[float] = Query(None, description="Distance minimale en km"),
    max_distance_km: Optional[float] = Query(None, description="Distance maximale en km"),
    # ── Pagination ────────────────────────────────────────────────────────────
    page: int = Query(1, ge=1, description="Numéro de page (commence à 1)"),
    page_size: int = Query(25, ge=1, le=500, description="Nombre de résultats par page (max 500)"),
    db: Session = Depends(get_db),
):
    # Validation du paramètre mode
    if mode and mode not in ("train", "flight"):
        raise HTTPException(
            status_code=422,
            detail="Le paramètre 'mode' doit être 'train' ou 'flight'.",
        )

    # ── Construction dynamique des filtres SQL ────────────────────────────────
    # On construit les conditions WHERE de façon sécurisée avec des paramètres
    # nommés (:param) pour éviter les injections SQL
    conditions = ["1=1"]
    params: dict = {}

    if mode:
        conditions.append("mode = :mode")
        params["mode"] = mode
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
    if is_night_train is not None:
        conditions.append("is_night_train = :is_night")
        params["is_night"] = is_night_train
    if agency_name:
        conditions.append("agency_name ILIKE :agency")
        params["agency"] = f"%{agency_name}%"
    if min_distance_km is not None:
        conditions.append("distance_km >= :min_dist")
        params["min_dist"] = min_distance_km
    if max_distance_km is not None:
        conditions.append("distance_km <= :max_dist")
        params["max_dist"] = max_distance_km

    where_clause = " AND ".join(conditions)

    # ── Requête de comptage (pour la pagination) ──────────────────────────────
    count_query = text(f"SELECT COUNT(*) FROM gold_routes WHERE {where_clause}")
    total = db.execute(count_query, params).scalar()

    # ── Requête principale ────────────────────────────────────────────────────
    offset = (page - 1) * page_size
    data_query = text(f"""
        SELECT
            trip_id, mode, source,
            departure_city, departure_country, departure_station, departure_time,
            arrival_city, arrival_country, arrival_station, arrival_time,
            agency_name, is_night_train, distance_km,
            emissions_co2, co2_per_pkm, days_of_week,
            service_start_date, service_end_date
        FROM gold_routes
        WHERE {where_clause}
        ORDER BY departure_country, departure_city
        LIMIT :limit OFFSET :offset
    """)
    params["limit"] = page_size
    params["offset"] = offset

    rows = db.execute(data_query, params).mappings().all()

    return {
        "status": "ok",
        "count": len(rows),
        "total": total,
        "page": page,
        "page_size": page_size,
        # dict(row) convertit chaque ligne SQL en dictionnaire Python
        "data": [dict(row) for row in rows],
    }


@router.get(
    "/trajets/{trip_id}",
    summary="Détail d'un trajet",
    description="Récupère toutes les informations d'un trajet par son identifiant unique.",
)
def get_trajet_by_id(
    trip_id: str,
    departure_country: Optional[str] = Query(
        None,
        description="Code pays départ — améliore les performances en ciblant la bonne partition PostgreSQL",
    ),
    db: Session = Depends(get_db),
):
    params: dict = {"trip_id": trip_id}
    condition = ""

    if departure_country:
        condition = "AND departure_country = :dep_country"
        params["dep_country"] = departure_country.upper()

    query = text(f"""
        SELECT *
        FROM gold_routes
        WHERE trip_id = :trip_id
        {condition}
        LIMIT 1
    """)

    row = db.execute(query, params).mappings().first()

    # Si aucun résultat : renvoie 404 avec un message clair
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Trajet '{trip_id}' introuvable.",
        )

    return {"status": "ok", "data": dict(row)}