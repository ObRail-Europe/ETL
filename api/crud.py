from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional
import models, schemas


# ── Récupérer tous les trajets (avec pagination) ──────────────────────────────
def get_trips(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.TrainTrip).offset(skip).limit(limit).all()


# ── Récupérer un trajet par ID ────────────────────────────────────────────────
def get_trip_by_id(db: Session, trip_id: int):
    return db.query(models.TrainTrip).filter(models.TrainTrip.id == trip_id).first()


# ── Recherche par ville de départ et/ou d'arrivée ────────────────────────────
def search_trips(
    db: Session,
    departure_city: Optional[str] = None,
    arrival_city: Optional[str] = None,
    date: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
):
    query = db.query(models.TrainTrip)

    if departure_city:
        query = query.filter(
            models.TrainTrip.departure_city.ilike(f"%{departure_city}%")
        )
    if arrival_city:
        query = query.filter(
            models.TrainTrip.arrival_city.ilike(f"%{arrival_city}%")
        )
    if date:
        query = query.filter(
            models.TrainTrip.service_start_date <= date,
            models.TrainTrip.service_end_date >= date,
        )

    return query.offset(skip).limit(limit).all()


# ── CO2 moyen train entre deux villes ────────────────────────────────────────
def get_avg_train_co2(db: Session, departure_city: str, arrival_city: str):
    result = (
        db.query(func.avg(models.TrainTrip.emissions_co2))
        .filter(
            models.TrainTrip.departure_city.ilike(f"%{departure_city}%"),
            models.TrainTrip.arrival_city.ilike(f"%{arrival_city}%"),
        )
        .scalar()
    )
    return result


# ── Statistiques globales ─────────────────────────────────────────────────────
def get_stats(db: Session):
    total = db.query(func.count(models.TrainTrip.id)).scalar()
    avg_co2 = db.query(func.avg(models.TrainTrip.emissions_co2)).scalar()
    max_co2 = db.query(func.max(models.TrainTrip.emissions_co2)).scalar()
    min_co2 = db.query(func.min(models.TrainTrip.emissions_co2)).scalar()
    avg_dist = db.query(func.avg(models.TrainTrip.distance_m)).scalar()

    return {
        "total_trips": total or 0,
        "avg_emissions_co2": avg_co2,
        "max_emissions_co2": max_co2,
        "min_emissions_co2": min_co2,
        "avg_distance_m": avg_dist,
    }


# ── Créer un trajet ───────────────────────────────────────────────────────────
def create_trip(db: Session, trip: schemas.TrainTripCreate):
    db_trip = models.TrainTrip(**trip.model_dump())
    db.add(db_trip)
    db.commit()
    db.refresh(db_trip)
    return db_trip


# ── Supprimer un trajet ───────────────────────────────────────────────────────
def delete_trip(db: Session, trip_id: int):
    db_trip = db.query(models.TrainTrip).filter(models.TrainTrip.id == trip_id).first()
    if db_trip:
        db.delete(db_trip)
        db.commit()
    return db_trip