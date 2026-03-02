from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List

import models, schemas, crud
from database import engine, get_db

# Crée les tables si elles n'existent pas encore
models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="MSPR Transport CO2 API",
    description="API de comparaison des émissions CO2 entre Train et Avion",
    version="1.0.0",
)


# ── Healthcheck ───────────────────────────────────────────────────────────────
@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "message": "MSPR Transport CO2 API opérationnelle"}


# ── Trajets trains ────────────────────────────────────────────────────────────
@app.get("/trains", response_model=List[schemas.TrainTripOut], tags=["Trains"])
def list_trips(
    skip: int = 0,
    limit: int = Query(default=100, le=1000),
    db: Session = Depends(get_db),
):
    """Liste tous les trajets ferroviaires (avec pagination)."""
    return crud.get_trips(db, skip=skip, limit=limit)


@app.get("/trains/{trip_id}", response_model=schemas.TrainTripOut, tags=["Trains"])
def get_trip(trip_id: int, db: Session = Depends(get_db)):
    """Récupère un trajet ferroviaire par son ID."""
    trip = crud.get_trip_by_id(db, trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trajet introuvable")
    return trip


@app.get("/trains/search/", response_model=List[schemas.TrainTripOut], tags=["Trains"])
def search_trips(
    departure_city: Optional[str] = Query(None, description="Ville de départ"),
    arrival_city: Optional[str] = Query(None, description="Ville d'arrivée"),
    date: Optional[str] = Query(None, description="Date au format YYYY-MM-DD"),
    skip: int = 0,
    limit: int = Query(default=100, le=1000),
    db: Session = Depends(get_db),
):
    """Recherche des trajets par ville de départ, d'arrivée et/ou date de service."""
    return crud.search_trips(db, departure_city, arrival_city, date, skip, limit)


@app.post("/trains", response_model=schemas.TrainTripOut, status_code=201, tags=["Trains"])
def create_trip(trip: schemas.TrainTripCreate, db: Session = Depends(get_db)):
    """Ajoute un nouveau trajet ferroviaire."""
    return crud.create_trip(db, trip)


@app.delete("/trains/{trip_id}", tags=["Trains"])
def delete_trip(trip_id: int, db: Session = Depends(get_db)):
    """Supprime un trajet ferroviaire par son ID."""
    deleted = crud.delete_trip(db, trip_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Trajet introuvable")
    return {"detail": f"Trajet {trip_id} supprimé"}


# ── Comparateur CO2 ───────────────────────────────────────────────────────────
@app.get("/compare/co2", response_model=schemas.Co2ComparisonOut, tags=["Comparateur CO2"])
def compare_co2(
    departure_city: str = Query(..., description="Ville de départ"),
    arrival_city: str = Query(..., description="Ville d'arrivée"),
    plane_emissions_co2: Optional[float] = Query(
        None, description="Émissions CO2 avion (kg) — à fournir jusqu'à l'intégration des données avion"
    ),
    db: Session = Depends(get_db),
):
    """
    Compare les émissions CO2 entre le train et l'avion pour un trajet donné.
    Les émissions avion peuvent être passées manuellement en attendant l'intégration
    des données aériennes dans la base.
    """
    train_co2 = crud.get_avg_train_co2(db, departure_city, arrival_city)

    diff = None
    winner = None
    if train_co2 is not None and plane_emissions_co2 is not None:
        diff = round(plane_emissions_co2 - train_co2, 4)
        if train_co2 < plane_emissions_co2:
            winner = "train"
        elif train_co2 > plane_emissions_co2:
            winner = "plane"
        else:
            winner = "égalité"

    return schemas.Co2ComparisonOut(
        departure_city=departure_city,
        arrival_city=arrival_city,
        train_emissions_co2=round(train_co2, 4) if train_co2 else None,
        plane_emissions_co2=plane_emissions_co2,
        difference_co2=diff,
        winner=winner,
    )


# ── Statistiques globales ─────────────────────────────────────────────────────
@app.get("/stats", response_model=schemas.StatsOut, tags=["Statistiques"])
def get_stats(db: Session = Depends(get_db)):
    """Retourne des statistiques globales sur les émissions CO2 des trajets ferroviaires."""
    return crud.get_stats(db)