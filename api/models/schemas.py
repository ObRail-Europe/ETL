"""
Modèles Pydantic — schémas de validation des données.

Pydantic est la bibliothèque de validation de FastAPI.
Chaque modèle définit la structure attendue d'une requête ou d'une réponse.
FastAPI valide automatiquement les données entrantes et renvoie une erreur
422 Unprocessable Entity si elles ne correspondent pas au schéma.

Ces modèles servent aussi à générer la doc Swagger automatiquement.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Any
from datetime import date


# ── Modèle générique de réponse paginée ───────────────────────────────────────
class PaginatedResponse(BaseModel):
    """Format standard de toutes les réponses paginées de l'API."""
    status: str = "ok"
    count: int = Field(description="Nombre d'éléments dans cette page")
    total: Optional[int] = Field(None, description="Nombre total d'éléments")
    page: int = 1
    page_size: int = 25
    data: List[Any]


# ── Trajet ────────────────────────────────────────────────────────────────────
class TrajetResponse(BaseModel):
    """Représente un trajet dans gold_routes."""
    trip_id: Optional[str] = None
    mode: Optional[str] = None
    source: Optional[str] = None
    departure_city: Optional[str] = None
    departure_country: Optional[str] = None
    departure_station: Optional[str] = None
    departure_time: Optional[str] = None
    arrival_city: Optional[str] = None
    arrival_country: Optional[str] = None
    arrival_station: Optional[str] = None
    arrival_time: Optional[str] = None
    agency_name: Optional[str] = None
    is_night_train: Optional[bool] = None
    distance_km: Optional[float] = None
    emissions_co2: Optional[float] = None
    co2_per_pkm: Optional[float] = None
    days_of_week: Optional[str] = None
    service_start_date: Optional[date] = None
    service_end_date: Optional[date] = None

    class Config:
        # Permet de créer le modèle depuis un objet SQLAlchemy Row
        from_attributes = True


# ── Prédiction ML ─────────────────────────────────────────────────────────────
class PredictRequest(BaseModel):
    """
    Corps de la requête POST /predict.

    Ces champs correspondent aux features typiquement utilisées
    pour prédire le meilleur mode de transport.
    Adapte-les aux features réelles de ton modèle.
    """
    departure_city: str = Field(example="Paris")
    arrival_city: str = Field(example="Berlin")
    distance_km: float = Field(gt=0, example=1050.0, description="Distance en km (doit être > 0)")
    train_duration_min: Optional[float] = Field(None, example=480.0, description="Durée train en minutes")
    flight_duration_min: Optional[float] = Field(None, example=120.0, description="Durée vol en minutes")
    train_co2: Optional[float] = Field(None, example=4200.0, description="Émissions CO₂ train (gCO₂e)")
    flight_co2: Optional[float] = Field(None, example=127000.0, description="Émissions CO₂ avion (gCO₂e)")


class PredictResponse(BaseModel):
    """Résultat de la prédiction ML."""
    prediction: str = Field(description="Mode recommandé : 'train' ou 'flight'")
    confidence: float = Field(description="Score de confiance entre 0 et 1")
    input_features: dict = Field(description="Features utilisées pour la prédiction")
    model_version: str = Field(description="Version ou nom du fichier modèle utilisé")


# ── Stats volumes ─────────────────────────────────────────────────────────────
class VolumeStats(BaseModel):
    """Statistiques de volume de la base de données."""
    total_routes: int
    total_train: int
    total_flight: int
    total_comparisons: int
    countries_departure: int
    countries_arrival: int
    unique_dep_cities: int
    unique_arr_cities: int
    unique_agencies: int
    night_train_routes: int
    day_train_routes: int
    unclassified_routes: int
    distance_completeness_pct: float
    emissions_completeness_pct: float
    schedule_completeness_pct: float
    dep_city_completeness_pct: float
    arr_city_completeness_pct: float
    train_wins_total: int
    flight_wins_total: int