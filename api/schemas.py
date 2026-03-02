from pydantic import BaseModel
from typing import Optional
from datetime import date


# ── Schéma de lecture (réponse API) ──────────────────────────────────────────
class TrainTripOut(BaseModel):
    id: int
    source: Optional[str]
    trip_id: Optional[str]
    destination: Optional[str]
    trip_short_name: Optional[str]
    agency_name: Optional[str]
    agency_timezone: Optional[str]
    service_id: Optional[str]
    route_id: Optional[str]
    route_type: Optional[int]
    route_short_name: Optional[str]
    route_long_name: Optional[str]
    departure_station: Optional[str]
    departure_city: Optional[str]
    departure_country: Optional[str]
    departure_time: Optional[str]
    departure_parent_station: Optional[str]
    arrival_station: Optional[str]
    arrival_city: Optional[str]
    arrival_country: Optional[str]
    arrival_time: Optional[str]
    arrival_parent_station: Optional[str]
    service_start_date: Optional[date]
    service_end_date: Optional[date]
    days_of_week: Optional[str]
    is_night_train: Optional[bool]
    distance_m: Optional[float]
    co2_per_pkm: Optional[float]
    emissions_co2: Optional[float]

    class Config:
        from_attributes = True


# ── Schéma de création (entrée API) ──────────────────────────────────────────
class TrainTripCreate(BaseModel):
    source: Optional[str] = None
    trip_id: Optional[str] = None
    destination: Optional[str] = None
    trip_short_name: Optional[str] = None
    agency_name: Optional[str] = None
    agency_timezone: Optional[str] = None
    service_id: Optional[str] = None
    route_id: Optional[str] = None
    route_type: Optional[int] = None
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    departure_station: Optional[str] = None
    departure_city: Optional[str] = None
    departure_country: Optional[str] = None
    departure_time: Optional[str] = None
    departure_parent_station: Optional[str] = None
    arrival_station: Optional[str] = None
    arrival_city: Optional[str] = None
    arrival_country: Optional[str] = None
    arrival_time: Optional[str] = None
    arrival_parent_station: Optional[str] = None
    service_start_date: Optional[date] = None
    service_end_date: Optional[date] = None
    days_of_week: Optional[str] = None
    is_night_train: Optional[bool] = None
    distance_m: Optional[float] = None
    co2_per_pkm: Optional[float] = None
    emissions_co2: Optional[float] = None


# ── Schémas pour le comparateur CO2 ──────────────────────────────────────────
class Co2ComparisonOut(BaseModel):
    departure_city: str
    arrival_city: str
    train_emissions_co2: Optional[float]
    plane_emissions_co2: Optional[float]
    difference_co2: Optional[float]
    winner: Optional[str]   # "train" | "plane" | "égalité"


# ── Schémas pour les statistiques ────────────────────────────────────────────
class StatsOut(BaseModel):
    total_trips: int
    avg_emissions_co2: Optional[float]
    max_emissions_co2: Optional[float]
    min_emissions_co2: Optional[float]
    avg_distance_m: Optional[float]