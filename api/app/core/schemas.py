"""
Schémas Pydantic partagés entre tous les routers.
- PaginatedResponse  : enveloppe standard pour les listes
- ImportResponse     : réponse standard pour les POST /import/*
- ErrorDetail        : détail d'une erreur de ligne lors d'un import
"""

from typing import Any, Generic, TypeVar
from pydantic import BaseModel, Field

T = TypeVar("T")


# ── Réponse paginée ──────────────────────────────────────────────────────────

class PaginatedResponse(BaseModel, Generic[T]):
    status: str = "ok"
    count: int
    total: int
    page: int
    page_size: int
    data: list[Any]


# ── Réponse import ───────────────────────────────────────────────────────────

class ErrorDetail(BaseModel):
    row: int | None = None
    message: str


class ImportResponse(BaseModel):
    status: str = "success"
    imported: int = 0
    skipped: int = 0
    errors: list[ErrorDetail] = Field(default_factory=list)
    duration_seconds: float | None = None


# ── Helpers pagination ────────────────────────────────────────────────────────

def pagination_params(page: int = 1, page_size: int = 25) -> dict[str, int]:
    """Retourne offset et limit pour SQLAlchemy."""
    from app.core.config import settings
    page_size = min(page_size, settings.MAX_PAGE_SIZE)
    return {"limit": page_size, "offset": (page - 1) * page_size}


# ── Whitelist colonnes triables (anti-injection SQL) ─────────────────────────

GOLD_ROUTES_SORTABLE = {
    "departure_country", "departure_city", "arrival_country", "arrival_city",
    "distance_km", "emissions_co2", "co2_per_pkm", "departure_time",
    "arrival_time", "agency_name", "mode", "is_night_train",
    "service_start_date", "service_end_date",
}

COMPARE_SORTABLE = {
    "departure_country", "departure_city", "arrival_country", "arrival_city",
    "train_distance_km", "train_duration_min", "flight_distance_km",
    "flight_duration_min", "train_emissions_co2", "flight_emissions_co2",
    "best_mode",
}

CARBON_RANKING_SORTABLE = {
    "co2_saving_pct", "train_emissions_co2", "flight_emissions_co2",
    "train_distance_km",
}


def safe_sort_col(col: str | None, whitelist: set[str], default: str) -> str:
    """Retourne la colonne de tri si dans la whitelist, sinon la valeur par défaut."""
    if col and col in whitelist:
        return col
    return default
