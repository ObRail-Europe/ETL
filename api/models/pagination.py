"""
Modèles Pydantic pour les enveloppes de réponse API.
"""

from typing import Any

from pydantic import BaseModel


class PaginatedResponse(BaseModel):
    """Enveloppe standard pour les GET paginés."""
    status: str = "ok"
    count: int
    total: int
    page: int
    page_size: int
    data: list[Any]


class ImportResponse(BaseModel):
    """Enveloppe standard pour les POST /import/*."""
    status: str = "success"
    imported: int = 0
    skipped: int = 0
    errors: list[dict[str, Any]] = []
