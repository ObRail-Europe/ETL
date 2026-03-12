"""
Dépendances FastAPI réutilisables : pagination, authentification.
"""

from fastapi import Query, HTTPException, Request
from starlette.status import HTTP_403_FORBIDDEN

from .config import ApiConfig


def pagination_params(
    page: int = Query(1, ge=1, description="Page courante"),
    page_size: int = Query(25, ge=1, le=500, description="Taille de la page (max 500)"),
) -> dict:
    """Retourne les paramètres de pagination validés."""
    return {
        "page": page,
        "page_size": page_size,
        "offset": (page - 1) * page_size,
    }


def verify_import_token(request: Request) -> None:
    """Valide le Bearer token pour l'accès à l'endpoint /import.
    
    Extrait le token depuis l'en-tête Authorization: Bearer <token>
    
    Args:
        request: Objet Request FastAPI contenant les headers
        
    Raises:
        HTTPException: 403 Forbidden si le token ne correspond pas ou est manquant
    """
    auth_header = request.headers.get("authorization", "")
    
    # Extraire le token depuis "Bearer <token>"
    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Invalid or missing import token",
        )
    
    token = auth_header[7:]  # Retirer "Bearer " (7 caractères)
    
    if token != ApiConfig.IMPORT_TOKEN:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Invalid or missing import token",
        )
