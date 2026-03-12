"""
Authentification par API Key pour les endpoints d'import (POST).
La clé doit être passée dans le header : X-API-Key: <valeur>
"""

from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from app.core.config import settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(api_key: str | None = Security(api_key_header)) -> str:
    """
    Dependency FastAPI – vérifie que le header X-API-Key est présent et valide.
    À utiliser sur tous les endpoints POST /import/*.
    """
    if api_key is None or api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Clé API manquante ou invalide. Fournissez le header X-API-Key.",
        )
    return api_key
