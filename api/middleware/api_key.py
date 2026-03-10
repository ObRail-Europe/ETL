"""
Middleware de sécurité par API Key.

Un middleware est un composant qui s'exécute AVANT chaque requête HTTP,
comme un videur à l'entrée d'une boîte : il vérifie les conditions d'accès
avant de laisser passer la requête vers l'endpoint.

Ce middleware protège uniquement les routes POST /import/*
(qui modifient les données en base).
Les endpoints GET sont publics.

Pour appeler un endpoint protégé :
    curl -X POST http://localhost:8000/api/v1/import/full \
         -H "X-API-Key: ma_cle_secrete"
"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from api.config import settings


# Routes qui nécessitent une API key
PROTECTED_PREFIXES = ["/api/v1/import/"]


class APIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Vérifie si la route est protégée
        is_protected = any(
            request.url.path.startswith(prefix)
            for prefix in PROTECTED_PREFIXES
        )

        if is_protected and request.method in ("POST", "PUT", "DELETE"):
            # Récupère la clé dans le header X-API-Key
            api_key = request.headers.get("X-API-Key")

            if not api_key:
                return JSONResponse(
                    status_code=401,
                    content={
                        "status": "error",
                        "message": "API key manquante. Ajoutez le header X-API-Key.",
                    },
                )

            if api_key != settings.api_key:
                return JSONResponse(
                    status_code=403,
                    content={
                        "status": "error",
                        "message": "API key invalide.",
                    },
                )

        # Laisse passer la requête vers l'endpoint
        response = await call_next(request)
        return response