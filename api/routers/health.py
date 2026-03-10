"""
Endpoint /health

Rôle : vérifier que l'API et ses dépendances sont opérationnelles.
Utilisé par Docker, Kubernetes, ou un outil de monitoring pour savoir
si le service est vivant et prêt à recevoir du trafic.

Retourne :
- 200 OK si tout va bien
- 503 Service Unavailable si la base de données est inaccessible
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse
import time
from api.database import check_db_connection

router = APIRouter(tags=["Santé"])


@router.get(
    "/health",
    summary="Vérification de l'état de l'API",
    description="Vérifie que l'API est opérationnelle et que la base de données est accessible.",
)
def health_check():
    db_ok = check_db_connection()

    status = "healthy" if db_ok else "degraded"
    http_code = 200 if db_ok else 503

    return JSONResponse(
        status_code=http_code,
        content={
            "status": status,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "version": "1.0.0",
            "checks": {
                "api": "ok",
                # "ok" si la DB répond, "error" sinon
                "database": "ok" if db_ok else "error",
            },
        },
    )