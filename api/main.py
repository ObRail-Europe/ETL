"""
Point d'entrée de l'API REST ObRail Europe.

Lancement : uvicorn api.main:app --host 0.0.0.0 --port 8000
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from .config import ApiConfig
from .database import init_pool, close_pool
from .routers import (
    import_router,
    referential_router,
    search_router,
    consultation_router,
    carbon_router,
    analysis_router,
    quality_router,
    stats_router,
)

# ── Rate limiter (par IP, 100 req/min par défaut) ─────────────────────────────
_rate_limit = f"{ApiConfig.RATE_LIMIT_PER_MINUTE}/minute"
limiter = Limiter(key_func=get_remote_address, default_limits=[_rate_limit])


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise le pool DB au démarrage, le ferme à l'arrêt."""
    init_pool()
    yield
    close_pool()


app = FastAPI(
    title="ObRail Europe API",
    description="API de consultation des données ferroviaires et aériennes européennes.",
    version="1.0.0",
    lifespan=lifespan,
)

# Rate limiting middleware (appliqué à toutes les routes sauf exemption)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PREFIX = "/api/v1"

# search_router AVANT consultation_router pour que /routes/search
# matche avant /routes/{trip_id}
app.include_router(import_router.router, prefix=PREFIX, tags=["Import"])
app.include_router(referential_router.router, prefix=PREFIX, tags=["Référentiel"])
app.include_router(search_router.router, prefix=PREFIX, tags=["Recherche"])
app.include_router(consultation_router.router, prefix=PREFIX, tags=["Consultation"])
app.include_router(carbon_router.router, prefix=PREFIX, tags=["Carbone"])
app.include_router(analysis_router.router, prefix=PREFIX, tags=["Analyse Jour/Nuit"])
app.include_router(quality_router.router, prefix=PREFIX, tags=["Qualité"])
app.include_router(stats_router.router, prefix=PREFIX, tags=["Statistiques"])


@app.get(f"{PREFIX}/health", tags=["Santé"])
@limiter.exempt
def health_check():
    """Vérification de santé de l'API — exemptée du rate limiting."""
    return {"status": "ok"}
