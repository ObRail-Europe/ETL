"""
Point d'entrée de l'API ObRail Europe.
Lance : uvicorn app.main:app --reload
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.routers import import_routes, referentiel, routes, carbon, analysis, quality_stats

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.PROJECT_VERSION,
    description=settings.PROJECT_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# ── CORS (ajuster les origines en production) ────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Enregistrement des routers ───────────────────────────────────────────────
PREFIX = settings.API_V1_PREFIX

app.include_router(import_routes.router,  prefix=PREFIX)
app.include_router(referentiel.router,    prefix=PREFIX)
app.include_router(routes.router,         prefix=PREFIX)
app.include_router(carbon.router,         prefix=PREFIX)
app.include_router(analysis.router,       prefix=PREFIX)
app.include_router(quality_stats.router,  prefix=PREFIX)


# ── Health check ─────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "version": settings.PROJECT_VERSION}


@app.get("/", tags=["Health"])
async def root():
    return {
        "name": settings.PROJECT_NAME,
        "version": settings.PROJECT_VERSION,
        "docs": "/docs",
        "redoc": "/redoc",
    }
