"""
Connection pool PostgreSQL pour l'API.

Utilise psycopg2.pool.ThreadedConnectionPool, initialisé au démarrage
de l'application via le lifespan FastAPI.
"""

from psycopg2.pool import ThreadedConnectionPool

from .config import ApiConfig

pool: ThreadedConnectionPool | None = None


def init_pool() -> None:
    """Crée le pool de connexions PostgreSQL."""
    global pool
    cfg = ApiConfig
    pool = ThreadedConnectionPool(
        minconn=cfg.DB_POOL_MIN,
        maxconn=cfg.DB_POOL_MAX,
        host=cfg.PG_HOST,
        port=int(cfg.PG_PORT),
        dbname=cfg.PG_DB,
        user=cfg.PG_USER,
        password=cfg.PG_PASSWORD,
    )


def close_pool() -> None:
    """Ferme toutes les connexions du pool."""
    global pool
    if pool:
        pool.closeall()
        pool = None


def get_db():
    """FastAPI dependency — yield une connexion psycopg2, rendue au pool après usage."""
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)
