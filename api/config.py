"""
Config de l'API REST ObRail Europe.

Lit les credentials PostgreSQL et les paramètres API depuis .env.
Suit le même pattern que src/chargement/config/settings.py.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class ApiConfig:
    """Configuration de l'API — PG credentials, pool, pagination, rate limit."""

    # PostgreSQL (mêmes variables que le pipeline ETL)
    PG_HOST: str = os.getenv("PG_HOST", "localhost")
    PG_PORT: str = os.getenv("PG_PORT", "5432")
    PG_DB: str = os.getenv("PG_DB", "obrail")
    PG_USER: str = os.getenv("PG_USER", "obrail")
    PG_PASSWORD: str = os.getenv("PG_PASSWORD", "")

    # Rate limiting — requêtes par minute par IP (0 = désactivé)
    RATE_LIMIT_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "100"))

    # Serveur
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))

    # Connection pool psycopg2
    DB_POOL_MIN: int = int(os.getenv("DB_POOL_MIN", "2"))
    DB_POOL_MAX: int = int(os.getenv("DB_POOL_MAX", "10"))

    # Pagination
    DEFAULT_PAGE_SIZE: int = 25
    MAX_PAGE_SIZE: int = 500

    # CSV export
    CSV_MAX_ROWS: int = 500_000

    # Authentification — Import endpoint
    IMPORT_TOKEN: str = os.getenv("API_IMPORT_TOKEN", "default_dev_token_change_in_production")
