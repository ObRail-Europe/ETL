"""
Configuration globale de l'API – chargée depuis les variables d'environnement
ou un fichier .env à la racine du projet.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Base de données ──────────────────────────────────────────────────
    # Variables cohérentes avec le docker-compose.yml du projet ETL (PG_*)
    PG_USER: str = "obrail"
    PG_PASSWORD: str = "obrail123"
    PG_HOST: str = "postgres"          # nom du service dans docker-compose
    PG_PORT: int = 5432
    PG_DB: str = "obrail"
    DB_ECHO: bool = False              # True = log toutes les requêtes SQL

    # DATABASE_URL peut être injectée directement (déjà fait dans docker-compose)
    # ou reconstruite depuis les variables PG_*
    DATABASE_URL: str = ""

    def model_post_init(self, __context: object) -> None:
        """Construit DATABASE_URL depuis les PG_* si elle n'est pas fournie."""
        if not self.DATABASE_URL:
            object.__setattr__(
                self,
                "DATABASE_URL",
                f"postgresql+asyncpg://{self.PG_USER}:{self.PG_PASSWORD}"
                f"@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DB}",
            )
        elif self.DATABASE_URL.startswith("postgresql://"):
            # docker-compose injecte postgresql:// → on convertit pour asyncpg
            object.__setattr__(
                self,
                "DATABASE_URL",
                self.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1),
            )

    # ── API ──────────────────────────────────────────────────────────────
    API_V1_PREFIX: str = "/api/v1"
    PROJECT_NAME: str = "ObRail Europe API"
    PROJECT_VERSION: str = "1.0.0"
    PROJECT_DESCRIPTION: str = (
        "API REST ObRail Europe – Dessertes ferroviaires et aériennes en Europe, "
        "comparaison CO₂ train vs avion, analyse jour/nuit."
    )

    # ── Sécurité (endpoints POST /import/*) ─────────────────────────────
    API_KEY: str = "changeme-obrail-secret"   # à surcharger en prod via .env

    # ── Pagination ───────────────────────────────────────────────────────
    DEFAULT_PAGE_SIZE: int = 25
    MAX_PAGE_SIZE: int = 500
    MAX_CSV_ROWS: int = 500_000


settings = Settings()
