"""
Configuration centralisée de l'API.

Les valeurs sont lues depuis les variables d'environnement,
avec des valeurs par défaut identiques à celles du docker-compose.yml.

Usage :
    from api.config import settings
    print(settings.pg_host)
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # ── Base de données ────────────────────────────────────────────────────────
    # Mêmes noms que dans docker-compose.yml
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "obrail"
    pg_user: str = "obrail"
    pg_password: str = "obrail123"

    @property
    def database_url(self) -> str:
        """Construit l'URL de connexion PostgreSQL pour SQLAlchemy."""
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )

    # ── Sécurité ───────────────────────────────────────────────────────────────
    # Clé API pour les endpoints d'import (POST)
    # À définir dans .env : API_KEY=ma_cle_secrete
    api_key: str = "changeme-set-in-env"

    # ── Modèle ML ─────────────────────────────────────────────────────────────
    # Chemin vers le fichier modèle sauvegardé (.joblib ou .pkl)
    # À définir dans .env quand le modèle est disponible : MODEL_PATH=/app/models/model.joblib
    model_path: str = "models/model.joblib"

    # ── Pagination ────────────────────────────────────────────────────────────
    default_page_size: int = 25
    max_page_size: int = 500

    class Config:
        # Lit automatiquement le fichier .env à la racine du projet
        env_file = ".env"
        env_file_encoding = "utf-8"
        # Permet les variables d'env sans préfixe (PG_HOST, pas OBRAIL_PG_HOST)
        case_sensitive = False


# lru_cache évite de recharger le .env à chaque requête
@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
