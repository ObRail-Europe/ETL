"""
Config du chargement gold – chemins silver/gold, credentials PostgreSQL.

Tous les paramètres PostgreSQL sont lus depuis le .env pour ne pas
mettre de secrets dans le code.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

from common.config import BaseConfig

load_dotenv()


class ChargementConfig(BaseConfig):
    """
    Config de la phase 3 (chargement) – agrégation gold + push PostgreSQL.

    Hérite de BaseConfig pour les configs génériques (Spark, chemins data/logs).
    Ajoute :
    - Chemins des parquets silver en entrée (produits par la transformation)
    - Chemins des parquets gold en sortie
    - Credentials PostgreSQL (lus depuis .env)
    - Paramètres JDBC / coalesce
    """

    # ──────────────────────────────────────────────────────────────
    # Parquets silver (entrées – produits par la transformation)
    # ──────────────────────────────────────────────────────────────
    PROCESSED_PATH: Path = BaseConfig.DATA_ROOT / "processed"

    TRAIN_TRIP_PATH: Path  = PROCESSED_PATH / "train_trips.parquet"
    FLIGHT_PATH: Path      = PROCESSED_PATH / "flight.parquet"
    LOCALITE_PATH: Path    = PROCESSED_PATH / "localite.parquet"
    EMISSION_PATH: Path    = PROCESSED_PATH / "emission.parquet"
    STOP_MATCHING_PATH: Path = PROCESSED_PATH / "stop_matching.parquet"

    # ──────────────────────────────────────────────────────────────
    # Parquets gold (sorties intermédiaires avant push PG)
    # ──────────────────────────────────────────────────────────────
    GOLD_PATH: Path = BaseConfig.DATA_ROOT / "gold"

    GOLD_TRAIN_PATH:               Path = GOLD_PATH / "gold_routes_train.parquet"
    GOLD_FLIGHT_PATH:              Path = GOLD_PATH / "gold_routes_flight.parquet"
    GOLD_AGG_PATH:                 Path = GOLD_PATH / "gold_routes_agglomere.parquet"
    GOLD_COMPARE_BEST_PATH:        Path = GOLD_PATH / "gold_compare_best.parquet"

    # ──────────────────────────────────────────────────────────────
    # PostgreSQL – credentials lus depuis .env
    # ──────────────────────────────────────────────────────────────
    PG_HOST:     str = os.getenv("PG_HOST", "localhost")
    PG_PORT:     str = os.getenv("PG_PORT", "5432")
    PG_DB:       str = os.getenv("PG_DB", "obrail")
    PG_USER:     str = os.getenv("PG_USER", "obrail")
    PG_PASSWORD: str = os.getenv("PG_PASSWORD", "")

    # Jar JDBC PostgreSQL – à fournir via .env ou argument Spark
    PG_JDBC_JAR: str = os.getenv(
        "PG_JDBC_JAR",
        "/opt/postgresql/postgresql-42.7.3.jar"
    )

    @classmethod
    def pg_jdbc_url(cls) -> str:
        """Retourne l'URL JDBC prête à l'emploi."""
        return f"jdbc:postgresql://{cls.PG_HOST}:{cls.PG_PORT}/{cls.PG_DB}"

    @classmethod
    def pg_jdbc_properties(cls) -> dict[str, str]:
        """Retourne les propriétés JDBC pour spark.write.jdbc."""
        return {
            "user":   cls.PG_USER,
            "password": cls.PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }

    # ──────────────────────────────────────────────────────────────
    # Paramètres Spark / taille des sorties
    # ──────────────────────────────────────────────────────────────

    # Nombre de partitions Spark pour les gros DataFrames gold
    GOLD_SHUFFLE_PARTITIONS: int = int(os.getenv("GOLD_SHUFFLE_PARTITIONS", "200"))

    # Coalesce avant écriture parquet (1 fichier par table pour les petites, 40 pour les grandes)
    GOLD_COALESCE_AGG:           int = 40  # gold_routes_agglomere (gros)
    GOLD_COALESCE_TRAIN:         int = 40  # gold_routes_train
    GOLD_COALESCE_FLIGHT:        int = 10  # gold_routes_flight (petit)
    GOLD_COALESCE_BEST:          int = 20  # compare_best

    # Batch size JDBC pour l'insertion en base
    JDBC_BATCH_SIZE: int = int(os.getenv("JDBC_BATCH_SIZE", "10000"))

    # Chemin du script SQL d'initialisation du schéma PostgreSQL
    SQL_INIT_SCRIPT: Path = BaseConfig.PROJECT_ROOT / "scripts" / "init.sql"

    # Chemin du script SQL post-chargement (index, ANALYZE) – exécuté après JDBC
    SQL_POST_LOAD_SCRIPT: Path = BaseConfig.PROJECT_ROOT / "scripts" / "post_load.sql"

    # ──────────────────────────────────────────────────────────────
    # Constantes métier pour les agrégations
    # ──────────────────────────────────────────────────────────────

    # Vitesse de croisière avion (km/h) pour estimer la durée de vol
    FLIGHT_CRUISE_SPEED_KMH: float = 700.0
    # Temps fixe aéroport (embarquement + sécurité, en minutes)
    FLIGHT_AIRPORT_OVERHEAD_MIN: float = 75.0
    # Vitesse de déplacement vers/depuis l'aéroport (km/h)
    AIRPORT_ACCESS_SPEED_KMH: float = 40.0
    # Vitesse moyenne du train si horaires absents (km/h)
    TRAIN_DEFAULT_SPEED_KMH: float = 90.0

    @classmethod
    def validate(cls) -> None:
        """
        Crée les dossiers gold/ et vérifie les credentials PG minimaux.
        """
        super().validate()
        cls.GOLD_PATH.mkdir(parents=True, exist_ok=True)

        if not cls.PG_PASSWORD:
            print(
                "WARNING: PG_PASSWORD non défini. "
                "Le chargement PostgreSQL nécessite un mot de passe."
            )
        if not cls.SQL_INIT_SCRIPT.exists():
            print(
                f"WARNING: Script SQL introuvable : {cls.SQL_INIT_SCRIPT}"
            )


# validation automatique à l'import
ChargementConfig.validate()
