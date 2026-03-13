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

    # Ces chemins pointent vers les sorties normalisées de la phase de
    # transformation ; on les garde centralisés pour éviter les divergences.
    PROCESSED_PATH: Path = BaseConfig.DATA_ROOT / "processed"

    TRAIN_TRIP_PATH: Path  = PROCESSED_PATH / "train_trips.parquet"
    FLIGHT_PATH: Path      = PROCESSED_PATH / "flight.parquet"
    LOCALITE_PATH: Path    = PROCESSED_PATH / "localite.parquet"
    EMISSION_PATH: Path    = PROCESSED_PATH / "emission.parquet"
    STOP_MATCHING_PATH: Path = PROCESSED_PATH / "stop_matching.parquet"

    # Les sorties gold restent sur disque pour faciliter les reprises,
    # les audits manuels et le debug avant chargement PostgreSQL.
    GOLD_PATH: Path = BaseConfig.DATA_ROOT / "gold"

    GOLD_TRAIN_PATH:               Path = GOLD_PATH / "gold_routes_train.parquet"
    GOLD_FLIGHT_PATH:              Path = GOLD_PATH / "gold_routes_flight.parquet"
    GOLD_AGG_PATH:                 Path = GOLD_PATH / "gold_routes_agglomere.parquet"
    GOLD_COMPARE_BEST_PATH:        Path = GOLD_PATH / "gold_compare_best.parquet"

    # Les credentials viennent de l'environnement pour éviter de figer des
    # secrets dans le dépôt et simplifier les déploiements multi-env.
    PG_HOST:     str = os.getenv("PG_HOST", "localhost")
    PG_PORT:     str = os.getenv("PG_PORT", "5432")
    PG_DB:       str = os.getenv("PG_DB", "obrail")
    PG_USER:     str = os.getenv("PG_USER", "obrail")
    PG_PASSWORD: str = os.getenv("PG_PASSWORD", "")

    # Le JAR JDBC reste configurable pour s'adapter aux environnements locaux,
    # conteneurisés ou CI sans changer le code Python.
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

    # Cette valeur pilote le compromis entre parallélisme Spark et coût de
    # shuffle pendant les agrégations lourdes.
    GOLD_SHUFFLE_PARTITIONS: int = int(os.getenv("GOLD_SHUFFLE_PARTITIONS", "200"))

    # Le coalesce est ajusté par table pour éviter des milliers de petits
    # fichiers tout en conservant du parallélisme sur les volumes importants.
    GOLD_COALESCE_AGG:           int = 40  # gold_routes_agglomere (gros)
    GOLD_COALESCE_TRAIN:         int = 40  # gold_routes_train
    GOLD_COALESCE_FLIGHT:        int = 10  # gold_routes_flight (petit)
    GOLD_COALESCE_BEST:          int = 20  # compare_best

    # Le batch JDBC agit directement sur le débit d'écriture en base.
    JDBC_BATCH_SIZE: int = int(os.getenv("JDBC_BATCH_SIZE", "10000"))

    # Le schéma SQL est versionné hors code pour faciliter les revues et
    # permettre son exécution indépendante si nécessaire.
    SQL_INIT_SCRIPT: Path = BaseConfig.PROJECT_ROOT / "scripts" / "init.sql"

    # Ce script post-load isole les opérations coûteuses (index, ANALYZE)
    # afin d'optimiser la fenêtre de chargement principal.
    SQL_POST_LOAD_SCRIPT: Path = BaseConfig.PROJECT_ROOT / "scripts" / "post_load.sql"

    # Ces constantes métier assurent des estimations homogènes entre runs,
    # même quand certaines sources ne fournissent pas toutes les métriques.
    FLIGHT_CRUISE_SPEED_KMH: float = 700.0

    # Temps fixe aéroport (embarquement + sécurité, en minutes).
    FLIGHT_AIRPORT_OVERHEAD_MIN: float = 75.0

    # Vitesse de déplacement vers/depuis l'aéroport (km/h).
    AIRPORT_ACCESS_SPEED_KMH: float = 40.0

    # Vitesse moyenne du train si horaires absents (km/h).
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


# La validation à l'import permet d'échouer tôt si la configuration locale
# est incomplète, avant de lancer un job Spark potentiellement coûteux.
ChargementConfig.validate()
