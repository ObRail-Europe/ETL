"""
Config partagée pour toute la pipeline ETL.

Centralise les paramètres Spark et les chemins - évite les valeurs en dur partout.
Tout est overridable via .env pour les différents environnements (dev/prod).
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv

# charge le fichier .env à la racine du projet
load_dotenv()


class BaseConfig:
    """
    Configuration de base partagée par tous les modules de la pipeline ETL.

    Centralise les paramètres Spark (mémoire, shuffle, serialization), les chemins
    des dossiers (data, logs), et les options de stockage (Parquet). Tous les
    paramètres peuvent être surchargés via variables d'environnement (.env).

    Notes
    -----
    Les dossiers data/ et logs/ sont créés automatiquement lors de l'import
    du module via l'appel à validate() en fin de fichier.
    """

    # Chemins de base - tout relatif à la racine du projet
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    DATA_ROOT = PROJECT_ROOT / "data"
    LOGS_PATH = PROJECT_ROOT / "logs"

    # Spark en local par défaut - en prod on override avec yarn ou k8s
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

    # Java Home - obligatoire pour PySpark, Java 17 recommandé (Java 8 fonctionne aussi)
    JAVA_HOME = os.getenv("JAVA_HOME")

    # Config Spark - 4g suffit pour le dev, monter à 8-16g en prod selon les datasets
    SPARK_CONFIG: dict[str, str] = {
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
        "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"),
        "spark.sql.adaptive.enabled": os.getenv("SPARK_SQL_ADAPTIVE_ENABLED", "true"),
        "spark.sql.adaptive.coalescePartitions.enabled": os.getenv("SPARK_SQL_ADAPTIVE_COALESCEPARTITIONS_ENABLED", "true"),
        "spark.serializer": os.getenv("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer"),  # plus rapide que Java serializer
        "spark.eventLog.enabled": os.getenv("SPARK_EVENTLOG_ENABLED", "false"),  # activer en prod pour le monitoring
    }

    # permet d'ajouter des configs Spark custom via variable d'environnement
    _spark_extra_config = os.getenv("SPARK_EXTRA_CONFIG")
    if _spark_extra_config:
        try:
            SPARK_CONFIG.update(json.loads(_spark_extra_config))
        except json.JSONDecodeError:
            print("WARNING: SPARK_EXTRA_CONFIG n'est pas un JSON valide")

    # Stockage - Parquet divise par 3 la taille des CSV et accélère les reads
    DEFAULT_OUTPUT_FORMAT = "parquet"
    PARQUET_COMPRESSION = "snappy"  # bon compromis vitesse/compression (gzip compresse plus mais plus lent)
    SAVE_MODE = "overwrite"  # écrase les données existantes à chaque run

    # Traçabilité - manifest.json permet de tracker toutes les exécutions
    MANIFEST_FILENAME = "manifest.json"
    ENABLE_DETAILED_LOGGING = os.getenv("ENABLE_DETAILED_LOGGING", "true").lower() == "true"

    @classmethod
    def validate(cls) -> None:
        """
        Crée les dossiers data/ et logs/ s'ils n'existent pas.

        Notes
        -----
        Appelée automatiquement lors de l'import du module pour éviter
        les crashes au premier run. Utilise exist_ok=True pour être idempotente.
        """
        cls.DATA_ROOT.mkdir(parents=True, exist_ok=True)
        cls.LOGS_PATH.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_versioned_path(cls, base_path: Path) -> Path:
        """
        Crée un sous-dossier horodaté pour conserver l'historique des exécutions.

        Parameters
        ----------
        base_path : Path
            Chemin de base où créer le sous-dossier versionné

        Returns
        -------
        Path
            Chemin du dossier créé avec timestamp (ex: data/raw/20250207_143022/)

        Notes
        -----
        Évite d'écraser les données des runs précédents - pratique pour comparer
        les exécutions ou revenir en arrière en cas de problème.
        """
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        versioned_path = base_path / timestamp
        versioned_path.mkdir(parents=True, exist_ok=True)
        return versioned_path


# crée les dossiers automatiquement quand on importe le module
BaseConfig.validate()
