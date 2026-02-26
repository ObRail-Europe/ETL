"""
Gestionnaire de session Spark - init et cleanup centralisés.

Une seule session Spark partagée par toute la pipeline - évite de réinitialiser
Spark à chaque étape (lent et consomme beaucoup de RAM).
"""

import os
from typing import Any
from pyspark.sql import SparkSession

from .config import BaseConfig
from .logging import SparkLogger


class SparkManager:
    """
    Gestionnaire du cycle de vie de la session Spark.

    Une seule session Spark partagée par toute la pipeline pour éviter
    de réinitialiser Spark à chaque étape (lent et gourmand en RAM).
    Supporte le pattern context manager (with statement) pour cleanup automatique.

    Notes
    -----
    Utiliser get_spark() pour lazy loading ou init_spark_session() pour
    initialisation explicite. Ne pas oublier cleanup() en fin de script.
    """

    def __init__(
        self,
        app_name: str,
        config: BaseConfig = BaseConfig(),
        logger: SparkLogger | None = None
    ):
        """
        Initialise le gestionnaire Spark (sans créer la session tout de suite).

        Parameters
        ----------
        app_name : str
            Nom de l'application Spark (affiché dans le Spark UI)
        config : BaseConfig, optional
            Configuration de la pipeline (défaut: BaseConfig())
        logger : SparkLogger, optional
            Logger pour tracer les opérations Spark (défaut: None)

        Notes
        -----
        La session Spark n'est pas créée immédiatement - elle sera initialisée
        au premier appel à init_spark_session() ou get_spark().
        """
        self.app_name = app_name
        self.config = config
        self.logger = logger
        self.spark: SparkSession | None = None

    def init_spark_session(self) -> SparkSession:
        """
        Initialise la session Spark avec la configuration du projet.

        Returns
        -------
        SparkSession
            Session Spark configurée et prête à l'emploi

        Notes
        -----
        Applique toutes les configs définies dans BaseConfig.SPARK_CONFIG,
        définit JAVA_HOME si configuré, et réduit le niveau de log à WARN
        pour éviter le spam dans la console.
        """
        # définit JAVA_HOME si configuré - obligatoire sur certains systèmes (macOS notamment)
        if self.config.JAVA_HOME:
            os.environ['JAVA_HOME'] = self.config.JAVA_HOME

        if self.logger:
            self.logger.info(f"Initialisation de Spark: {self.app_name}")

        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.config.SPARK_MASTER)

        # applique toutes les configs Spark du fichier de config
        for key, value in self.config.SPARK_CONFIG.items():
            builder = builder.config(key, value)

        self.spark = builder.getOrCreate()

        # coupe le spam des logs Spark - garde que les WARN et ERROR
        self.spark.sparkContext.setLogLevel("WARN")

        if self.logger:
            self.logger.info(f"Session Spark démarrée (version {self.spark.version})")

        return self.spark

    def get_spark(self) -> SparkSession:
        """
        Retourne la session Spark (lazy loading).

        Returns
        -------
        SparkSession
            Session Spark (initialisée automatiquement si nécessaire)

        Notes
        -----
        Utilise le pattern lazy loading : si la session n'existe pas encore,
        elle est créée automatiquement via init_spark_session().
        """
        if self.spark is None:
            self.spark = self.init_spark_session()
        return self.spark

    def cleanup(self) -> None:
        """
        Arrête la session Spark et libère la mémoire.

        Notes
        -----
        Important de l'appeler à la fin du script pour éviter que Spark
        reste en mémoire (surtout en environnement partagé ou notebook).
        """
        if self.spark:
            if self.logger:
                self.logger.info("Arrêt de la session Spark...")

            self.spark.stop()
            self.spark = None

            if self.logger:
                self.logger.info("Session Spark arrêtée")

    def __enter__(self):
        """
        Initialise Spark à l'entrée du bloc with.

        Returns
        -------
        SparkManager
            L'instance elle-même pour utilisation dans le bloc with
        """
        self.init_spark_session()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any):
        """
        Cleanup automatique à la sortie du bloc with.

        Parameters
        ----------
        exc_type : Any
            Type de l'exception levée (None si pas d'exception)
        exc_val : Any
            Valeur de l'exception levée (None si pas d'exception)
        exc_tb : Any
            Traceback de l'exception levée (None si pas d'exception)
        """
        self.cleanup()
