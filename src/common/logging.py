"""
Logging pour la pipeline ObRail - gère à la fois Python et Spark (JVM).

Spark tourne sur la JVM avec son propre système de logs (Log4j), on bridge les deux
pour pas perdre la moitié des messages. Pratique pour débugger quand un job plante
sans raison apparente.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from pyspark.sql import SparkSession


class SparkLogger:
    """
    Logger dual Python/Spark pour capturer tous les messages.

    Écrit simultanément dans le logger Python (console + fichier) et dans
    le logger Spark Log4j (JVM) pour éviter de perdre des logs.
    Pour débugger quand un job Spark plante sans raison apparente.
    """

    def __init__(
        self,
        name: str,
        spark: SparkSession | None = None,
        log_file: Path | None = None,
        console_level: int = logging.INFO,
        file_level: int = logging.DEBUG,
        configure_handlers: bool = True
    ):
        """
        Initialise le logger avec handlers console et fichier.

        Parameters
        ----------
        name : str
            Nom du logger (ex: 'ObRail', 'ObRail.Extraction')
        spark : SparkSession, optional
            Session Spark pour bridge avec Log4j (défaut: None)
        log_file : Path, optional
            Chemin du fichier de log (défaut: None, pas de log fichier)
        console_level : int, optional
            Niveau de log pour la console (défaut: logging.INFO)
        file_level : int, optional
            Niveau de log pour le fichier (défaut: logging.DEBUG)
        configure_handlers : bool, optional
            Si True, configure les handlers (défaut: True)

        Notes
        -----
        Console en INFO pour ne pas polluer, fichier en DEBUG pour tout garder.
        Le bridge avec Log4j échoue silencieusement si Spark n'est pas disponible.
        """
        self.name = name
        self.spark = spark

        # logger Python classique
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # évite de dupliquer les handlers si on instancie plusieurs fois
        if configure_handlers and not self.logger.handlers:
            # logs console - format simple pour lire rapidement
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(console_level)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)

            # logs fichier - format détaillé avec fonction et ligne pour debugger
            if log_file:
                log_file.parent.mkdir(parents=True, exist_ok=True)
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_handler.setLevel(file_level)
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(levelname)-8s - %(name)35s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
                file_handler.setFormatter(file_formatter)
                self.logger.addHandler(file_handler)

        # branchement sur le logger Spark (Log4j) pour capturer les logs JVM
        self.spark_logger = None
        if spark:
            try:
                jvm: Any = spark._jvm  # pyright: ignore[reportPrivateUsage]
                log4j = jvm.org.apache.log4j
                self.spark_logger = log4j.LogManager.getLogger(name)

            except Exception:
                # pas grave si Log4j est absent, on aura juste les logs Python
                pass

    def get_child(self, suffix: str) -> 'SparkLogger':
        """
        Crée un sous-logger qui hérite de la configuration du parent.

        Parameters
        ----------
        suffix : str
            Suffixe à ajouter au nom (ex: 'Extraction' donne 'ObRail.Extraction')

        Returns
        -------
        SparkLogger
            Nouveau logger enfant avec nom hiérarchique

        Notes
        -----
        Permet de tracer chaque module de la pipeline.
        """
        return SparkLogger(
            name=f"{self.name}.{suffix}",
            spark=self.spark,
            configure_handlers=False  # le parent gère déjà console et fichier
        )

    def info(self, message: str) -> None:
        """
        Log un message INFO.

        Parameters
        ----------
        message : str
            Message à logger
        """
        self.logger.info(message, stacklevel=2)
        if self.spark_logger:
            self.spark_logger.info(message)

    def debug(self, message: str) -> None:
        """
        Log un message DEBUG.

        Parameters
        ----------
        message : str
            Message à logger
        """
        self.logger.debug(message, stacklevel=2)
        if self.spark_logger:
            self.spark_logger.debug(message)

    def warning(self, message: str) -> None:
        """
        Log un message WARNING.

        Parameters
        ----------
        message : str
            Message à logger
        """
        self.logger.warning(message, stacklevel=2)
        if self.spark_logger:
            self.spark_logger.warn(message)

    def error(self, message: str) -> None:
        """
        Log un message ERROR.

        Parameters
        ----------
        message : str
            Message à logger
        """
        self.logger.error(message, stacklevel=2)
        if self.spark_logger:
            self.spark_logger.error(message)

    def critical(self, message: str) -> None:
        """
        Log un message CRITICAL.

        Parameters
        ----------
        message : str
            Message à logger
        """
        self.logger.critical(message, stacklevel=2)
        if self.spark_logger:
            self.spark_logger.fatal(message)
    
    def log_section(self, title: str, level: str = "INFO") -> None:
        """
        Affiche un titre encadré de séparateurs.

        Parameters
        ----------
        title : str
            Titre de la section
        level : str, optional
            Niveau de log (défaut: "INFO")

        Notes
        -----
        80 caractères de séparation '=' pour délimiter visuellement les phases
        dans les logs - plus facile à scanner.
        """
        separator = "=" * 80
        log_method = getattr(self, level.lower(), self.info)
        log_method(separator)
        log_method(f"  {title}")
        log_method(separator)

    def log_metrics(self, metrics: dict[str, str | int | float], prefix: str = "") -> None:
        """
        Affiche des métriques formatées.

        Parameters
        ----------
        metrics : dict[str, str | int | float]
            Dictionnaire de métriques à afficher
        prefix : str, optional
            Préfixe optionnel (défaut: "", non utilisé actuellement)

        Notes
        -----
        Formate automatiquement les floats à 2 décimales et les gros entiers
        avec séparateurs de milliers (1000000 → 1,000,000).
        """
        formatted_parts: list[str] = []
        for key, value in metrics.items():

            # formatte les nombres pour qu'ils soient lisibles
            if isinstance(value, float):
                formatted_value = f"{value:.2f}"
            elif isinstance(value, int) and value > 1000:
                formatted_value = f"{value:,}"  # 1000000 devient 1,000,000
            else:
                formatted_value = str(value)

            formatted_parts.append(f"{key}: {formatted_value}")

        if formatted_parts:
            self.info("Récap de " + prefix + " : " + " | ".join(formatted_parts))


class PerformanceMonitor:
    """
    Chronomètre pour mesurer les temps d'exécution des opérations.

    Permet de tracker plusieurs opérations en parallèle et d'identifier
    les étapes lentes de la pipeline. Supporte le context manager (with).
    """

    def __init__(self, logger: SparkLogger):
        """
        Initialise le chronomètre.

        Parameters
        ----------
        logger : SparkLogger
            Logger pour afficher les durées mesurées
        """
        self.logger = logger
        self.start_times: dict[str, datetime]= {}
        self.metrics: dict[str, dict[str, Any]] = {}

    def start(self, operation: str) -> None:
        """
        Démarre le chronomètre pour une opération.

        Parameters
        ----------
        operation : str
            Identifiant unique de l'opération
        """
        self.start_times[operation] = datetime.now()
        self.logger.debug(f"Démarage du moniteur pour `{operation}`")

    def stop(self, operation: str) -> float:
        """
        Arrête le chronomètre et log la durée.

        Parameters
        ----------
        operation : str
            Identifiant de l'opération (doit correspondre au start())
        
        Returns
        -------
        float
            Durée en secondes (0.0 si l'opération n'a pas été démarrée)
        """
        if operation not in self.start_times:
            self.logger.warning(f"Opération '{operation}' non démarrée")
            return 0.0

        end_time = datetime.now()
        duration = (end_time - self.start_times[operation]).total_seconds()

        self.metrics[operation] = {
            'start_time': self.start_times[operation].isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration
        }

        self.logger.info(f"Tâche `{operation}` terminée en {duration:.2f}s")

        del self.start_times[operation]
        return duration

    def get_metrics(self) -> dict[str, dict[str, Any]]:
        """
        Retourne toutes les métriques collectées.

        Returns
        -------
        dict[str, dict[str, Any]]
            Dictionnaire des opérations avec start_time, end_time, duration_seconds
        """
        return self.metrics.copy()

    def __enter__(self):
        """
        Initialise le context manager.

        Returns
        -------
        PerformanceMonitor
            L'instance elle-même pour utilisation dans le bloc with
        """
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """
        Cleanup à la sortie du bloc with.

        Parameters
        ----------
        _exc_type : Any
            Type de l'exception (non utilisé)
        _exc_val : Any
            Valeur de l'exception (non utilisée)
        _exc_tb : Any
            Traceback de l'exception (non utilisé)

        Notes
        -----
        Arrête automatiquement tous les chronos en cours.
        """
        for operation in list(self.start_times.keys()):
            self.stop(operation)