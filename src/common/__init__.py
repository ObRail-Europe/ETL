"""
Module commun partagé par tous les composants de la pipeline ETL ObRail.

Ce module contient les composants réutilisables :
- Configuration de base (Spark, chemins, environnement)
- Système de logging unifié (Python/JVM)
- Gestion des métadonnées et manifests
- Gestionnaire de session Spark
"""

from .config import BaseConfig
from .logging import SparkLogger, PerformanceMonitor
from .metadata import MetadataManager
from .spark_manager import SparkManager

__all__ = [
    'BaseConfig',
    'SparkLogger',
    'PerformanceMonitor',
    'MetadataManager',
    'SparkManager'
]
