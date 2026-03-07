"""
Classe de base pour tous les transformateurs de données

Chaque transformateur (MDB, BOTN, Ember, ADEME, OurAirport, Merge) hérite de cette classe
Elle gère le monitoring, le logging et la propagation d'erreurs - les transformateurs ont juste à implémenter la logique métier dans transform()
"""

import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession, DataFrame

from common.logging import SparkLogger, PerformanceMonitor
from transformation.config.settings import TransformationConfig


class BaseTransformer(ABC):

    def __init__(
        self,
        spark: SparkSession,
        logger: SparkLogger,
        config: TransformationConfig
    ):
        """
        Initialise le transformateur avec les dépendances injectées

        Parameters
        ----------
        spark : SparkSession
            Session Spark partagée par toute la pipeline
        logger : SparkLogger
            Logger racine, spécialisé par transformateur avec get_child()
        config : TransformationConfig
            Config globale de la transformation
        """
        self.spark = spark
        self.config = config

        # logger perso par transformateur : "ObRail.Transformation.MobilityDatabase"
        logger_suffix = self.__class__.__name__.replace("Transformer", "")
        self.logger = logger.get_child(logger_suffix)

        self.monitor = PerformanceMonitor(self.logger)

        # métadonnées collectées pendant la transformationi
        self.transformation_metadata: dict[str, Any] = {
            'transformer_name': self.get_transformer_name(),
            'start_time': None,
            'end_time': None,
            'status': 'NOT_STARTED',
            'rows_output': 0,
            'duration_seconds': 0.0,
            'errors': []
        }

    @abstractmethod
    def get_transformer_name(self) -> str:
        """
        Retourne l'identifiant du transformateur

        Returns
        -------
        str
            Nom du transformateur (ex: 'mobility_database', 'backontrack')
        """
        pass

    @abstractmethod
    def transform(self, **kwargs: Any) -> dict[str, DataFrame]:
        """
        Implémente la logique de transformation - à définir dans chaque sous-classe

        Contient la logique métier spécifique : jointures, filtres, calculs,
        enrichissement. Appelée automatiquement par run() qui gère monitoring et erreurs

        Parameters
        ----------
        **kwargs : Any
            DataFrames d'entrée passés par l'orchestrateur

        Returns
        -------
        dict[str, DataFrame]
            DataFrames produits, identifiés par nom (ex: {'df_mdb': df})

        Raises
        ------
        RuntimeError
            Si la transformation échoue (données manquantes, schéma inattendu)
        """
        pass

    def run(self, **kwargs: Any) -> dict[str, Any]:
        """
        Lance la transformation en wrappant transform() avec monitoring et gestion d'erreurs

        Méthode appelée par l'orchestrateur - elle appelle transform() et
        enregistre automatiquement les métriques (durée, statut, erreurs)

        Parameters
        ----------
        **kwargs : Any
            DataFrames d'entrée transmis à transform()

        Returns
        -------
        dict[str, Any]
            Métadonnées complètes avec les DataFrames produits sous la clé 'dataframes'

        Raises
        ------
        Exception
            Propage l'exception d'origine si la transformation échoue
            Contrairement à l'extraction (parallèle), la transformation est
            séquentielle - un échec bloque la suite.
        """
        transformer_name = self.get_transformer_name()

        self.transformation_metadata['start_time'] = datetime.now().isoformat()
        self.transformation_metadata['status'] = 'IN_PROGRESS'

        self.monitor.start(f'transform_{transformer_name}')

        try:
            result_dfs = self.transform(**kwargs)

            self.transformation_metadata['status'] = 'SUCCESS'
            self.transformation_metadata['dataframes'] = result_dfs

            duration = self.monitor.stop(f'transform_{transformer_name}')
            self.transformation_metadata['duration_seconds'] = duration

            self.logger.info(
                f"Transformation {self.__class__.__name__.replace('Transformer', '')} réussie"
            )
            self._log_summary()

        except Exception as e:
            self.logger.error(
                f"Échec de la transformation {transformer_name}: {str(e)}"
            )

            self.transformation_metadata['status'] = 'FAILED'
            self.transformation_metadata['errors'].append({
                'error_type': type(e).__name__,
                'error_message': str(e),
                'timestamp': datetime.now().isoformat()
            })

            duration = self.monitor.stop(f'transform_{transformer_name}')
            self.transformation_metadata['duration_seconds'] = duration

            # propage l'exception : la transformation est séquentielle,
            # un echec bloque les étapes suivantes
            self.logger.debug(traceback.format_exc())
            raise

        finally:
            self.transformation_metadata['end_time'] = datetime.now().isoformat()

        return self.transformation_metadata

    def _log_summary(self) -> None:
        """affiche un résumé des métriques de transfo (durée, statut)"""
        metrics: dict[str, str | float | int] = {
            'Durée': f"{self.transformation_metadata.get('duration_seconds', 0):.2f}s",
            'Statut': self.transformation_metadata.get('status', 'UNKNOWN'),
        }

        rows_out = self.transformation_metadata.get('rows_output', 0)
        if rows_out:
            metrics['Lignes produites'] = rows_out

        self.logger.log_metrics(
            metrics,
            prefix=f"la transformation {self.__class__.__name__.replace('Transformer', '')}"
        )
