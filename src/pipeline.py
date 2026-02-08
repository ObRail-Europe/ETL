"""
Pipeline ETL globale ObRail Europe - orchestrateur des 3 phases.

1. Extraction : télécharge les données brutes (4 sources : backontrack, eea, ourairports, mobility_database)
2. Transformation : nettoie, normalise, croise les données (pas encore implémenté)
3. Chargement : agrège pour l'API finale (pas encore implémenté)

Seule l'extraction est opérationnelle pour l'instant.
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Any

from common.config import BaseConfig
from common.logging import SparkLogger
from common.metadata import MetadataManager
from common.spark_manager import SparkManager


class ETLPipeline:
    """
    Orchestrateur principal de la pipeline ETL complète.

    Instancie Spark une seule fois et le partage entre toutes les phases
    (extraction, transformation, chargement) pour éviter de recréer
    des sessions à chaque fois - économise RAM et temps de setup.
    """

    def __init__(
        self,
        config: BaseConfig = BaseConfig(),
        log_file: Path | None = None
    ):
        """
        Initialise la pipeline avec Spark, logger et metadata manager.

        Parameters
        ----------
        config : BaseConfig, optional
            Config globale de la pipeline (chemins, Spark settings, etc.)
        log_file : Path, optional
            Fichier de log personnalisé. Si None, génère un nom horodaté
        """
        self.config = config

        if not log_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = config.LOGS_PATH / f"pipeline_{timestamp}.log"

        # Spark initialisé une fois pour toutes les phases - économise temps et RAM
        self.spark_manager = SparkManager(
            app_name="ObRail Europe - ETL Pipeline",
            config=config
        )
        self.spark = self.spark_manager.init_spark_session()

        self.logger = SparkLogger(
            name="ObRail",
            spark=self.spark,
            log_file=log_file
        )

        self.metadata_manager = MetadataManager(config.DATA_ROOT)

    def run_extraction(self) -> dict[str, Any]:
        """
        Lance la phase 1 - téléchargement des données brutes.

        Returns
        -------
        dict[str, Any]
            Manifest d'extraction avec statut et métriques de chaque source
        """
        
        self.logger.log_section("PHASE 1 : PIPELINE D'EXTRACTION", level="INFO")

        from extraction.main_extraction import RawDataIngestor
        from extraction.config.settings import ExtractionConfig

        # crée l'ingestor avec les dépendances de la pipeline
        ingestor = RawDataIngestor(
            spark=self.spark,
            spark_manager=self.spark_manager,
            logger=self.logger.get_child("Extraction"),
            config=self.config,
            extraction_config=ExtractionConfig()
        )

        # enregistre et lance tous les extracteurs
        ingestor.register_extractors()
        manifest = ingestor.run()

        return manifest

    def run_transformation(self) -> dict[str, Any]:
        """
        Lance la phase 2 - nettoyage et normalisation.

        Returns
        -------
        dict[str, Any]
            Statut de la transformation (SKIPPED pour l'instant)

        Notes
        -----
        Pas encore implémenté. Devra nettoyer les doublons, harmoniser les formats
        (codes gares, fuseaux horaires), croiser GTFS avec Back-on-Track.
        """
        self.logger.log_section("PHASE 2 : TRANSFORMATION", level="INFO")
        self.logger.info("Transformation non encore implémentée")

        return {
            'status': 'SKIPPED',
            'message': 'Module de transformation non encore implémenté'
        }

    def run_loading(self) -> dict[str, Any]:
        """
        Lance la phase 3 - agrégation finale pour l'API.

        Returns
        -------
        dict[str, Any]
            Statut du chargement (SKIPPED pour l'instant)

        Notes
        -----
        Pas encore implémenté. Devra agréger les données pour l'API
        (routes jour/nuit, gares, émissions train vs avion).
        """
        self.logger.log_section("PHASE 3 : CHARGEMENT", level="INFO")
        self.logger.info("Chargement non encore implémenté")

        return {
            'status': 'SKIPPED',
            'message': 'Module de chargement non encore implémenté'
        }

    def run(self, phases: list[str] | None = None) -> dict[str, Any]:
        """
        Lance la pipeline complète ou certaines phases seulement.

        Parameters
        ----------
        phases : list[str], optional
            Phases à exécuter parmi ['extraction', 'transform', 'load']
            Si None, lance toutes les phases

        Returns
        -------
        dict[str, Any]
            Statut global et résultats de chaque phase
        """
        if phases is None:
            phases = ['extraction', 'transform', 'load']

        self.logger.log_section("PIPELINE ETL OBRAIL EUROPE", level="INFO")
        self.logger.info(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Version Spark: {self.spark.version}")
        self.logger.info(f"Phases à exécuter: {', '.join(phases)}")
        self.logger.info("")

        results: dict[str, Any] = {}

        # exécute les phases demandées dans l'ordre
        if 'extraction' in phases:
            results['extraction'] = self.run_extraction()

        if 'transform' in phases:
            results['transform'] = self.run_transformation()

        if 'load' in phases:
            results['load'] = self.run_loading()

        # calcule le statut global - SUCCESS si tout passe, PARTIAL si au moins 1 réussit
        statuses = [phase_result.get('status', 'UNKNOWN') for phase_result in results.values()]

        if all(s == 'SUCCESS' for s in statuses):
            overall_status = 'SUCCESS'
        elif any(s == 'SUCCESS' for s in statuses):
            overall_status = 'PARTIAL_SUCCESS'
        else:
            overall_status = 'FAILED'

        self.logger.log_section(f"PIPELINE TERMINÉE - {overall_status}", level="INFO")

        return {
            'status': overall_status,
            'phases': results
        }

    def cleanup(self) -> None:
        """
        Arrête Spark et libère la RAM.

        Notes
        -----
        Important de l'appeler à la fin pour éviter que Spark reste
        en mémoire après la fin du script (surtout en environnement partagé).
        """
        self.spark_manager.cleanup()


def main():
    """
    Point d'entrée CLI - parse les arguments et lance la pipeline.

    Notes
    -----
    Exit codes pour intégration Airflow/bash :
    - 0 : SUCCESS (tout OK)
    - 1 : PARTIAL_SUCCESS (au moins une phase OK)
    - 2 : FAILED (tout a échoué)
    - 130 : interruption utilisateur (SIGINT)
    """
    parser = argparse.ArgumentParser(
        description="Pipeline ETL ObRail Europe - Extraction, Transformation, Chargement"
    )

    parser.add_argument(
        '--phases',
        nargs='+',
        choices=['extraction', 'transform', 'load'],
        help='Phases à exécuter (défaut: toutes)'
    )

    parser.add_argument(
        '--log-file',
        type=str,
        help='Fichier de log personnalisé'
    )

    args = parser.parse_args()

    pipeline = ETLPipeline(
        log_file=Path(args.log_file) if args.log_file else None
    )

    try:
        result = pipeline.run(phases=args.phases)

        # Exit codes pour intégration Airflow/bash/cron
        # 0=tout OK, 1=partiel (ex: extraction OK mais transform SKIPPED), 2=échec total
        status = result['status']
        if status == 'SUCCESS':
            exit_code = 0
        elif status == 'PARTIAL_SUCCESS':
            exit_code = 1
        else:
            exit_code = 2

        sys.exit(exit_code)

    except KeyboardInterrupt:
        pipeline.logger.warning("Interruption par l'utilisateur")
        # 130 = exit code standard POSIX pour SIGINT (Ctrl+C)
        sys.exit(130)

    except Exception as e:
        pipeline.logger.critical(f"Erreur fatale: {e}")
        import traceback
        pipeline.logger.error(traceback.format_exc())
        sys.exit(1)

    finally:
        # Cleanup Spark même en cas d'erreur - évite les process zombies
        pipeline.cleanup()


if __name__ == '__main__':
    main()
