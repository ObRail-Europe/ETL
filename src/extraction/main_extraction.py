"""
Orchestrateur d'extraction - lance les 4 sources (backontrack, eea, ourairports, mobility_database)
en parallèle via threads. Génère un manifest.json avec toutes les métriques.
"""

import concurrent.futures
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from common.logging import SparkLogger
from common.metadata import MetadataManager
from common.spark_manager import SparkManager
from common.config import BaseConfig

from extraction.extractors.base_extractor import BaseExtractor
from extraction.config.settings import ExtractionConfig
from extraction.extractors.backontrack_extractor import BackOnTrackExtractor
from extraction.extractors.eea_extractor import EEAExtractor
from extraction.extractors.ourairports_extractor import OurAirportsExtractor
from extraction.extractors.mobilitydatabase_extractor import MobilityDatabaseExtractor
from extraction.extractors.geonames_extractor import GeonamesExtractor


class RawDataIngestor:
    """
    Orchestrateur d'extraction - lance les extracteurs en parallèle.

    ATTENTION : cette classe est intégrée dans la pipeline ETL principale.
    À instancier via pipeline.py avec les ressources partagées.
    """

    def __init__(
        self,
        spark: SparkSession,
        spark_manager: SparkManager,
        logger: SparkLogger,
        config: BaseConfig,
        extraction_config: ExtractionConfig | None = None
    ):
        """
        Instancie l'orchestrateur avec les dépendances injectées par la pipeline.

        Parameters
        ----------
        spark : SparkSession
            Session Spark partagée par toute la pipeline
        spark_manager : SparkManager
            Manager du cycle de vie Spark
        logger : SparkLogger
            Logger pré-configuré pour l'extraction
        config : BaseConfig
            Config de base de la pipeline
        extraction_config : ExtractionConfig, optional
            Config spécifique à l'extraction (si None, charge ExtractionConfig par défaut)
        """
        self.spark = spark
        self.spark_manager = spark_manager
        self.logger = logger
        self.config = config
        self.extraction_config = extraction_config or ExtractionConfig()

        # MetadataManager trace tout : fichiers téléchargés, tailles, erreurs
        # utilise RAW_DATA_PATH de l'extraction pour sauvegarder le manifest.json
        self.metadata_manager = MetadataManager(self.extraction_config.RAW_DATA_PATH)

        self.extractors: list[BaseExtractor] = []
    
    def register_extractors(self, sources: list[str] | None = None) -> None:
        """
        Instancie les extracteurs demandés.

        Parameters
        ----------
        sources : list[str], optional
            Liste des sources à charger (ex: ['backontrack', 'eea'])
            Si None, charge toutes les sources disponibles
        """
        # 4 sources dispo : backontrack (trains nuit), eea (émissions),
        # ourairports (aéroports EU), mobility_database (feeds GTFS)
        available_extractors: dict[str, type[BaseExtractor]] = {
            'backontrack': BackOnTrackExtractor,
            'eea': EEAExtractor,
            'ourairports': OurAirportsExtractor,
            'mobility_database': MobilityDatabaseExtractor,
            'geonames': GeonamesExtractor
        }

        if sources is None:
            sources = list(available_extractors.keys())

        for source_name in sources:
            if source_name not in available_extractors:
                self.logger.warning(f"Source inconnue ignorée: {source_name}")
                continue

            extractor_class : type[BaseExtractor] = available_extractors[source_name]
            extractor = extractor_class(
                spark=self.spark,
                logger=self.logger,
                config=self.extraction_config
            )
            self.extractors.append(extractor)

            self.logger.debug(f"Source {source_name} enregistrée pour extraction")
    
    def run(self) -> dict[str, Any]:
        """
        Lance tous les extracteurs en parallèle et génère le manifest JSON.

        Returns
        -------
        dict[str, Any]
            Manifest complet avec statut global et métriques de chaque source
        """

        self.logger.info(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Nombre de sources: {len(self.extractors)}")
        self.logger.info("")

        sources_success = 0
        sources_failed = 0
        sources_skipped = 0

        # threads pour extraction = I/O réseau (requêtes HTTP, téléchargements)
        # par rapport au process : pas de calculs lourds CPU, donc GIL pas un problème et threads suffisent
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.extractors)) as executor:
            future_to_extractor: dict[concurrent.futures.Future[dict[str, Any]], BaseExtractor] = {
                executor.submit(extractor.run): extractor 
                for extractor in self.extractors
            }
            
            for future in concurrent.futures.as_completed(future_to_extractor):
                extractor = future_to_extractor[future]
                source_name = extractor.get_source_name()
                
                try:
                    metadata = future.result()

                    status = metadata.get('status', 'UNKNOWN')

                    # Métriques communes à toutes les sources
                    metrics = {
                        'files_downloaded': metadata.get('files_downloaded', 0),
                        'total_size_bytes': metadata.get('total_size_bytes', 0),
                        'duration_seconds': metadata.get('duration_seconds', 0),
                    }

                    # Métriques optionnelles - dépendent de la source
                    # (ex: total_rows pour EEA, feeds_total pour MobilityDatabase, by_country pour GTFS)
                    if 'total_rows' in metadata:
                        metrics['total_rows'] = metadata['total_rows']
                    if 'feeds_total' in metadata:
                        metrics['feeds_total'] = metadata['feeds_total']
                    if 'by_country' in metadata:
                        metrics['by_country'] = metadata['by_country']
                    
                    self.metadata_manager.add_source_metadata(
                        source_name=source_name,
                        status=status,
                        metrics=metrics,
                        output_paths=metadata.get('output_paths', []),
                        error_message=None if status == 'SUCCESS' else metadata.get('errors')
                    )
                    
                    if status == 'SUCCESS':
                        sources_success += 1
                    elif status == 'FAILED':
                        sources_failed += 1
                    else:
                        sources_skipped += 1
                    
                except Exception as e:
                    self.logger.error(f"Erreur critique pour {source_name}: {e}")
                    
                    self.metadata_manager.add_source_metadata(
                        source_name=source_name,
                        status='FAILED',
                        metrics={},
                        error_message=str(e)
                    )
                    
                    sources_failed += 1

        # Statut global : SUCCESS si tout passe, PARTIAL_SUCCESS si au moins une source OK,
        # FAILED si toutes ont échoué (dans ce cas faut investiguer config/réseau)
        if sources_failed == 0 and sources_skipped == 0:
            overall_status = 'SUCCESS'
        elif sources_success > 0:
            overall_status = 'PARTIAL_SUCCESS'
        else:
            overall_status = 'FAILED'
        
        global_metrics = {
            'sources_total': len(self.extractors),
            'sources_success': sources_success,
            'sources_failed': sources_failed,
            'sources_skipped': sources_skipped
        }
        
        self.metadata_manager.set_global_metrics(global_metrics)
        self.metadata_manager.finalize(overall_status)
        
        manifest_path = self.metadata_manager.save()
        
        self._print_summary(overall_status, global_metrics, manifest_path)
        
        return self.metadata_manager.get_manifest()
    
    def _print_summary(
        self,
        status: str,
        metrics: dict[str, Any],
        manifest_path: Path
    ) -> None:
        """
        Affiche le résumé final dans les logs.

        Parameters
        ----------
        status : str
            Statut global (SUCCESS, PARTIAL_SUCCESS, FAILED)
        metrics : dict[str, Any]
            Métriques globales (sources_total, sources_success, etc.)
        manifest_path : Path
            Chemin du manifest.json sauvegardé
        """
        self.logger.log_section("RÉSUMÉ DE L'EXÉCUTION", level="INFO")

        self.logger.info(f"Statut global: {status}")
        self.logger.log_metrics(metrics, prefix="global")
        self.logger.info(f"Manifest sauvegardé: {manifest_path}")

        self.logger.log_section(f"PIPELINE TERMINÉE - {status}", level="INFO")


if __name__ == '__main__':
    raise RuntimeError(
        "ERREUR: Ce module ne doit pas être exécuté directement!\n"
        "\n"
        "Le module d'extraction est intégré à la pipeline ETL principale.\n"
        "Utilisez plutôt:\n"
        "  python src/pipeline.py [--phases extraction ...]\n"
        "\n"
        "Pour plus d'informations, consultez src/pipeline.py"
    )