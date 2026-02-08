"""
Classe de base pour tous les extracteurs de données.

Chaque source (Mobility Database, Back-on-Track, etc.) hérite de cette classe.
Elle s'occupe du monitoring, logging et gestion d'erreurs - les extracteurs
n'ont qu'à implémenter la logique métier dans extract().
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from datetime import datetime

from pyspark.sql import SparkSession
from common.logging import SparkLogger, PerformanceMonitor
from extraction.config.settings import ExtractionConfig


class BaseExtractor(ABC):
    """
    Classe de base pour tous les extracteurs.

    Chaque source (Mobility Database, Back-on-Track, etc.) hérite de cette classe.
    Elle s'occupe du monitoring, logging et gestion d'erreurs - les extracteurs
    n'ont qu'à implémenter la logique métier dans extract().
    """

    def __init__(
        self,
        spark: SparkSession,
        logger: SparkLogger,
        config: ExtractionConfig,
        output_dir: Path | None = None
    ):
        """
        Initialise l'extracteur avec Spark, logger et config.

        Parameters
        ----------
        spark : SparkSession
            Session Spark pour les conversions CSV→Parquet si besoin
        logger : SparkLogger
            Logger racine (sera spécialisé par source via get_child)
        config : ExtractionConfig
            Configuration globale de l'extraction
        output_dir : Path, optional
            Répertoire de sortie (défaut: None, utilise get_default_output_dir())
        """
        self.spark = spark
        self.config = config

        # logger personnalisé par source : "ObRail.Extraction.BackOnTrack" au lieu de "ObRail.Extraction"
        logger_suffix = self.__class__.__name__.replace("Extractor", "")
        self.logger = logger.get_child(logger_suffix)

        self.output_dir = output_dir or self.get_default_output_dir()
        self.monitor = PerformanceMonitor(self.logger)

        # crée le dossier de sortie si absent
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # métadonnées collectées pendant l'extraction
        self.extraction_metadata: dict[str, Any]= {
            'source_name': self.get_source_name(),
            'start_time': None,
            'end_time': None,
            'status': 'NOT_STARTED',
            'files_downloaded': 0,
            'total_size_bytes': 0,
            'output_paths': [],
            'errors': []
        }
    
    @abstractmethod
    def get_source_name(self) -> str:
        """
        Retourne l'identifiant de la source.

        Returns
        -------
        str
            Nom de la source (ex: 'backontrack', 'mobility_database')
        """
        pass

    @abstractmethod
    def get_default_output_dir(self) -> Path:
        """
        Retourne le répertoire de sortie par défaut pour cette source.

        Returns
        -------
        Path
            Chemin du répertoire (ex: data/raw/backontrack/)
        """
        pass

    @abstractmethod
    def extract(self) -> dict[str, Any]:
        """
        Extrait les données depuis la source - à implémenter par chaque extracteur.

        Contient la logique métier spécifique : téléchargement, parsing,
        sauvegarde des fichiers bruts. Appelée automatiquement par run()
        qui gère le monitoring et les erreurs.

        Returns
        -------
        dict[str, Any]
            Stats d'extraction (nb fichiers, taille, chemins)

        Raises
        ------
        RuntimeError
            Si l'extraction échoue (téléchargement, parsing, I/O)
        """
        pass
    
    def run(self) -> dict[str, Any]:
        """
        Lance l'extraction en wrappant extract() avec monitoring et gestion d'erreurs.

        Méthode appelée par l'orchestrateur - elle appelle extract() et enregistre
        automatiquement les métriques (durée, taille, erreurs).

        Returns
        -------
        dict[str, Any]
            Métadonnées complètes (status, start_time, end_time, duration_seconds,
            files_downloaded, total_size_bytes, output_paths, errors)
        """
        source_name = self.get_source_name()

        self.extraction_metadata['start_time'] = datetime.now().isoformat()
        self.extraction_metadata['status'] = 'IN_PROGRESS'

        self.monitor.start(f'extract_{source_name}')

        try:
            # appel de la méthode extract() implémentée par la sous-classe
            result = self.extract()

            # fusionne les résultats dans les métadonnées
            self.extraction_metadata.update(result)
            self.extraction_metadata['status'] = 'SUCCESS'

            duration = self.monitor.stop(f'extract_{source_name}')
            self.extraction_metadata['duration_seconds'] = duration

            self.logger.info(f"Extraction de {self.__class__.__name__.replace('Extractor', '')} réussie")
            self._log_summary()

        except Exception as e:
            self.logger.error(f"Échec de l'extraction de la source {source_name}: {str(e)}")

            self.extraction_metadata['status'] = 'FAILED'
            self.extraction_metadata['errors'].append({
                'error_type': type(e).__name__,
                'error_message': str(e),
                'timestamp': datetime.now().isoformat()
            })

            duration = self.monitor.stop(f'extract_{source_name}')
            self.extraction_metadata['duration_seconds'] = duration

            # ne propage pas l'exception - permet aux autres sources de continuer
            # utile quand on extrait 10 sources et qu'une seule plante
            import traceback
            self.logger.debug(traceback.format_exc())

        finally:
            self.extraction_metadata['end_time'] = datetime.now().isoformat()

        return self.extraction_metadata
    
    def _log_summary(self) -> None:
        """Affiche un résumé des métriques d'extraction (nb fichiers, taille, durée)."""
        metrics: dict[str, str | float | int] = {
            'Fichiers téléchargés': self.extraction_metadata.get('files_downloaded', 0),
            'Taille totale': self._format_size(
                self.extraction_metadata.get('total_size_bytes', 0)
            ),
            'Durée': f"{self.extraction_metadata.get('duration_seconds', 0):.2f}s"
        }

        self.logger.log_metrics(metrics, prefix=f"l'extraction de {self.__class__.__name__.replace('Extractor', '')}")

    @staticmethod
    def _format_size(size_bytes: float) -> str:
        """
        Convertit une taille en bytes vers une unité lisible.

        Parameters
        ----------
        size_bytes : float
            Taille en bytes

        Returns
        -------
        str
            Taille formatée (ex: "1.23 GB")
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"

    def _get_file_size(self, file_path: Path) -> int:
        """
        Calcule la taille d'un fichier ou d'un répertoire complet.

        Parameters
        ----------
        file_path : Path
            Chemin du fichier ou répertoire

        Returns
        -------
        int
            Taille totale en bytes (somme récursive pour les répertoires)
        """
        if file_path.is_file():
            return file_path.stat().st_size
        elif file_path.is_dir():
            # somme récursive de tous les fichiers du répertoire
            return sum(f.stat().st_size for f in file_path.rglob('*') if f.is_file())
        return 0
    
    def _save_as_parquet(
        self,
        csv_path: Path,
        parquet_path: Path | None = None,
        delete_csv: bool = False
    ) -> Path:
        """
        Convertit un CSV en Parquet (format colonnaire plus rapide pour Spark).

        Parameters
        ----------
        csv_path : Path
            Chemin du fichier CSV source
        parquet_path : Path, optional
            Chemin de sortie (défaut: None, remplace juste l'extension)
        delete_csv : bool, optional
            Supprime le CSV après conversion pour économiser l'espace (défaut: False)

        Returns
        -------
        Path
            Chemin du fichier Parquet créé

        Notes
        -----
        Parquet divise par ~3 la taille du CSV et accélère les reads Spark.
        Utilise compression Snappy par défaut.
        """
        if parquet_path is None:
            parquet_path = csv_path.with_suffix('.parquet')

        self.logger.debug(f"Conversion CSV en Parquet de {self.__class__.__name__.replace("Extractor", "")}:{csv_path.name}")

        # lecture avec inférence automatique des types
        df = self.spark.read.csv(
            str(csv_path),
            header=True,
            inferSchema=True
        )

        # écriture en Parquet avec compression (snappy par défaut)
        df.write.mode(self.config.SAVE_MODE).parquet(
            str(parquet_path),
            compression=self.config.PARQUET_COMPRESSION
        )

        # supprime le CSV original si demandé (économie d'espace disque)
        if delete_csv and csv_path.exists():
            csv_path.unlink()
            self.logger.debug(f"CSV supprimé: {csv_path.name}")

        self.logger.debug(f"Conversion en parquet de {self.__class__.__name__.replace('Extractor', '')}:{parquet_path.name} terminée: {self._format_size(self._get_file_size(parquet_path))}"
                          f" (compression: ~{100 - (100 * self._get_file_size(parquet_path) / self._get_file_size(csv_path)):.1f}%)")

        return parquet_path