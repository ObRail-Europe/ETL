"""
Gestionnaire de métadonnées - génère le manifest.json pour tracer les runs.

Collecte durée, taille des fichiers, erreurs de chaque source et sauvegarde tout en JSON.
Pratique pour débugger les runs passés et surveiller la prod.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any


class MetadataManager:
    """
    Gestionnaire de métadonnées pour tracer les exécutions de la pipeline.

    Collecte les métriques de chaque source (durée, taille fichiers, erreurs),
    agrège les statuts globaux, et sauvegarde le tout dans manifest.json.
    Pratique pour débugger les runs passés et monitorer la production.
    """

    def __init__(self, output_path: Path):
        """
        Initialise le manifest avec timestamp et ID d'exécution.

        Parameters
        ----------
        output_path : Path
            Répertoire où sauvegarder le manifest.json

        Notes
        -----
        Le manifest démarre en statut IN_PROGRESS et sera finalisé par finalize().
        """
        self.output_path = output_path
        self.output_path.mkdir(parents=True, exist_ok=True)

        self.manifest: dict[str, Any] = {
            'pipeline_version': '1.0.0',
            'pipeline_name': 'ObRail Europe - Staging Extraction',
            'execution_id': self._generate_execution_id(),
            'execution_start': datetime.now().isoformat(),
            'execution_end': None,
            'status': 'IN_PROGRESS',
            'sources': {},
            'global_metrics': {},
            'errors': []
        }

    @staticmethod
    def _generate_execution_id() -> str:
        """
        Génère un ID unique basé sur le timestamp.

        Returns
        -------
        str
            ID d'exécution au format YYYYMMDD_HHMMSS (ex: 20250207_143022)
        """
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def add_source_metadata(
        self,
        source_name: str,
        status: str,
        metrics: dict[str, Any],
        output_paths: list[str] | None = None,
        error_message: str | None = None
    ) -> None:
        """
        Enregistre les métriques d'exécution d'une source.

        Parameters
        ----------
        source_name : str
            Nom de la source (ex: 'backontrack', 'mobility_database')
        status : str
            Statut de l'extraction (SUCCESS, FAILED, ou SKIPPED)
        metrics : dict[str, Any]
            Métriques collectées (nb fichiers, taille, durée, etc.)
        output_paths : list[str], optional
            Liste des chemins des fichiers générés (défaut: None)
        error_message : str, optional
            Message d'erreur si l'extraction a échoué (défaut: None)

        Notes
        -----
        Si error_message est fourni, l'erreur est ajoutée à la fois dans
        les métadonnées de la source et dans la liste globale des erreurs.
        """
        source_metadata: dict[str, Any] = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'metrics': metrics,
            'output_paths': output_paths or [],
        }

        # si erreur, on l'enregistre à la fois dans la source et dans la liste globale d'erreurs
        if error_message:
            source_metadata['error'] = error_message
            self.manifest['errors'].append({
                'source': source_name,
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            })

        self.manifest['sources'][source_name] = source_metadata
    
    def set_global_metrics(self, metrics: dict[str, Any]) -> None:
        """
        Enregistre les métriques globales de l'exécution.

        Parameters
        ----------
        metrics : dict[str, Any]
            Métriques agrégées (sources_total, sources_success, sources_failed, etc.)
        """
        self.manifest['global_metrics'] = metrics

    def finalize(self, overall_status: str = 'SUCCESS') -> None:
        """
        Finalise le manifest en calculant la durée et agrégeant les statuts.

        Parameters
        ----------
        overall_status : str, optional
            Statut global de l'exécution (défaut: 'SUCCESS')

        Notes
        -----
        Calcule automatiquement la durée totale d'exécution et agrège
        le nombre de sources par statut (SUCCESS: 3, FAILED: 1, etc.).
        """
        self.manifest['execution_end'] = datetime.now().isoformat()
        self.manifest['status'] = overall_status

        # durée totale du run
        start = datetime.fromisoformat(self.manifest['execution_start'])
        end = datetime.fromisoformat(self.manifest['execution_end'])
        duration = (end - start).total_seconds()

        self.manifest['global_metrics']['total_duration_seconds'] = duration

        # compte combien de sources par statut (SUCCESS: 3, FAILED: 1, etc.)
        statuses: dict[str, int]= {}
        for source_meta in self.manifest['sources'].values():
            status = source_meta['status']
            statuses[status] = statuses.get(status, 0) + 1

        self.manifest['global_metrics']['sources_by_status'] = statuses
    
    def save(self, filename: str = "manifest.json") -> Path:
        """
        Sauvegarde le manifest en JSON avec indentation.

        Parameters
        ----------
        filename : str, optional
            Nom du fichier de sortie (défaut: "manifest.json")

        Returns
        -------
        Path
            Chemin complet du fichier manifest sauvegardé
        """
        manifest_path = self.output_path / filename

        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(self.manifest, f, indent=2, ensure_ascii=False)

        return manifest_path

    def get_manifest(self) -> dict[str, Any]:
        """
        Retourne une copie du manifest actuel.

        Returns
        -------
        dict[str, Any]
            Copie du dictionnaire manifest avec toutes les métriques
        """
        return self.manifest.copy()

    @staticmethod
    def load_manifest(manifest_path: Path) -> dict[str, Any]:
        """
        Charge un manifest.json existant depuis le disque.

        Parameters
        ----------
        manifest_path : Path
            Chemin vers le fichier manifest.json à charger

        Returns
        -------
        dict[str, Any]
            Contenu du manifest désérialisé

        Notes
        -----
        Pour analyser les runs passés et comparer les métriques.
        """
        with open(manifest_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    @staticmethod
    def format_size(size_bytes: float) -> str:
        """
        Convertit des bytes en unité lisible.

        Parameters
        ----------
        size_bytes : float
            Taille en bytes à convertir

        Returns
        -------
        str
            Taille formatée avec unité (ex: "1.43 MB", "523.00 KB")
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"

    @staticmethod
    def get_file_size(file_path: Path) -> int:
        """
        Calcule la taille d'un fichier ou dossier.

        Parameters
        ----------
        file_path : Path
            Chemin du fichier ou dossier à mesurer

        Returns
        -------
        int
            Taille en bytes (somme récursive pour les dossiers)
        """
        if file_path.is_file():
            return file_path.stat().st_size
        elif file_path.is_dir():
            return sum(f.stat().st_size for f in file_path.rglob('*') if f.is_file())
        return 0