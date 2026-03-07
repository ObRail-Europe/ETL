"""
Extracteur pour OurAirports (référentiels aéroportuaires mondiaux).

Source = plusieurs fichiers CSV statiques (aéroports, pistes, fréquences radio, etc.).
On utilise ThreadPoolExecutor pour télécharger tous les fichiers en parallèle.
"""

import requests
import pandas as pd
import concurrent.futures
from io import StringIO
from pathlib import Path
from typing import Any

from .base_extractor import BaseExtractor


class OurAirportsExtractor(BaseExtractor):
    """
    Extracteur pour les référentiels aéroportuaires OurAirports.

    Notes
    -----
    Plusieurs fichiers CSV statiques (airports, runways, frequencies, etc.).
    Utilise ThreadPoolExecutor pour télécharger tous les fichiers en parallèle.
    """

    def get_source_name(self) -> str:
        """
        Retourne l'identifiant de la source.

        Returns
        -------
        str
            Nom de la source ('ourairports')
        """
        return "ourairports"

    def get_default_output_dir(self) -> Path:
        """
        Retourne le répertoire de sortie par défaut.

        Returns
        -------
        Path
            Chemin du répertoire OurAirports
        """
        return self.config.OURAIRPORTS_OUTPUT_DIR

    def extract(self) -> dict[str, Any]:
        """
        Télécharge tous les référentiels CSV en parallèle.

        Returns
        -------
        dict[str, Any]
            Stats d'extraction (nb fichiers, lignes totales, taille, liste datasets)

        Raises
        ------
        RuntimeError
            Si un téléchargement ou la validation échoue
        """
        self.logger.info("Démarrage de l'extraction des données OurAirports depuis URL...")

        base_url = self.config.OURAIRPORTS_BASE_URL
        files_to_download = self.config.OURAIRPORTS_FILES

        output_paths: list[str] = []
        total_rows = 0
        total_size = 0

        # téléchargement parallèle avec threads - efficace car principalement de l'I/O réseau
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures: list[concurrent.futures.Future[dict[str, Any]]] = []
            for dataset, filename in files_to_download.items():
                url = f"{base_url}/{filename}"
                output_path = self.output_dir / filename
                futures.append(
                    executor.submit(self._process_file, dataset, filename, url, output_path)
                )

            # récupère les résultats au fur et à mesure qu'ils arrivent
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    total_rows += result['rows']
                    total_size += result['size']
                    output_paths.append(result['path'])
                except Exception as e:
                    # propage l'erreur pour arrêter l'extraction complète
                    raise e
                
        self.logger.info(f"Téléchargement OurAirports terminée: {len(output_paths)} fichiers - "
                    f"{self._format_size(total_size)} total")

        return {
            'files_downloaded': len(output_paths),
            'total_rows': total_rows,
            'total_size_bytes': total_size,
            'output_paths': output_paths,
            'datasets': list(files_to_download.keys())
        }
    
    def _process_file(self, dataset: str, filename: str, url: str, output_path: Path) -> dict[str, Any]:
        """
        Télécharge et valide un fichier CSV.

        Exécuté en parallèle par ThreadPoolExecutor.

        Parameters
        ----------
        dataset : str
            Identifiant du dataset (ex: 'airports', 'runways')
        filename : str
            Nom du fichier CSV
        url : str
            URL de téléchargement
        output_path : Path
            Chemin de sauvegarde

        Returns
        -------
        dict[str, Any]
            Métriques du fichier (rows, size, path)
        """
        self.logger.debug(f"Téléchargement de OurAirports:{filename}")

        try:
            # téléchargement direct en DataFrame
            df = self._download_csv(url)

            # validation du schéma pour le fichier critique airports.csv
            if dataset == "airports":
                self._validate_airports_schema(df)

            # sauvegarde locale en CSV
            df.to_csv(output_path, index=False, encoding='utf-8')

            # conversion en Parquet pour optimiser les performances Spark
            parquet_path = self._save_as_parquet(output_path, delete_csv=True)
            file_size = self._get_file_size(parquet_path)

            return {
                'rows': len(df),
                'size': file_size,
                'path': str(parquet_path)
            }

        except Exception as e:
            self.logger.error(f"    ✗ Échec {filename}: {e}")
            raise RuntimeError(f"Échec téléchargement {filename}: {e}")

    def _download_csv(self, url: str) -> pd.DataFrame:
        """
        Télécharge un CSV et le charge directement en DataFrame.

        Parameters
        ----------
        url : str
            URL du fichier CSV

        Returns
        -------
        pd.DataFrame
            Données chargées en mémoire
        """

        response = requests.get(
            url,
            timeout=300,  # 5min par fichier - généralement rapide mais sécurise
        )
        response.raise_for_status()

        # parse le CSV en mémoire sans sauvegarder de fichier intermédiaire
        df = pd.read_csv(StringIO(response.text))  # type: ignore

        return df

    def _validate_airports_schema(self, df: pd.DataFrame) -> None:
        """
        Vérifie que airports.csv contient bien les colonnes essentielles.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame à valider

        Raises
        ------
        ValueError
            Si des colonnes critiques manquent (ident, type, name, etc.)
        """
        # colonnes indispensables pour nos traitements ultérieurs
        required_columns: set[str] = {
            "ident",
            "type",
            "name",
            "latitude_deg",
            "longitude_deg",
            "iso_country"
        }

        missing = required_columns - set(df.columns)

        if missing:
            raise ValueError(f"Colonnes manquantes dans airports.csv: {missing}")

        self.logger.debug("Schéma OurAirports:airports.csv validé")