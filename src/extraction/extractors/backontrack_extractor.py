"""
Extracteur pour Back-on-Track (données trains de nuit européens).

Source = Google Sheets publique maintenue par la communauté ferroviaire.
On télécharge le fichier Excel complet et on sépare chaque feuille en CSV.
"""

import requests
import pandas as pd
from io import BytesIO
from pathlib import Path
from typing import Dict, Any

from .base_extractor import BaseExtractor


class BackOnTrackExtractor(BaseExtractor):
    """
    Extracteur pour les données Back-on-Track (trains de nuit européens).

    Notes
    -----
    Source = Google Sheets publique maintenue par la communauté ferroviaire.
    Télécharge le fichier Excel complet et sépare chaque feuille en CSV.
    """

    def get_source_name(self) -> str:
        """
        Retourne l'identifiant de la source.

        Returns
        -------
        str
            Nom de la source ('backontrack')
        """
        return "backontrack"

    def get_default_output_dir(self) -> Path:
        """
        Retourne le répertoire de sortie par défaut.

        Returns
        -------
        Path
            Chemin du répertoire Back-on-Track
        """
        return self.config.BACKONTRACK_OUTPUT_DIR

    def extract(self) -> Dict[str, Any]:
        """
        Télécharge le fichier Excel et éclate chaque feuille en CSV séparé.

        Returns
        -------
        dict[str, Any]
            Stats d'extraction (nb fichiers, lignes totales, taille, liste des feuilles)

        Raises
        ------
        RuntimeError
            Si le téléchargement ou le parsing Excel échoue
        """
        self.logger.info("Démarrage de l'extraction des données Back-on-Track Night Trains depuis Google Sheets...")

        # Google Sheets permet l'export direct en XLSX via cette URL
        spreadsheet_id = self.config.BACKONTRACK_SPREADSHEET_ID
        xlsx_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"

        # téléchargement du fichier complet
        try:
            self.logger.debug(f"Téléchargement de BackOnTrack: {xlsx_url} - format xlsx")
            response = requests.get(
                xlsx_url,
                timeout=300,  # 5min - fichier assez léger mais connexion peut être lente
            )
            response.raise_for_status()

            self.logger.info(f"Téléchargement terminé: - {self._format_size(len(response.content))} total")

        except requests.RequestException as e:
            raise RuntimeError(f"Échec du téléchargement: {e}")

        # parse le fichier Excel en mémoire (pas besoin de sauvegarder le .xlsx)
        try:
            all_sheets: dict[str, pd.DataFrame] = pd.read_excel(  # pyright: ignore[reportUnknownMemberType]
                BytesIO(response.content),
                sheet_name=None,  # charge toutes les feuilles d'un coup
                engine='openpyxl'
            )

            self.logger.info(f"Extraction BackOntrack de {len(all_sheets)} feuilles dans {self.output_dir}")

        except Exception as e:
            raise RuntimeError(f"Échec du parsing Excel: {e}")

        # conversion : 1 feuille = 1 fichier CSV + Parquet
        output_paths: list[str] = []
        total_rows = 0

        for sheet_name, df in all_sheets.items():
            # supprime les caractères spéciaux des noms de feuilles (espaces, accents, etc.)
            safe_name = "".join([c if c.isalnum() else "_" for c in sheet_name])
            csv_filename = f"{safe_name}.csv"
            csv_path = self.output_dir / csv_filename

            # export en CSV UTF-8 sans index
            df.to_csv(csv_path, index=False, encoding='utf-8')

            # conversion en Parquet pour optimiser les performances Spark
            parquet_path = self._save_as_parquet(csv_path, delete_csv=False)

            output_paths.append(str(parquet_path))
            total_rows += len(df)

        # calcul de la taille totale des CSV générés
        total_size = sum(
            self._get_file_size(Path(p)) for p in output_paths
        )
        
        return {
            'files_downloaded': len(output_paths),
            'total_rows': total_rows,
            'total_size_bytes': total_size,
            'output_paths': output_paths,
            'sheets_exported': list(all_sheets.keys())
        }