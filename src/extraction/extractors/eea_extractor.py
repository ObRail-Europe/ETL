"""
Extracteur pour l'EEA (European Environment Agency) - données d'intensité CO2.

Source = serveur SDI de l'EEA qui renvoie parfois un ZIP, parfois un CSV direct.
Le serveur est capricieux avec les headers HTTP - sans User-Agent correct on prend un 403.
"""

import requests
import zipfile
from pathlib import Path
from typing import Dict, Any

from .base_extractor import BaseExtractor


class EEAExtractor(BaseExtractor):
    """
    Extracteur pour les données d'intensité CO2 de l'EEA (European Environment Agency).

    Notes
    -----
    Le serveur SDI de l'EEA est capricieux avec les headers HTTP - sans
    User-Agent et Referer corrects on prend un 403. Renvoie parfois un ZIP,
    parfois un CSV direct selon l'humeur du serveur.
    """

    def get_source_name(self) -> str:
        """
        Retourne l'identifiant de la source.

        Returns
        -------
        str
            Nom de la source ('eea')
        """
        return "eea"

    def get_default_output_dir(self) -> Path:
        """
        Retourne le répertoire de sortie par défaut.

        Returns
        -------
        Path
            Chemin du répertoire EEA
        """
        return self.config.EEA_OUTPUT_DIR

    def extract(self) -> Dict[str, Any]:
        """
        Télécharge les données CO2 depuis le serveur SDI.

        Gère automatiquement les deux formats possibles (ZIP ou CSV direct).

        Returns
        -------
        dict[str, Any]
            Stats d'extraction (taille, chemin, format détecté ZIP ou CSV)

        Raises
        ------
        RuntimeError
            Si le téléchargement échoue, le serveur renvoie du HTML, ou le ZIP est corrompu
        ValueError
            Si le ZIP ne contient aucun CSV
        """
        self.logger.info("Démarrage de l'extraction des données EEA CO2 intensity par téléchargement depuis SDI...")

        url = self.config.EEA_URL
        filename = self.config.EEA_FILENAME
        final_path = self.output_dir / filename

        # headers obligatoires - sans ça le serveur SDI renvoie un 403
        headers = {
            'Accept': 'text/csv,application/zip,application/octet-stream,*/*',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.eea.europa.eu/',
            'Connection': 'keep-alive',
        }

        # téléchargement avec headers stricts
        try:
            self.logger.debug(f"Téléchargement de EEA:{url}")
            response = requests.get(
                url,
                headers=headers,
                timeout=300,  # 5min - fichier unique mais peut être lent
                allow_redirects=True
            )
            response.raise_for_status()

            self.logger.info(f"Téléchargement EEA terminé : 1 fichier - {self._format_size(len(response.content))} total")

        except requests.RequestException as e:
            raise RuntimeError(f"Échec du téléchargement EEA: {e}")

        # détecte si le serveur a renvoyé une page d'erreur HTML au lieu des données
        content_preview = response.content[:500].decode('utf-8', errors='ignore')
        if content_preview.strip().lower().startswith(('<!doctype', '<html')):
            raise RuntimeError("Le serveur a retourné du HTML au lieu de données")

        # détecte le format : fichiers ZIP commencent toujours par 'PK'
        is_zip = response.content[:2] == b'PK'

        if is_zip:
            self.logger.info("Format détecté: Archive ZIP - Extraction du CSV à partir de l'archive")

            # sauvegarde temporaire du ZIP pour extraction
            zip_path = final_path.with_suffix('.zip')
            with open(zip_path, 'wb') as f:
                f.write(response.content)

            # extraction du CSV depuis le ZIP
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    # cherche tous les CSV dans l'archive
                    csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]

                    if not csv_files:
                        raise ValueError("Aucun fichier CSV trouvé dans l'archive")

                    # extrait le premier CSV trouvé (normalement il y en a qu'un)
                    csv_file = csv_files[0]

                    zip_ref.extract(csv_file, self.output_dir)
                    extracted_path = self.output_dir / csv_file

                    # renomme avec le nom attendu si différent
                    if extracted_path != final_path:
                        extracted_path.rename(final_path)

                    # nettoie les dossiers intermédiaires créés par l'extraction
                    # (le ZIP peut contenir "data/file.csv" au lieu de "file.csv")
                    if '/' in csv_file:
                        parent_folder = self.output_dir / csv_file.split('/')[0]
                        if parent_folder.exists() and parent_folder.is_dir():
                            import shutil
                            shutil.rmtree(parent_folder, ignore_errors=True)

                # supprime le ZIP temporaire
                zip_path.unlink()

            except zipfile.BadZipFile as e:
                if zip_path.exists():
                    zip_path.unlink()
                raise RuntimeError(f"Archive ZIP corrompue: {e}")

        else:
            # format CSV direct - sauvegarde directe
            self.logger.info("Format détecté: CSV")
            with open(final_path, 'wb') as f:
                f.write(response.content)

        # vérification finale
        if not final_path.exists():
            raise RuntimeError("Le fichier de sortie n'a pas été créé")

        # conversion en Parquet pour optimiser les performances de Spark
        parquet_path = self._save_as_parquet(final_path, delete_csv=False)
        file_size = self._get_file_size(parquet_path)

        self.logger.debug(f"Sauvegardes terminées de EEA:{filename} et EEA:{parquet_path.name}")

        return {
            'files_downloaded': 1,
            'total_size_bytes': file_size,
            'output_paths': [str(parquet_path)],
            'format': 'ZIP' if is_zip else 'CSV'
        }