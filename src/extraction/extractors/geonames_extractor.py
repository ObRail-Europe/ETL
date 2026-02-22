"""
Extracteur pour la base Geonames cities15000.zip.
Télécharge, décompresse, convertit en Parquet.
"""

import zipfile
import requests
from pathlib import Path
from extraction.extractors.base_extractor import BaseExtractor

class GeonamesExtractor(BaseExtractor):
    """
    Extracteur pour la base Geonames cities15000.zip.
    """
    URL = "https://download.geonames.org/export/dump/cities15000.zip"
    FILENAME = "cities15000.zip"
    CSV_FILENAME = "cities15000.txt"

    def get_source_name(self) -> str:
        return "geonames"

    def get_default_output_dir(self) -> Path:
        return self.config.RAW_DATA_PATH / "geonames"

    def extract(self) -> dict[str, int | list[str]]:
        output_dir = self.output_dir
        zip_path = output_dir / self.FILENAME
        csv_path = output_dir / self.CSV_FILENAME

        # Téléchargement
        self.logger.info(f"Téléchargement de {self.URL}")
        response = requests.get(self.URL, stream=True)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        self.logger.info(f"Décompression de {zip_path}")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extract(self.CSV_FILENAME, path=output_dir)

        # Conversion en Parquet
        parquet_path = self._save_as_parquet(csv_path)

        files_downloaded = 2  # zip + txt
        total_size_bytes = self._get_file_size(zip_path) + self._get_file_size(csv_path)
        output_paths = [str(csv_path), str(parquet_path)]

        

        return {
            "files_downloaded": files_downloaded,
            "total_size_bytes": total_size_bytes,
            "output_paths": output_paths,
        }
