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

        # Conversion en Parquet avec schéma explicite (pas d'en-tête, séparateur tab)
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
        schema = StructType([
            StructField("geonameid", LongType(), False),
            StructField("name", StringType(), False),
            StructField("asciiname", StringType(), True),
            StructField("alternatenames", StringType(), True),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("feature_class", StringType(), True),
            StructField("feature_code", StringType(), True),
            StructField("country_code", StringType(), False),
            StructField("cc2", StringType(), True),
            StructField("admin1_code", StringType(), True),
            StructField("admin2_code", StringType(), True),
            StructField("admin3_code", StringType(), True),
            StructField("admin4_code", StringType(), True),
            StructField("population", LongType(), False),
            StructField("elevation", IntegerType(), True),
            StructField("dem", IntegerType(), True),
            StructField("timezone", StringType(), True),
            StructField("modification_date", StringType(), True)
        ])
        parquet_path = self._save_as_parquet(
            csv_path,
            parquet_path=None,
            delete_csv=False,
            schema=schema,
            sep='\t',
            header=False,
            inferSchema=False
        )

        files_downloaded = 2  # zip + txt
        total_size_bytes = self._get_file_size(zip_path) + self._get_file_size(csv_path)
        output_paths = [str(csv_path), str(parquet_path)]

        return {
            "files_downloaded": files_downloaded,
            "total_size_bytes": total_size_bytes,
            "output_paths": output_paths,
        }
