"""
Config de l'extraction - URLs des sources, chemins de sortie, tokens API.

Tout ce qui est spécifique à l'extraction est ici. Les configs génériques (Spark, etc.)
sont dans BaseConfig.
"""

import os
from dotenv import load_dotenv

from common.config import BaseConfig

# charge le fichier .env à la racine du projet
load_dotenv()


class ExtractionConfig(BaseConfig):
    """
    Config d'extraction - URLs, chemins, tokens API pour les 4 sources.

    Hérite de BaseConfig pour les configs génériques (Spark, chemins data/logs).
    Ajoute tout ce qui est spécifique aux sources : Back-on-Track, EEA, OurAirports, MobilityDatabase.
    """

    # Chemin de base pour les données brutes
    RAW_DATA_PATH = BaseConfig.DATA_ROOT / "raw"

    # Back-on-Track - trains de nuit européens (Google Sheets public)
    BACKONTRACK_SPREADSHEET_ID = "15zsK-lBuibUtZ1s2FxVHvAmSu-pEuE0NDT6CAMYL2TY"
    BACKONTRACK_OUTPUT_DIR = RAW_DATA_PATH / "back_on_track"

    # OurAirports - référentiel mondial des aéroports (CSV publics)
    OURAIRPORTS_BASE_URL =  "https://davidmegginson.github.io/ourairports-data"
    OURAIRPORTS_OUTPUT_DIR = RAW_DATA_PATH / "ourairports"
    OURAIRPORTS_FILES: dict[str, str] = {
        "airports": "airports.csv",
        "countries": "countries.csv",
        "regions": "regions.csv"
    }

    # Mobility Database - flux GTFS des réseaux de transport européens (API avec token gratuit)
    MOBILITY_API_BASE_URL = "https://api.mobilitydatabase.org/v1"
    MOBILITY_API_REFRESH_TOKEN = os.getenv("MOBILITY_API_REFRESH_TOKEN")
    MOBILITY_OUTPUT_DIR = RAW_DATA_PATH / "mobilitydatabase"
    # 27 pays de l'UE - on récupère tous les feeds GTFS actifs (~1000 réseaux)
    MOBILITY_EU_COUNTRIES = [
        'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR',
        'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL',
        'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE'
    ]

    # Paramètres de téléchargement Mobility Database
    MOBILITY_MAX_CONCURRENT = int(os.getenv("MOBILITY_MAX_CONCURRENT", "20"))  # 20 téléchargements en parallèle max
    MOBILITY_CHUNK_SIZE = int(os.getenv("MOBILITY_CHUNK_SIZE", "1048576"))  # 1MB par chunk pour le streaming
    
    # Ember - données d'intensité carbone (gCO₂/kWh) par pays et année
    EMBER_API_BASE_URL = "https://api.ember-energy.org/v1/carbon-intensity/yearly"
    EMBER_API_KEY = os.getenv("EMBER_API_KEY")
    EMBER_OUTPUT_DIR = RAW_DATA_PATH / "ember"
    EMBER_FILENAME = "ember_carbon_intensity.csv"
    # Années à extraire pour Ember
    EMBER_YEARS = [2013, 2014, 2015, 2016, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

    # Geonames - référentiel géographique mondial des villes (ZIP contenant CSV)
    GEONAMES_URL = "https://download.geonames.org/export/dump/cities1000.zip"
    GEONAMES_OUTPUT_DIR = RAW_DATA_PATH / "geonames"
    GEONAMES_ZIP_FILENAME = "cities1000.zip"
    GEONAMES_CSV_FILENAME = "cities1000.txt"
    
    @classmethod
    def validate(cls) -> None:
        """
        Crée les dossiers de sortie et warn si le token Mobility Database manque.

        Notes
        -----
        Appelée automatiquement lors de l'import du module. Crée tous les
        dossiers de sortie pour les 4 sources (BackOnTrack, EEA, OurAirports,
        Mobility Database).
        """
        # crée data/ et logs/ via BaseConfig
        super().validate()

        # crée tous les dossiers de sortie pour chaque source
        cls.RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
        cls.BACKONTRACK_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cls.OURAIRPORTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cls.MOBILITY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cls.EMBER_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cls.GEONAMES_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # warn si les tokens API manquent - ces sources seront skippées
        if not cls.MOBILITY_API_REFRESH_TOKEN:
            print(
                "WARNING: MOBILITY_API_REFRESH_TOKEN non défini. "
                "L'extraction Mobility Database sera ignorée."
            )
        
        if not cls.EMBER_API_KEY:
            print(
                "WARNING: EMBER_API_KEY non défini. "
                "L'extraction Ember sera ignorée."
            )


# crée les dossiers automatiquement quand on importe le module
ExtractionConfig.validate()