"""
Script ETL - Extraction OurAirports
Télécharge les données aéroportuaires mondiales
Auteur: Équipe MSPR ObRail Europe
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from io import StringIO
import requests
import pandas as pd
from dotenv import load_dotenv
import os

# Chargement des variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = os.getenv("OURAIRPORTS_BASE_URL")
RAW_PATH = os.getenv("OURAIRPORTS_RAW_PATH")
DATA_VERSION = os.getenv("OURAIRPORTS_DATA_VERSION")
TIMEOUT = os.getenv("TIMEOUT")

FILES = {
    "airports": "airports.csv",
    "countries": "countries.csv",
    "regions": "regions.csv"
}

REQUIRED_COLUMNS = {
    "ident", "type", "name",
    "latitude_deg", "longitude_deg", "iso_country"
}


def download_csv(file_name: str) -> pd.DataFrame:
    """
    Télécharge un fichier CSV depuis OurAirports.
    
    Args:
        file_name: Nom du fichier à télécharger
    
    Returns:
        DataFrame pandas
    """
    url = f"{BASE_URL}/{file_name}"
    logger.info(f"Téléchargement: {url}")
    
    response = requests.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    
    return pd.read_csv(StringIO(response.text))


def validate_schema(dataframe: pd.DataFrame) -> bool:
    """
    Valide le schéma du dataset airports.
    
    Args:
        dataframe: DataFrame à valider
    
    Returns:
        True si valide, False sinon
    """
    missing = REQUIRED_COLUMNS - set(dataframe.columns)
    if missing:
        logger.error(f"Colonnes manquantes: {missing}")
        return False
    return True


def extract_ourairports(output_folder: str = RAW_PATH) -> bool:
    """
    Extrait les données aéroportuaires depuis OurAirports.
    
    Args:
        output_folder: Répertoire de destination
    
    Returns:
        True si extraction réussie, False sinon
    """
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Démarrage extraction OurAirports")
    
    metadata = {
        "source": "OurAirports",
        "source_url": BASE_URL,
        "version": DATA_VERSION,
        "ingestion_date": datetime.utcnow().isoformat(),
        "files": []
    }
    
    try:
        # Téléchargement et sauvegarde de chaque fichier
        for dataset, file_name in FILES.items():
            logger.info(f"Traitement: {file_name}")
            
            dataframe = download_csv(file_name)
            
            # Validation pour les aéroports
            if dataset == "airports":
                if not validate_schema(dataframe):
                    logger.error(f"Validation échouée pour {file_name}")
                    return False
            
            # Sauvegarde
            csv_path = output_path / file_name
            dataframe.to_csv(csv_path, index=False)
            
            logger.info(f"Sauvegardé: {file_name} ({len(dataframe)} lignes)")
            
            metadata["files"].append({
                "file": file_name,
                "rows": len(dataframe)
            })
        
        # Sauvegarde des métadonnées
        metadata_path = output_path / "metadata.json"
        with metadata_path.open('w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info("Extraction OurAirports terminée avec succès")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur HTTP: {e}")
        return False
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = extract_ourairports()
    sys.exit(0 if success else 1)
