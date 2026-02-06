"""
Script ETL - Extraction Back-on-Track
Télécharge les données ferroviaires depuis Google Sheets public
Auteur: Équipe MSPR ObRail Europe
"""

import os
import logging
import sys
from pathlib import Path
from io import BytesIO
import requests
import pandas as pd
from dotenv import load_dotenv

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
SPREADSHEET_ID = os.getenv("BACKONTRACK_SPREADSHEET_ID")
TIMEOUT = os.getenv("TIMEOUT")
RAW_PATH = os.getenv("BACKONTRACK_RAW_PATH")


def extract_backontrack(output_folder: str) -> bool:
    """
    Extrait les données ferroviaires depuis Google Sheets Back-on-Track.
    
    Args:
        output_folder: Répertoire de destination des fichiers CSV
    
    Returns:
        True si extraction réussie, False sinon
    """
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    
    xlsx_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=xlsx"
    
    logger.info(f"Démarrage extraction Back-on-Track (ID: {SPREADSHEET_ID})")
    
    try:
        # Téléchargement du classeur
        logger.info("Téléchargement du classeur XLSX...")
        response = requests.get(xlsx_url, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Lecture de toutes les feuilles
        logger.info("Lecture des feuilles de calcul...")
        all_sheets = pd.read_excel(
            BytesIO(response.content),
            sheet_name=None,
            engine='openpyxl'
        )
        
        logger.info(f"{len(all_sheets)} feuille(s) détectée(s)")
        
        # Exportation de chaque feuille en CSV
        for sheet_name, dataframe in all_sheets.items():
            safe_name = "".join(c if c.isalnum() else "_" for c in sheet_name)
            csv_path = output_path / f"{safe_name}.csv"
            
            dataframe.to_csv(csv_path, index=False, encoding='utf-8')
            logger.info(f"Exporté: {sheet_name} → {csv_path.name} ({len(dataframe)} lignes)")
        
        logger.info("Extraction Back-on-Track terminée avec succès")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur HTTP: {e}")
        return False
    except ImportError as e:
        logger.error(f"Dépendance manquante (openpyxl requis): {e}")
        return False
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = extract_backontrack(RAW_PATH)
    sys.exit(0 if success else 1)