"""
Script ETL - Extraction EEA
Télécharge les données d'intensité CO2 de l'électricité européenne
Auteur: Équipe MSPR ObRail Europe
"""

import os
import logging
import sys
import zipfile
from pathlib import Path
import requests
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
EEA_URL = os.getenv("EEA_URL")
TIMEOUT = os.getenv("TIMEOUT")
DEFAULT_FILENAME = os.getenv("EEA_FILENAME")
RAW_PATH = os.getenv("EEA_RAW_PATH")


def extract_eea(output_folder: str = None, filename: str = None) -> bool:
    """
    Extrait les données EEA sur l'intensité carbone de l'électricité.
    
    Args:
        output_folder: Répertoire de destination
        filename: Nom du fichier de sortie
    
    Returns:
        True si extraction réussie, False sinon
    """
    output_path = Path(output_folder or RAW_PATH)
    output_path.mkdir(parents=True, exist_ok=True)
    
    csv_path = output_path / (filename or DEFAULT_FILENAME)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSPR-ETL/1.0)',
        'Accept': 'text/csv,application/zip,application/octet-stream,*/*',
        'Referer': 'https://www.eea.europa.eu/',
    }
    
    logger.info("Démarrage extraction EEA CO2 Intensity (2017)")
    
    try:
        # Téléchargement
        logger.info(f"Téléchargement depuis {EEA_URL}")
        response = requests.get(EEA_URL, headers=headers, timeout=TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        
        # Vérification du contenu (éviter HTML)
        content_preview = response.content[:500].decode('utf-8', errors='ignore')
        if content_preview.strip().startswith(('<!doctype', '<html')):
            logger.error("Contenu HTML reçu au lieu de données")
            return False
        
        # Détection du format (ZIP ou CSV)
        is_zip = response.content[:2] == b'PK'
        
        if is_zip:
            logger.info("Format détecté: ZIP")
            zip_path = output_path / "temp.zip"
            
            # Sauvegarde temporaire
            zip_path.write_bytes(response.content)
            
            try:
                # Extraction du CSV
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
                    
                    if not csv_files:
                        logger.error("Aucun fichier CSV dans l'archive")
                        zip_path.unlink()
                        return False
                    
                    # Extraction du premier CSV
                    csv_file = csv_files[0]
                    logger.info(f"Extraction: {csv_file}")
                    
                    with zip_ref.open(csv_file) as source, csv_path.open('wb') as target:
                        target.write(source.read())
                
                # Nettoyage
                zip_path.unlink()
                
            except zipfile.BadZipFile:
                logger.error("Archive ZIP corrompue")
                if zip_path.exists():
                    zip_path.unlink()
                return False
        else:
            # Sauvegarde directe du CSV
            logger.info("Format détecté: CSV")
            csv_path.write_bytes(response.content)
        
        file_size_kb = csv_path.stat().st_size / 1024
        logger.info(f"Fichier sauvegardé: {filename} ({file_size_kb:.2f} KB)")
        logger.info("Extraction EEA terminée avec succès")
        return True
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout après {TIMEOUT} secondes")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur HTTP: {e}")
        return False
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = extract_eea()
    sys.exit(0 if success else 1)
