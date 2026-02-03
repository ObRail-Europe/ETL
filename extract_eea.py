"""
Script d'extraction des données EEA (European Environment Agency)
Dataset : Greenhouse gas emission intensity of electricity generation
Auteur : Data Engineer - Projet ObRail Europe
Date : 2026-02-03
"""

import os
import requests
import zipfile
from pathlib import Path


def download_eea_data(output_folder: str, filename: str = "eea_co2_intensity.csv") -> None:
    """
    Télécharge le dataset EEA sur l'intensité carbone de la production d'électricité.

    Cette fonction récupère les données depuis l'infrastructure de données spatiales
    de l'EEA (SDI), gère automatiquement l'extraction des archives ZIP, et exporte
    le fichier CSV dans le dossier de destination spécifié.

    Args:
        output_folder (str): Chemin du répertoire de destination pour le fichier CSV.
        filename (str): Nom du fichier de sortie (défaut: "eea_co2_intensity.csv").
    """
    # URL de téléchargement direct Nextcloud - 2017 data
    url = "https://sdi.eea.europa.eu/datashare/s/JNfX8GiTPEprBcS/download"

    TIMEOUT = 30  # Timeout en secondes pour la requête

    # Vérification et création de l'arborescence
    if not os.path.exists(output_folder):
        print(f"[INFO] Création du répertoire : {output_folder}")
        os.makedirs(output_folder, exist_ok=True)

    full_path = os.path.join(output_folder, filename)

    # Configuration du User-Agent pour éviter les erreurs 403
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSPR-ETL/1.0)',
        'Accept': 'text/csv,application/zip,application/octet-stream,*/*',
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.eea.europa.eu/',
        'Connection': 'keep-alive',
    }

    print("[INFO] Initialisation du téléchargement du dataset EEA...")
    print("[INFO] Dataset : CO2 Intensity of Electricity Generation (2017)")

    try:
        # Effectuer la requête GET avec headers personnalisés
        response = requests.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=True)
        response.raise_for_status()

        # Vérifier que le contenu est bien du CSV/ZIP et non du HTML
        content_preview = response.content[:500].decode('utf-8', errors='ignore')
        if content_preview.strip().startswith('<!doctype') or content_preview.strip().startswith('<html'):
            print(f"[ERREUR] Le serveur a retourné du HTML au lieu de données")
            return

        # Vérifier si le contenu est un fichier ZIP
        is_zip = response.content[:2] == b'PK'

        if is_zip:
            print(f"[INFO] Format détecté : Archive ZIP")

            # Sauvegarder temporairement le ZIP
            zip_filename = full_path.replace('.csv', '.zip')
            with open(zip_filename, 'wb') as f:
                f.write(response.content)

            # Extraire le fichier CSV du ZIP
            try:
                with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                    # Chercher le fichier CSV dans l'archive
                    csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]

                    if not csv_files:
                        print(f"[ERREUR] Aucun fichier CSV trouvé dans l'archive")
                        Path(zip_filename).unlink()
                        return

                    # Extraire le premier fichier CSV trouvé
                    csv_file = csv_files[0]
                    print(f"[INFO] Extraction de : {csv_file}")

                    # Extraire et renommer
                    zip_ref.extract(csv_file, output_folder)
                    extracted_path = Path(output_folder) / csv_file
                    extracted_path.rename(full_path)

                    # Nettoyer le dossier extrait s'il existe
                    parent_folder = extracted_path.parent
                    if parent_folder != Path(output_folder):
                        import shutil
                        shutil.rmtree(parent_folder, ignore_errors=True)

                    # Supprimer le fichier ZIP temporaire
                    Path(zip_filename).unlink()

            except zipfile.BadZipFile:
                print(f"[ERREUR] Le fichier ZIP est corrompu")
                if Path(zip_filename).exists():
                    Path(zip_filename).unlink()
                return
        else:
            # Sauvegarder directement le CSV
            print(f"[INFO] Format détecté : CSV")
            with open(full_path, 'wb') as f:
                f.write(response.content)

        # Afficher les informations sur le fichier téléchargé
        file_size_kb = Path(full_path).stat().st_size / 1024
        print(f"\n[SUCCÈS] Fichier téléchargé : {filename}")
        print(f"[INFO] Taille du fichier : {file_size_kb:.2f} KB")

    except requests.exceptions.Timeout:
        print(f"[ERREUR] Timeout après {TIMEOUT} secondes")
    except requests.exceptions.HTTPError as e:
        print(f"[ERREUR] Échec de la requête HTTP : {e.response.status_code}")
    except Exception as e:
        print(f"[ERREUR] Une exception inattendue est survenue : {e}")


if __name__ == "__main__":
    # Configuration des chemins d'exécution
    RAW_DATA_PATH = os.path.join("data", "raw", "eea")

    download_eea_data(RAW_DATA_PATH)
