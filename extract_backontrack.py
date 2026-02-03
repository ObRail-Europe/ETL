import os
import requests
import pandas as pd
from io import BytesIO

def download_and_extract_all_sheets(output_folder: str) -> None:
    """
    Télécharge un classeur Google Sheets public et exporte chaque feuille en fichier CSV.

    Cette fonction récupère le document complet au format XLSX via une requête HTTP,
    charge le contenu en mémoire, et itère sur chaque feuille de calcul pour
    générer les fichiers CSV correspondants dans le dossier cible.

    Args:
        output_folder (str): Chemin du répertoire de destination pour les fichiers CSV.
    """
    # Identifiant du document public Back-on-Track
    spreadsheet_id = "15zsK-lBuibUtZ1s2FxVHvAmSu-pEuE0NDT6CAMYL2TY"
    
    # Construction de l'URL d'exportation au format XLSX
    xlsx_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"
    
    # Vérification et création de l'arborescence
    if not os.path.exists(output_folder):
        print(f"[INFO] Création du répertoire : {output_folder}")
        os.makedirs(output_folder, exist_ok=True)
    
    print(f"[INFO] Démarrage du téléchargement du classeur (ID: {spreadsheet_id})...")
    
    try:
        response = requests.get(xlsx_url)
        response.raise_for_status()
        
        print("[INFO] Fichier téléchargé avec succès. Traitement des feuilles de calcul...")
        
        # Chargement du fichier Excel depuis la mémoire (BytesIO)
        # L'argument sheet_name=None permet de charger toutes les feuilles dans un dictionnaire
        all_sheets = pd.read_excel(BytesIO(response.content), sheet_name=None, engine='openpyxl')
        
        print(f"[INFO] Nombre de feuilles détectées : {len(all_sheets)}")
        
        # Itération sur chaque feuille pour l'exportation
        for sheet_name, df in all_sheets.items():
            # Normalisation du nom de fichier (caractères alphanumériques uniquement)
            safe_name = "".join([c if c.isalnum() else "_" for c in sheet_name])
            csv_filename = f"{safe_name}.csv"
            full_path = os.path.join(output_folder, csv_filename)
            
            # Exportation en CSV
            df.to_csv(full_path, index=False, sep=",", encoding='utf-8')
            print(f"[INFO] Exportation : '{sheet_name}' -> {csv_filename} ({len(df)} lignes)")
            
        print("\n[SUCCÈS] Processus d'extraction terminé.")

    except requests.exceptions.HTTPError as e:
        print(f"[ERREUR] Échec du téléchargement HTTP : {e}")
    except ImportError:
        print("[ERREUR] Dépendance manquante : la bibliothèque 'openpyxl' est requise.")
    except Exception as e:
        print(f"[ERREUR] Une exception inattendue est survenue : {e}")

if __name__ == "__main__":
    # Définition du chemin de sortie
    raw_data_path = os.path.join("data", "raw", "back_on_track")
    download_and_extract_all_sheets(raw_data_path)