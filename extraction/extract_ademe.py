import os
import requests
from tqdm import tqdm
import time
import csv

def download_ademe_with_pagination(output_folder: str, filename: str = "base_empreinte_ademe.csv", search_query: str = "transport"):
    """
    Exécute l'extraction des données de la Base Empreinte de l'ADEME via l'API publique.

    Cette fonction interroge l'API par lots (pagination) pour récupérer l'ensemble des
    facteurs d'émission correspondant au filtre de recherche spécifié. Les données
    sont ensuite consolidées et exportées au format CSV.

    Args:
        output_folder (str): Chemin absolu ou relatif du dossier de destination.
        filename (str): Nom du fichier de sortie (ex: "base_empreinte.csv").
        search_query (str): Mot-clé pour le filtrage des données (défaut: "transport").
    """
    dataset_id = "base-carboner"
    base_url = f"https://data.ademe.fr/data-fair/api/v1/datasets/{dataset_id}/lines"

    full_path = os.path.join(output_folder, filename)

    # Vérification et création de l'arborescence
    if not os.path.exists(output_folder):
        print(f"[INFO] Création du répertoire : {output_folder}")
        os.makedirs(output_folder, exist_ok=True)

    print(f"[INFO] Initialisation du téléchargement. Filtre : '{search_query}'")

    # Définition des en-têtes HTTP
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSPR-ETL/1.0)',
        'Accept': 'application/json'
    }

    try:
        # Requête initiale pour déterminer la volumétrie
        params = {'size': 1, 'q': search_query}
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        total_records = data.get('total', 0)
        print(f"[INFO] Nombre total d'enregistrements identifiés : {total_records}")

        # Configuration de la pagination
        page_size = 1000
        all_results = []

        # Boucle d'extraction avec barre de progression
        with tqdm(total=total_records, desc="Progression", unit="req") as pbar:
            for offset in range(0, total_records, page_size):
                params = {
                    'size': page_size,
                    'skip': offset,
                    'q': search_query
                }

                response = requests.get(base_url, headers=headers, params=params)
                response.raise_for_status()

                page_data = response.json()
                results = page_data.get('results', [])

                if not results:
                    break

                all_results.extend(results)
                pbar.update(len(results))
                
                # Temporisation pour respecter les limites de l'API
                time.sleep(0.2)

        # Export des données
        if all_results:
            print(f"\n[INFO] Écriture des données dans le fichier : {filename}")

            # Identification exhaustive des colonnes
            all_keys = set()
            for result in all_results:
                all_keys.update(result.keys())
            
            sorted_keys = sorted(list(all_keys))

            with open(full_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=sorted_keys, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(all_results)

            file_size_mb = os.path.getsize(full_path) / (1024 * 1024)
            print(f"[SUCCÈS] Fichier généré : {full_path}")
            print(f"[INFO] Taille du fichier : {file_size_mb:.2f} MB")
            print(f"[INFO] Lignes traitées : {len(all_results)}")
        else:
            print("[AVERTISSEMENT] Aucune donnée n'a été récupérée.")

    except requests.exceptions.HTTPError as e:
        print(f"\n[ERREUR] Échec de la requête HTTP : {e.response.status_code}")
        print(f"[DEBUG] Détails : {e}")
    except Exception as e:
        print(f"\n[ERREUR] Une exception non gérée est survenue : {e}")


def download_ademe_real_link(output_folder: str, filename: str = "base_empreinte_ademe.csv"):
    """
    Fonction utilitaire encapsulant la logique de téléchargement paginé.
    
    Args:
        output_folder (str): Dossier de destination.
        filename (str): Nom du fichier cible.
    """
    return download_ademe_with_pagination(output_folder, filename)

if __name__ == "__main__":
    # Configuration des chemins d'exécution
    RAW_DATA_PATH = os.path.join("data", "raw", "ademe")
    
    download_ademe_real_link(RAW_DATA_PATH)