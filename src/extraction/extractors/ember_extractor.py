"""
Extracteur pour Ember - données d'intensité carbone (gCO₂/kWh) par pays et année.

Source : Ember API (https://api.ember-energy.org/v1/carbon-intensity/yearly)

Télécharge les données pour les années spécifiées et les sauvegarde en Parquet.
"""

import requests
from pathlib import Path
from typing import Any

from .base_extractor import BaseExtractor

class EmberExtractor(BaseExtractor):
    """
    Extracteur pour les données d'intensité carbone (gCO₂/kWh) d'Ember.
    """

    def get_source_name(self) -> str:
        return "ember"

    def get_default_output_dir(self) -> Path:
        return self.config.RAW_DATA_PATH / "ember"

    def extract(self) -> dict[str, Any]:
        self.logger.info("Extraction des données Ember (intensité carbone annuelle)...")

        api_key = self.config.EMBER_API_KEY
        if not api_key:
            raise RuntimeError("La variable d'environnement EMBER_API_KEY doit être définie pour accéder à l'API Ember.")

        output_dir = self.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        # Années à extraire
        years = self.config.EMBER_YEARS

        # API endpoint
        base_url = self.config.EMBER_API_BASE_URL

        # On récupère toutes les entités (pays) disponibles
        # (optionnel: on pourrait filtrer sur l'UE ou un sous-ensemble)
        params: dict[str, Any] = {
            "start_date": min(years),
            "end_date": max(years),
            "api_key": api_key
        }

        self.logger.debug(f"Requête Ember: {base_url} params={params}")

        response = requests.get(base_url, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        

        # Robust parsing: handle dict with a key containing the list
        if isinstance(data, dict):
            for key in ("data", "results", "records", "items"):  # try common keys
                if key in data and isinstance(data[key], list):
                    data_list = data[key]  # pyright: ignore[reportUnknownVariableType]
                    break
            else:
                # If no list found, try to use the dict as a single record
                if all(isinstance(v, (str, int, float, type(None))) for v in data.values()): # pyright: ignore[reportUnknownVariableType]
                    data_list = [data] # pyright: ignore[reportUnknownVariableType]
                else:
                    raise ValueError("API response does not contain a list of records under expected keys.\nAperçu: " + str(data)[:500])  # pyright: ignore[reportUnknownArgumentType]
        elif isinstance(data, list):
            data_list = data # pyright: ignore[reportUnknownVariableType]
        else:
            raise ValueError(f"Réponse inattendue de l'API Ember: type={type(data)}")

        # Filtrage des années demandées (clé correcte = 'date' dans la réponse API)
        filtered = [row for row in data_list if isinstance(row, dict) and int(row.get("date", 0)) in years] # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType, reportUnknownMemberType]

        # Gestion du cas où aucune donnée n'est trouvée
        if not filtered:
            raise RuntimeError(
                f"Aucune donnée Ember pour les années demandées: {years}.\n"
                f"Aperçu de la réponse API (10 premiers éléments): {data_list[:10]}"
            )

        # Sauvegarde CSV intermédiaire
        import csv
        csv_path = output_dir / self.config.EMBER_FILENAME
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=filtered[0].keys()) # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            writer.writeheader()
            writer.writerows(filtered) # pyright: ignore[reportUnknownMemberType]

        # Conversion en Parquet
        parquet_path = self._save_as_parquet(csv_path, delete_csv=True)
        file_size = self._get_file_size(parquet_path)

        self.logger.info(f"Extraction Ember terminée: {len(filtered)} lignes, {self._format_size(file_size)}") # pyright: ignore[reportUnknownArgumentType]

        return {
            "files_downloaded": 1,
            "total_size_bytes": file_size,
            "output_paths": [str(parquet_path)],
            "years": years,
            "rows": len(filtered) # pyright: ignore[reportUnknownArgumentType]
        }
