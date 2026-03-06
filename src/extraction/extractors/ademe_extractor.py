"""
Extracteur pour ADEME Base Carbone (facteurs d'émission du transport aérien).

Source = API publique data.ademe.fr, aucune authentification requise.
On filtre sur la catégorie 'Transport de personnes > Aérien' et on pagine
jusqu'à avoir collecté tous les enregistrements (~443 lignes).
"""

import csv
import requests
from pathlib import Path
from typing import Any

from .base_extractor import BaseExtractor


class AdemeExtractor(BaseExtractor):
    """
    Extracteur pour la Base Carbone ADEME - facteurs d'émission du transport aérien.

    Notes
    -----
    Source = API publique data.ademe.fr (pas de token requis).
    Filtre sur la catégorie 'Transport de personnes > Aérien' via le paramètre
    Code_de_la_catégorie_starts. Pagine via le champ 'next' de chaque réponse.
    """

    def get_source_name(self) -> str:
        """
        Retourne l'identifiant de la source.

        Returns
        -------
        str
            Nom de la source ('ademe')
        """
        return "ademe"

    def get_default_output_dir(self) -> Path:
        """
        Retourne le répertoire de sortie par défaut.

        Returns
        -------
        Path
            Chemin du répertoire ADEME Base Carbone
        """
        return self.config.ADEME_OUTPUT_DIR

    def extract(self) -> dict[str, Any]:
        """
        Télécharge tous les facteurs d'émission aérien depuis l'API ADEME Base Carbone.

        Parcourt les pages via la pagination curseur (champ 'next') jusqu'à épuisement
        des résultats, puis sauvegarde en CSV et convertit en Parquet.

        Returns
        -------
        dict[str, Any]
            Stats d'extraction : nb fichiers, nb lignes, taille totale, chemins de sortie

        Raises
        ------
        RuntimeError
            Si la requête échoue ou si aucun enregistrement n'est retourné
        """
        self.logger.info(
            "Démarrage de l'extraction des données ADEME Base Carbone "
            f"(catégorie : {self.config.ADEME_CATEGORY_FILTER})..."
        )

        base_url = self.config.ADEME_API_BASE_URL
        category_filter = self.config.ADEME_CATEGORY_FILTER
        page_size = self.config.ADEME_PAGE_SIZE

        # paramètres de la première requête - requests gère l'URL-encoding automatiquement
        params: dict[str, Any] = {
            "Code_de_la_catégorie_starts": category_filter,
            "size": page_size,
        }

        all_records: list[dict[str, Any]] = []
        page_num = 0
        next_url: str | None = None  # URL complète fournie par le champ 'next'

        # boucle de pagination : on suit les URL 'next' jusqu'à ce que le champ soit absent
        while True:
            page_num += 1

            try:
                if next_url:
                    # l'API fournit l'URL complète avec curseur - on l'utilise directement
                    self.logger.debug(f"Page ADEME {page_num}: {next_url}")
                    response = requests.get(next_url, timeout=60)
                else:
                    # première page : on construit l'URL depuis la config
                    self.logger.debug(f"Page ADEME {page_num}: {base_url}")
                    response = requests.get(base_url, params=params, timeout=60)

                response.raise_for_status()

            except requests.RequestException as e:
                raise RuntimeError(f"Échec de la requête ADEME (page {page_num}): {e}")

            data = response.json()

            # extraction des enregistrements de cette page
            page_records: list[dict[str, Any]] = data.get("results", [])
            all_records.extend(page_records)

            self.logger.debug(
                f"Page {page_num}: {len(page_records)} enregistrements "
                f"(total cumulé: {len(all_records)} / {data.get('total', '?')})"
            )

            # 'next' est absent ou None quand on est sur la dernière page
            next_url = data.get("next")
            if not next_url:
                break

        # validation : au moins un enregistrement attendu
        if not all_records:
            raise RuntimeError(
                f"Aucun enregistrement ADEME retourné pour la catégorie "
                f"'{category_filter}'. Vérifier le filtre ou l'API."
            )

        self.logger.info(
            f"Collecte ADEME terminée : {len(all_records)} enregistrements "
            f"en {page_num} page(s)"
        )

        # sauvegarde CSV intermédiaire - union de toutes les clés car certains
        # enregistrements ont des champs supplémentaires (ex: Type_poste, Nom_poste_*)
        csv_path = self.output_dir / self.config.ADEME_FILENAME
        seen: set[str] = set()
        fieldnames: list[str] = []
        for record in all_records:
            for key in record:
                if key not in seen:
                    seen.add(key)
                    fieldnames.append(key)

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, restval="")
            writer.writeheader()
            writer.writerows(all_records)

        self.logger.debug(f"CSV ADEME sauvegardé : {csv_path.name}")

        # conversion en Parquet pour optimiser les performances Spark en aval
        parquet_path = self._save_as_parquet(csv_path, delete_csv=True)
        file_size = self._get_file_size(parquet_path)

        self.logger.info(
            f"Extraction ADEME terminée : {len(all_records)} lignes, "
            f"{self._format_size(file_size)}"
        )

        return {
            "files_downloaded": 1,
            "total_size_bytes": file_size,
            "output_paths": [str(parquet_path)],
            "total_rows": len(all_records),
            "pages_fetched": page_num,
        }
