"""
Transformateur pour les données Ember (intensité carbone)

Charge les données d'intensité carbone par pays (ember_carbon_intensity.parquet), 
filtre la valeur la plus récente, convertit les codes ISO alpha-3 en alpha-2 et 
gère les cas particuliers des micro-états (Monaco, Vatican, Liechtenstein, Andorre)
"""

from typing import Any

import pycountry
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from transformation.transformers.base_transformer import BaseTransformer


class EmberTransformer(BaseTransformer):

    def get_transformer_name(self) -> str:
        return "ember"

    def transform(self, **kwargs: Any) -> dict[str, DataFrame]:
        """
        Charge et transforme les données Ember d'intensité carbone

        Returns
        -------
        dict[str, DataFrame]
            {'df_ember': DataFrame} — DataFrame Ember avec country_code alpha-2

        Raises
        ------
        RuntimeError
            Si le fichier Ember est manquant
        """
        cfg = self.config
        self.logger.info("Chargement Ember...")

        df_ember = self.spark.read.parquet(
            str(cfg.EMBER_RAW_PATH / "ember_carbon_intensity.parquet")
        )

        # filtrage de la valeur la plus récente par pays
        self.logger.debug("Filtrage de la valeur la plus récente par pays...")
        df_ember_last = self._filter_latest_per_country(df_ember)

        # conversion alpha-3 → alpha-2
        self.logger.debug("Conversion des codes pays alpha-3 → alpha-2...")
        df_ember_last = self._convert_alpha3_to_alpha2(df_ember_last)

        # mapping des micro-états
        self.logger.debug("Mapping des micro-états (Monaco, Vatican, Liechtenstein)...")
        df_ember_last = self._handle_micro_states(df_ember_last)

        # calcul de la moyenne Andorre (FR + ES)
        self.logger.debug("Calcul de l'intensité carbone Andorre (moyenne FR/ES)...")
        df_ember_last = self._compute_andorra_average(df_ember_last)

        self.logger.info("Phase Ember terminée.")
        return {"df_ember": df_ember_last}

    # -----------------------------------------------------------------
    # Méthodes privées
    # -----------------------------------------------------------------

    def _filter_latest_per_country(self, df: DataFrame) -> DataFrame:
        """
        Filtre la valeur la plus recente par pays, en excluant les agregats EU

        Parameters
        ----------
        df : DataFrame
            DataFrame Ember brut avec colonnes entity_code, date, emissions_intensity_gco2_per_kwh

        Returns
        -------
        DataFrame
            DataFrame avec une seule ligne par pays (la plus récente)
        """
        w_last = Window.partitionBy("entity_code").orderBy(F.desc("date"))
        return (
            df
            .withColumn("rn", F.row_number().over(w_last))
            .filter(F.col("rn") == 1)
            .filter(~F.col("entity_code").startswith("EU"))
        )

    def _convert_alpha3_to_alpha2(self, df: DataFrame) -> DataFrame:
        """
        Convertit les codes pays ISO alpha-3 en alpha-2 via pycountry

        Parameters
        ----------
        df : DataFrame
            DataFrame avec colonne entity_code (alpha-3)

        Returns
        -------
        DataFrame
            DataFrame avec colonne country_code ajoutée (alpha-2)
        """
        a3_a2_dict = {
            c.alpha_3: c.alpha_2
            for c in pycountry.countries
            if hasattr(c, "alpha_3") and hasattr(c, "alpha_2")
        }
        a3_a2_map = F.create_map(
            [F.lit(x) for k, v in a3_a2_dict.items() for x in (k, v)]
        )
        return df.withColumn("country_code", a3_a2_map[F.col("entity_code")])

    def _handle_micro_states(self, df: DataFrame) -> DataFrame:
        """
        Mappe les micro-états vers le pays voisin le plus pertinent

        Parameters
        ----------
        df : DataFrame
            DataFrame avec colonne entity_code et country_code

        Returns
        -------
        DataFrame
            DataFrame avec country_code corrigé pour Monaco, Vatican, Liechtenstein
        """
        for a3, a2 in self.config.MICRO_STATES_MAP.items():
            df = df.withColumn(
                "country_code",
                F.when(F.col("entity_code") == a3, F.lit(a2))
                 .otherwise(F.col("country_code"))
            )
        return df

    def _compute_andorra_average(self, df: DataFrame) -> DataFrame:
        """
        Calcule l'intensité carbone de l'Andorre comme moyenne de la France et l'Espagne

        Parameters
        ----------
        df : DataFrame
            DataFrame Ember avec colonnes entity_code, emissions_intensity_gco2_per_kwh, country_code

        Returns
        -------
        DataFrame
            DataFrame avec une ligne Andorre ajoutée si FR et ES sont dispo
        """
        rows = (
            df.filter(F.col("entity_code").isin("FRA", "ESP"))
            .groupBy("entity_code")
            .agg(F.first("emissions_intensity_gco2_per_kwh").alias("intensity"))
            .collect()
        )
        vals = {r["entity_code"]: r["intensity"] for r in rows}

        if ("FRA" in vals and "ESP" in vals
                and vals["FRA"] is not None
                and vals["ESP"] is not None):
            ad_v = (float(vals["FRA"]) + float(vals["ESP"])) / 2.0
            df_add = self.spark.createDataFrame(
                [("AND", ad_v, "AD")],
                schema=["entity_code", "emissions_intensity_gco2_per_kwh", "country_code"]
            )
            df = df.unionByName(df_add, allowMissingColumns=True)

        return df
