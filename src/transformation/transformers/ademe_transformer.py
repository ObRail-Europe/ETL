"""
Transformateur pour les données ADEME (facteurs d'émission aériens)

Charge les facteurs d'émission de la base carbone ADEME, filtre les données,
valides, catégorise par taille d'avion et distance, puis calcule les
facteurs pondérés pour chaque type de trajet aérien
"""

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType,
)
import pyspark.sql.functions as F

from transformation.transformers.base_transformer import BaseTransformer


class AdemeTransformer(BaseTransformer):
    """
    Produit une table avec le schéma unifié (emission_ref, emission_gCO2_per_pkm,
    detail) contenant 7 lignes de base + 7 lignes de mapping pondéré = 14 lignes
    """

    def get_transformer_name(self) -> str:
        return "ademe"

    def transform(self, **kwargs: Any) -> dict[str, DataFrame]:
        self.logger.info("Chargement ADEME...")

        df = self._load_and_filter()
        df = self._parse_and_categorize(df)
        df_base = self._aggregate(df)
        df_full = self._add_weighted_mappings(df_base)
        df_full = self._format_unified_schema(df_full)

        self.logger.info("Phase ADEME terminée.")
        return {"df_ademe": df_full}

    # -----------------------------------------------------------------
    # Méthodes privées
    # ------------------------------------------------------------------

    def _load_and_filter(self) -> DataFrame:
        """charge le parquet ADEME et filtre Statut 'valide générique' + Type_poste NULL"""
        cfg = self.config
        df = self.spark.read.parquet(
            str(cfg.ADEME_RAW_PATH / "ademe_base_carbone_aerien.parquet")
        )
        self.logger.debug(f"ADEME brut : {df.count()} lignes")

        df = (
            df
            .filter(F.col("Statut_de_l'élément") == "Valide générique")
            .filter(F.col("Type_poste").isNull())
        )
        self.logger.debug(f"Après filtrage statut + type_poste : {df.count()} lignes")
        return df

    def _parse_and_categorize(self, df: DataFrame) -> DataFrame:
        """parse Nom_attribut_français et catégorise directement en seat/distance"""
        df = df.select("Nom_attribut_français", "Total_poste_non_décomposé")

        # catégorisation taille (sièges) : small / medium / large
        df = df.withColumn(
            "seat_category",
            F.when(F.col("Nom_attribut_français").rlike(r"20-50"), F.lit("small"))
             .when(F.col("Nom_attribut_français").rlike(r"51-100"), F.lit("medium"))
             .when(F.col("Nom_attribut_français").rlike(r"101-220"), F.lit("medium"))
             .when(F.col("Nom_attribut_français").rlike(r">220"), F.lit("large"))
             .otherwise(F.lit("unknown"))
        )

        # catégorisation distance (kms) : short / medium / long
        df = df.withColumn(
            "distance_category",
            F.when(F.col("Nom_attribut_français").rlike(r">5000"), F.lit("long"))
             .when(F.col("Nom_attribut_français").rlike(r"1000-2000|2000-5000"), F.lit("medium"))
             .when(F.col("Nom_attribut_français").rlike(r"<500|500-1000"), F.lit("short"))
             .otherwise(F.lit("unknown"))
        )

        return df

    def _aggregate(self, df: DataFrame) -> DataFrame:
        """agrège par (seat_category, distance_category) → moyenne des émissions"""
        df_agg = (
            df
            .groupBy("seat_category", "distance_category")
            .agg(F.avg("Total_poste_non_décomposé").alias("emission_kgCO2_per_pkm"))
        )
        self.logger.debug(f"Lignes de base après agrégation : {df_agg.count()}")
        return df_agg

    def _add_weighted_mappings(self, df_base: DataFrame) -> DataFrame:
        """calcule les 7 facteurs pondérés et les ajoute aux 7 lignes de base."""
        cfg = self.config

        #collecter les facteurs de base en dict {clé: valeur}
        base_dict: dict[str, float] = {}
        for row in df_base.collect():
            key = f"{row['seat_category']}_{row['distance_category']}"
            base_dict[key] = row["emission_kgCO2_per_pkm"]

        self.logger.debug(f"Facteurs de base : {base_dict}")

        # calculer les 7 mappings pondérés
        mapping_rows = []
        for emission_id, trajet_type, weights in cfg.ADEME_EMISSION_WEIGHTS:
            emission_value = sum(
                base_dict.get(base_key, 0.0) * weight
                for base_key, weight in weights.items()
            )
            # construire le detail de la formule
            detail_parts = [
                f"{base_key}×{weight}"
                for base_key, weight in weights.items()
            ]
            detail = (
                f"Aérien {trajet_type}, "
                + " + ".join(detail_parts)
                + f" = {emission_value * 1000:.2f} gCO2/p.km"
            )
            mapping_rows.append((
                emission_id,
                trajet_type,
                trajet_type,
                emission_value,
                detail,
            ))

        mapping_schema = StructType([
            StructField("emission_ref", LongType(), False),
            StructField("seat_category", StringType(), True),
            StructField("distance_category", StringType(), True),
            StructField("emission_kgCO2_per_pkm", DoubleType(), True),
            StructField("detail", StringType(), True),
        ])
        df_mapping = self.spark.createDataFrame(mapping_rows, schema=mapping_schema)

        # ajouter emission_ref et detail aux lignes de bzse
        df_base_with_detail = (
            df_base
            .withColumn(
                "emission_ref",
                F.monotonically_increasing_id()
            )
            .withColumn(
                "detail",
                F.concat(
                    F.lit("Aérien "),
                    F.col("seat_category"), F.lit("_"), F.col("distance_category"),
                    F.lit(", AVG(AVEC/SANS traînées), "),
                    F.round(F.col("emission_kgCO2_per_pkm"), 5).cast("string"),
                    F.lit(" kgCO2e/p.km"),
                )
            )
        )

        return df_base_with_detail.unionByName(df_mapping)

    def _format_unified_schema(self, df: DataFrame) -> DataFrame:
        """projette sur le schéma unifié : emission_ref, emission_gCO2_per_pkm, detail"""
        return df.select(
            F.col("emission_ref"),
            F.round(F.col("emission_kgCO2_per_pkm") * 1000, 3).alias("emission_gCO2e_per_p_km"),
            F.col("detail"),
        )
