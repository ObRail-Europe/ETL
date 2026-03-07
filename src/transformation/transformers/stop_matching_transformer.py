"""
Transformateur pour le matching gare ↔ aéroport

Pour chaque gare, trouve l'aéroport le plus proche dans un rayon de 100 km, et inversement
Les gares et aéroports non matchés sont conservés avec NULL
"""

from typing import Any

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from transformation.transformers.base_transformer import BaseTransformer
from transformation.utils.spark_functions import haversine_dist


class StopMatchingTransformer(BaseTransformer):
    """
    Produit stop_matching.parquet avec colonnes source, gare_id, airport_id, distance_km
    Utilise le geo-bucketing (grille de 1.0°, 9 buckets adjacents) pour limiter
    le nombre de paires à évaluer, puis filtre par distance Haversine < 100 km.
    """

    def get_transformer_name(self) -> str:
        return "stop_matching"

    def transform(self, **kwargs: Any) -> dict[str, DataFrame]:
        df_trip: DataFrame = kwargs["df_trip"]
        df_airports: DataFrame = kwargs["df_airports"]

        self.logger.info("Matching gare ↔ aéroport...")

        df_stops = self._extract_distinct_stops(df_trip)
        df_matched = self._geo_bucket_matching(df_stops, df_airports)
        df_result = self._add_unmatched(df_matched, df_stops, df_airports)
        self._save_parquet(df_result)

        self.logger.info("Phase StopMatching terminée.")
        return {"df_stop_matching": df_result}

    # ----------------------------------------------------------------
    # Méthodes privées
    # ----------------------------------------------------------------

    def _extract_distinct_stops(self, df_trip: DataFrame) -> DataFrame:
        """extrait les arrêts distincts du dataFrame trip avec leur source"""
        df_stops = (
            df_trip
            .select("source", "stop_id", "stop_lat", "stop_lon")
            .distinct()
            .filter(
                F.col("stop_lat").isNotNull()
                & F.col("stop_lon").isNotNull()
            )
        )
        self.logger.debug(f"Arrêts distincts : {df_stops.count()}")
        return df_stops

    def _geo_bucket_matching(
        self,
        df_stops: DataFrame,
        df_airports: DataFrame,
    ) -> DataFrame:
        """
        Match gares et aeroports via geo-bucketing + Haversine < 100 km

        Pour chaque gare : l'aéroport le plus proche
        Pour chaque aéroport : la gare la plus proche
        Union des deux résultats, dédupliqué
        """
        cfg = self.config
        bucket_size = cfg.STOP_MATCHING_BUCKET_SIZE
        max_dist = cfg.STOP_MATCHING_MAX_DIST_M

        # bucketer les aéroports
        df_airports_b = (
            df_airports
            .select(
                F.col("ident").alias("airport_id"),
                F.col("latitude_deg").alias("airport_lat"),
                F.col("longitude_deg").alias("airport_lon"),
            )
            .withColumn("apt_lat_b", F.floor(F.col("airport_lat") / bucket_size))
            .withColumn("apt_lon_b", F.floor(F.col("airport_lon") / bucket_size))
        )

        # 9 buckets adjacents pour les gares
        offsets = F.array(*[
            F.struct(F.lit(dlat).alias("dlat"), F.lit(dlon).alias("dlon"))
            for dlat in [-1, 0, 1]
            for dlon in [-1, 0, 1]
        ])
        df_stops_b = (
            df_stops
            .withColumn("offset", F.explode(offsets))
            .withColumn(
                "search_lat_b",
                F.floor(F.col("stop_lat") / bucket_size) + F.col("offset.dlat")
            )
            .withColumn(
                "search_lon_b",
                F.floor(F.col("stop_lon") / bucket_size) + F.col("offset.dlon")
            )
        )

        # jointure par bucket
        self.logger.debug("Jointure geo-bucketing gares × aéroports...")
        df_paired = df_stops_b.join(
            F.broadcast(df_airports_b),
            (df_stops_b.search_lat_b == df_airports_b.apt_lat_b)
            & (df_stops_b.search_lon_b == df_airports_b.apt_lon_b)
        )

        # distance Haversine et filtre < 100 km
        df_paired = (
            df_paired
            .withColumn(
                "dist_m",
                haversine_dist(
                    F.col("stop_lat"), F.col("stop_lon"),
                    F.col("airport_lat"), F.col("airport_lon"),
                )
            )
            .filter(F.col("dist_m") < max_dist)
            .withColumn("distance_km", F.round(F.col("dist_m") / 1000.0, 2))
        )

        # pour chaque gare : l'aéroport le plus proche
        w_stop = Window.partitionBy("source", "stop_id").orderBy(F.asc("dist_m"))
        df_stop_best = (
            df_paired
            .withColumn("rn", F.row_number().over(w_stop))
            .filter(F.col("rn") == 1)
            .select(
                "source",
                F.col("stop_id").alias("gare_id"),
                "airport_id",
                "distance_km",
            )
        )

        # pour chaque aéroport : la gare la plus proche
        w_airport = Window.partitionBy("airport_id").orderBy(F.asc("dist_m"))
        df_airport_best = (
            df_paired
            .withColumn("rn", F.row_number().over(w_airport))
            .filter(F.col("rn") == 1)
            .select(
                "source",
                F.col("stop_id").alias("gare_id"),
                "airport_id",
                "distance_km",
            )
        )

        # union et dedup
        df_matched = (
            df_stop_best
            .unionByName(df_airport_best)
            .dropDuplicates(["source", "gare_id", "airport_id"])
        )

        self.logger.debug(f"Paires gare-aéroport matchées : {df_matched.count()}")
        return df_matched

    def _add_unmatched(
        self,
        df_matched: DataFrame,
        df_stops: DataFrame,
        df_airports: DataFrame,
    ) -> DataFrame:
        """ajoute les gares et aéroports non matchés avec NULL"""
        # gares non matchees
        matched_stops = df_matched.select("source", "gare_id").distinct()
        df_unmatched_stops = (
            df_stops
            .select("source", F.col("stop_id").alias("gare_id"))
            .join(matched_stops, ["source", "gare_id"], "left_anti")
            .withColumn("airport_id", F.lit(None).cast("string"))
            .withColumn("distance_km", F.lit(None).cast("double"))
        )

        # aéroports non matchés
        matched_airports = df_matched.select("airport_id").distinct()
        df_unmatched_airports = (
            df_airports
            .select(F.col("ident").alias("airport_id"))
            .join(matched_airports, "airport_id", "left_anti")
            .withColumn("source", F.lit(None).cast("string"))
            .withColumn("gare_id", F.lit(None).cast("string"))
            .withColumn("distance_km", F.lit(None).cast("double"))
        )

        # union finale
        df_result = (
            df_matched
            .unionByName(df_unmatched_stops)
            .unionByName(df_unmatched_airports)
        )

        self.logger.debug(
            f"Stop matching final : {df_result.count()} lignes "
            f"(dont {df_unmatched_stops.count()} gares non matchées, "
            f"{df_unmatched_airports.count()} aéroports non matchés)"
        )
        return df_result

    def _save_parquet(self, df: DataFrame) -> None:
        """sauvegarde stop_matching.parquet"""
        cfg = self.config

        self.logger.info("Sauvegarde stop_matching.parquet...")
        df.coalesce(cfg.DIM_COALESCE_PARTITIONS).write.mode("overwrite").parquet(
            str(cfg.STOP_MATCHING_OUTPUT_PATH)
        )
        self.logger.debug(f"stop_matching sauvegardé : {cfg.STOP_MATCHING_OUTPUT_PATH}")
