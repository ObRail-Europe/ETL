"""
GoldAggregator – 5 agrégations Spark pour construire la couche gold.

Chaque méthode correspond à une table PostgreSQL finale :
1. build_gold_train()              → gold_routes_train       (O/D train dédoublonnées)
2. build_gold_flight()             → gold_routes_flight      (paires aéroport enrichies)
3. build_gold_agg()                → gold_routes_agglomere   (union train + avion)
4. build_compare_candidates()      → gold_compare_candidates (candidats comparaison)
5. build_compare_best()            → gold_compare_best       (meilleure option par O/D)
"""

from typing import Any

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from common.logging import SparkLogger
from chargement.config.settings import ChargementConfig


class GoldAggregator:
    """
    Construit les 5 DataFrames gold depuis les parquets silver.

    Le schéma final est identique entre gold_routes_train et gold_routes_flight
    pour permettre l'UNION dans gold_routes_agglomere.

    Colonnes du schéma unifié gold_routes :
        source, trip_id, mode, destination, trip_short_name,
        agency_name, agency_timezone,
        service_id, route_id, route_type, route_short_name, route_long_name,
        departure_station, departure_city, departure_country, departure_time,
        departure_parent_station,
        arrival_station, arrival_city, arrival_country, arrival_time,
        arrival_parent_station,
        service_start_date, service_end_date, days_of_week,
        is_night_train, distance_km, co2_per_pkm, emissions_co2
    """

    # Colonnes gold communes (ordre canonique)
    GOLD_COLS = [
        "source", "trip_id", "mode", "destination", "trip_short_name",
        "agency_name", "agency_timezone",
        "service_id", "route_id", "route_type", "route_short_name", "route_long_name",
        "departure_station", "departure_city", "departure_country", "departure_time",
        "departure_parent_station",
        "arrival_station", "arrival_city", "arrival_country", "arrival_time",
        "arrival_parent_station",
        "service_start_date", "service_end_date", "days_of_week",
        "is_night_train", "distance_km", "co2_per_pkm", "emissions_co2",
    ]

    def __init__(
        self,
        spark: SparkSession,
        logger: SparkLogger,
        config: ChargementConfig,
    ):
        self.spark = spark
        self.logger = logger
        self.config = config

    # ──────────────────────────────────────────────────────────────
    # Chargement des parquets silver
    # ──────────────────────────────────────────────────────────────

    def load_silver(self) -> dict[str, DataFrame]:
        """
        Charge les 5 parquets silver depuis data/processed/.

        Returns
        -------
        dict avec clés : train_trip, flight, localite, emission, stop_matching
        """
        cfg = self.config
        self.logger.info("Chargement des parquets silver...")

        dfs: dict[str, DataFrame] = {
            "train_trip":    self.spark.read.parquet(str(cfg.TRAIN_TRIP_PATH)),
            "flight":        self.spark.read.parquet(str(cfg.FLIGHT_PATH)),
            "localite":      self.spark.read.parquet(str(cfg.LOCALITE_PATH)),
            "emission":      self.spark.read.parquet(str(cfg.EMISSION_PATH)),
            "stop_matching": self.spark.read.parquet(str(cfg.STOP_MATCHING_PATH)),
        }

        for name, df in dfs.items():
            self.logger.info(f"  ✓ {name} : {df.count():,} lignes")

        return dfs

    # ──────────────────────────────────────────────────────────────
    # 1. Gold Train
    # ──────────────────────────────────────────────────────────────

    def build_gold_train(
        self,
        df_train_trip: DataFrame,
        df_localite: DataFrame,
        df_emission: DataFrame,
    ) -> DataFrame:
        """
        Construit les paires O/D train agglomérées.

        Étapes :
        1. Enrichissement ville + facteur co2 (jointures)
        2. Calcul distance cumulée par stop
        3. Génération paires départ × arrivée (dep_seq < arr_seq)
        4. Agrégation trip-level (route_id, agency…)
        5. Dédoublonnage par gare/ville/horaires + fusion days_of_week (OR bit à bit)
        """
        self.logger.log_section("GOLD TRAIN – paires O/D", level="INFO")
        cfg = self.config

        # 1) Enrichissement ville + emission
        df_enriched = (
            df_train_trip.alias("t")
            .join(
                df_localite.select(
                    F.col("city_id").alias("loc_city_id"),
                    F.col("city_name"),
                ).alias("l"),
                F.col("t.city_id") == F.col("l.loc_city_id"),
                "left",
            )
            .join(
                df_emission.select(
                    F.col("emission_ref").cast("long").alias("em_ref"),
                    F.col("emission_gCO2e_per_p_km").alias("co2_per_pkm"),
                ).alias("e"),
                F.col("t.emission_ref").cast("long") == F.col("e.em_ref"),
                "left",
            )
            .drop("loc_city_id", "em_ref")
        )

        # 2) Distance cumulée depuis le 1er arrêt du trip
        w_cumul = (
            Window.partitionBy("source", "trip_id")
            .orderBy("stop_sequence")
            .rowsBetween(Window.unboundedPreceding, -1)
        )
        df_work = (
            df_enriched
            .withColumn(
                "cumul_dist_m",
                F.coalesce(
                    F.sum(F.coalesce(F.col("segment_dist_m"), F.lit(0.0))).over(w_cumul),
                    F.lit(0.0),
                ),
            )
            .repartition(cfg.GOLD_SHUFFLE_PARTITIONS, "source", "trip_id")
            .persist()
        )

        self.logger.info(f"  Stops train total : {df_work.count():,}")

        # 3) Côtés départ et arrivée
        df_dep = df_work.select(
            F.col("source").alias("trip_source"),
            F.col("trip_id"),
            F.col("stop_sequence").alias("dep_seq"),
            F.col("stop_name").alias("departure_station"),
            F.col("city_name").alias("departure_city"),
            F.col("country_name").alias("departure_country"),
            F.col("departure_time"),
            F.col("parent_station").alias("departure_parent_station"),
            F.col("cumul_dist_m").alias("dep_cumul"),
        )
        df_arr = df_work.select(
            F.col("source").alias("arr_source"),
            F.col("trip_id").alias("_trip_id_arr"),
            F.col("stop_sequence").alias("arr_seq"),
            F.col("stop_name").alias("arrival_station"),
            F.col("city_name").alias("arrival_city"),
            F.col("country_name").alias("arrival_country"),
            F.col("arrival_time"),
            F.col("parent_station").alias("arrival_parent_station"),
            F.col("cumul_dist_m").alias("arr_cumul"),
        )

        # 4) Attributs trip-level
        df_trip_attrs = (
            df_work
            .select(
                "source", "trip_id", "route_id", "service_id", "route_type",
                "route_short_name", "route_long_name", "trip_short_name",
                "agency_name", "agency_timezone",
                "start_date", "end_date", "days_of_week", "is_night_train", "co2_per_pkm",
            )
            .dropDuplicates(["source", "trip_id"])
        )

        # 5) Paires O/D
        df_pairs = (
            df_dep.join(
                df_arr,
                (F.col("trip_source") == F.col("arr_source"))
                & (F.col("trip_id") == F.col("_trip_id_arr"))
                & (F.col("dep_seq") < F.col("arr_seq")),
                "inner",
            )
            .withColumn("distance_km", F.round(
                (F.col("arr_cumul") - F.col("dep_cumul")) / F.lit(1000.0), 2
            ))
            .drop("arr_source", "_trip_id_arr", "dep_cumul", "arr_cumul", "dep_seq", "arr_seq")
            .withColumnRenamed("trip_source", "source")
        )

        # 6) Dataset gold brut
        df_raw = (
            df_pairs
            .join(df_trip_attrs, ["source", "trip_id"], "inner")
            .withColumn("mode", F.lit("train"))
            .withColumn("destination", F.coalesce(F.col("route_long_name"), F.col("trip_short_name")))
            .withColumn("route_type", F.col("route_type").cast("string"))
            .withColumnRenamed("start_date", "service_start_date")
            .withColumnRenamed("end_date", "service_end_date")
            .withColumn("emissions_co2", F.round(F.col("distance_km") * F.col("co2_per_pkm"), 3))
        )

        # 7) Dédoublonnage – fusion days_of_week par OR bit à bit
        dedup_keys = [
            "source", "mode",
            "departure_station", "departure_city", "departure_country",
            "departure_parent_station", "departure_time",
            "arrival_station", "arrival_city", "arrival_country",
            "arrival_parent_station", "arrival_time",
            "route_type",
        ]

        def merged_days_expr(col_name: str = "days_of_week") -> F.Column:
            return F.concat(*[
                F.max(
                    F.when(
                        F.substring(F.coalesce(F.col(col_name), F.lit("0000000")), i, 1).isin("0", "1"),
                        F.substring(F.coalesce(F.col(col_name), F.lit("0000000")), i, 1).cast("int"),
                    ).otherwise(F.lit(0))
                ).cast("string")
                for i in range(1, 8)
            ])

        preserve_cols = [
            "trip_id", "destination", "trip_short_name",
            "agency_name", "agency_timezone",
            "service_id", "route_id", "route_short_name", "route_long_name",
            "service_start_date", "service_end_date",
            "is_night_train", "distance_km", "co2_per_pkm", "emissions_co2",
        ]

        agg_exprs: list[F.Column] = [merged_days_expr("days_of_week").alias("days_of_week")]
        agg_exprs.extend([F.first(F.col(c), ignorenulls=True).alias(c) for c in preserve_cols])

        df_gold_train = (
            df_raw
            .groupBy(*dedup_keys)
            .agg(*agg_exprs)
            .select(self.GOLD_COLS)
        )

        df_work.unpersist()

        n_raw = df_raw.count()
        n_gold = df_gold_train.count()
        self.logger.info(f"  Gold train brut    : {n_raw:,}")
        self.logger.info(f"  Gold train dédupé  : {n_gold:,}")

        # Sauvegarde
        (
            df_gold_train
            .coalesce(cfg.GOLD_COALESCE_TRAIN)
            .write.mode("overwrite")
            .parquet(str(cfg.GOLD_TRAIN_PATH))
        )
        self.logger.info(f"  ✓ Sauvegardé : {cfg.GOLD_TRAIN_PATH}")

        return df_gold_train

    # ──────────────────────────────────────────────────────────────
    # 2. Gold Flight
    # ──────────────────────────────────────────────────────────────

    def build_gold_flight(
        self,
        df_flight: DataFrame,
        df_localite: DataFrame,
        df_emission: DataFrame,
    ) -> DataFrame:
        """
        Construit les paires O/D avion enrichies depuis OurAirports.

        Labels de route construits depuis trajet_type (ex: small_large_1000km)
        → "Court courrier - Petit Aéroport vers Grand Aéroport"
        """
        self.logger.log_section("GOLD FLIGHT – paires aéroport", level="INFO")
        cfg = self.config

        df_loc_origin = df_localite.select(
            F.col("city_id").alias("origin_city_id_l"),
            F.col("city_name").alias("departure_city"),
        )
        df_loc_dest = df_localite.select(
            F.col("city_id").alias("dest_city_id_l"),
            F.col("city_name").alias("arrival_city"),
        )

        # Mapping libellés taille aéroport
        size_label = F.create_map(
            F.lit("small"),  F.lit("Petit Aéroport"),
            F.lit("medium"), F.lit("Moyen Aéroport"),
            F.lit("large"),  F.lit("Grand Aéroport"),
        )
        haul_label = (
            F.when(F.col("haul_band") == F.lit("1000km"), F.lit("Court courrier"))
            .when(F.col("haul_band") == F.lit("3000km"), F.lit("Moyen Courrier"))
            .otherwise(F.lit("Long Courrier"))
        )

        df_gold_flight = (
            df_flight.alias("f")
            .join(df_loc_origin.alias("lo"),
                  F.col("f.origin_city_id") == F.col("lo.origin_city_id_l"), "left")
            .join(df_loc_dest.alias("ld"),
                  F.col("f.dest_city_id") == F.col("ld.dest_city_id_l"), "left")
            .join(
                df_emission.select(
                    F.col("emission_ref").cast("long").alias("em_ref"),
                    F.col("emission_gCO2e_per_p_km").alias("co2_per_pkm"),
                ).alias("e"),
                F.col("f.emission_ref").cast("long") == F.col("e.em_ref"),
                "left",
            )
            .withColumn("source", F.lit("ourairports"))
            .withColumn("trip_id", F.concat_ws("__",
                F.col("f.origin_id").cast("string"),
                F.col("f.dest_id").cast("string"),
            ))
            .withColumn("mode", F.lit("flight"))
            .withColumn("destination", F.concat_ws(" - ",
                F.col("f.origin_name"), F.col("f.dest_name"),
            ))
            .withColumn("trip_short_name", F.concat_ws(" - ",
                F.col("f.origin_ident"), F.col("f.dest_ident"),
            ))
            .withColumn("route_type", F.col("f.trajet_type"))
            .withColumn("route_tokens", F.split(F.col("f.trajet_type"), "_"))
            .withColumn("origin_size",  F.element_at(F.col("route_tokens"), 1))
            .withColumn("dest_size",    F.element_at(F.col("route_tokens"), 2))
            .withColumn("haul_band",    F.element_at(F.col("route_tokens"), 3))
            .withColumn("route_long_name", F.concat_ws(" - ",
                haul_label,
                F.concat(
                    F.element_at(size_label, F.col("origin_size")),
                    F.lit(" vers "),
                    F.element_at(size_label, F.col("dest_size")),
                ),
            ))
            .withColumn("distance_km",   F.round(F.col("f.distance_km"), 2))
            .withColumn("emissions_co2", F.round(F.col("distance_km") * F.col("co2_per_pkm"), 3))
            .select(
                "source", "trip_id", "mode", "destination", "trip_short_name",
                F.lit(None).cast(StringType()).alias("agency_name"),
                F.lit(None).cast(StringType()).alias("agency_timezone"),
                F.lit(None).cast(StringType()).alias("service_id"),
                F.lit(None).cast(StringType()).alias("route_id"),
                "route_type",
                F.lit(None).cast(StringType()).alias("route_short_name"),
                "route_long_name",
                F.col("f.origin_name").alias("departure_station"),
                "departure_city",
                F.col("f.origin_country").alias("departure_country"),
                F.lit(None).cast(StringType()).alias("departure_time"),
                F.lit(None).cast(StringType()).alias("departure_parent_station"),
                F.col("f.dest_name").alias("arrival_station"),
                "arrival_city",
                F.col("f.dest_country").alias("arrival_country"),
                F.lit(None).cast(StringType()).alias("arrival_time"),
                F.lit(None).cast(StringType()).alias("arrival_parent_station"),
                F.lit(None).cast(StringType()).alias("service_start_date"),
                F.lit(None).cast(StringType()).alias("service_end_date"),
                F.lit(None).cast(StringType()).alias("days_of_week"),
                F.lit(None).cast("boolean").alias("is_night_train"),
                "distance_km", "co2_per_pkm", "emissions_co2",
            )
        )

        df_gold_flight = df_gold_flight.select(self.GOLD_COLS)

        n = df_gold_flight.count()
        self.logger.info(f"  Gold flight : {n:,} lignes")

        (
            df_gold_flight
            .coalesce(cfg.GOLD_COALESCE_FLIGHT)
            .write.mode("overwrite")
            .parquet(str(cfg.GOLD_FLIGHT_PATH))
        )
        self.logger.info(f"  ✓ Sauvegardé : {cfg.GOLD_FLIGHT_PATH}")

        return df_gold_flight

    # ──────────────────────────────────────────────────────────────
    # 3. Gold aggloméré (UNION train + avion)
    # ──────────────────────────────────────────────────────────────

    def build_gold_agg(
        self,
        df_gold_train: DataFrame,
        df_gold_flight: DataFrame,
    ) -> DataFrame:
        """Union des datasets train et avion dans un schéma commun."""
        self.logger.log_section("GOLD AGG – union train + avion", level="INFO")
        cfg = self.config

        df_agg = df_gold_train.unionByName(df_gold_flight)

        n = df_agg.count()
        self.logger.info(f"  Gold aggloméré : {n:,} lignes")

        (
            df_agg
            .coalesce(cfg.GOLD_COALESCE_AGG)
            .write.mode("overwrite")
            .parquet(str(cfg.GOLD_AGG_PATH))
        )
        self.logger.info(f"  ✓ Sauvegardé : {cfg.GOLD_AGG_PATH}")

        return df_agg

    # ──────────────────────────────────────────────────────────────
    # 4. Candidats comparaison train vs avion
    # ──────────────────────────────────────────────────────────────

    def build_compare_candidates(
        self,
        df_gold_train: DataFrame,
        df_flight: DataFrame,
        df_emission: DataFrame,
        df_stop_matching: DataFrame,
    ) -> DataFrame:
        """
        Construit tous les candidats de comparaison train vs avion.

        Pour chaque trajet train O/D :
        - Candidat 1 : le train lui-même (0 correspondance)
        - Candidat 2 : le meilleur vol accessible depuis les gares (via stop_matching)
        """
        self.logger.log_section("GOLD COMPARE – candidats", level="INFO")
        cfg = self.config

        # Utilitaire : HH:MM:SS → secondes
        def hms_to_seconds(col_name: str) -> F.Column:
            parts = F.split(F.col(col_name), ":")
            return (
                F.coalesce(parts.getItem(0).cast("int"), F.lit(0)) * F.lit(3600)
                + F.coalesce(parts.getItem(1).cast("int"), F.lit(0)) * F.lit(60)
                + F.coalesce(parts.getItem(2).cast("int"), F.lit(0))
            )

        sec_in_day = F.lit(24 * 3600)
        dep_sec = hms_to_seconds("departure_time")
        arr_sec = hms_to_seconds("arrival_time")

        train_duration_min = F.when(
            F.col("departure_time").isNotNull() & F.col("arrival_time").isNotNull(),
            F.round(
                F.when(arr_sec >= dep_sec, arr_sec - dep_sec)
                 .otherwise((arr_sec + sec_in_day) - dep_sec) / F.lit(60.0),
                1,
            ),
        ).otherwise(F.round(F.col("distance_km") / F.lit(cfg.TRAIN_DEFAULT_SPEED_KMH) * F.lit(60.0), 1))

        # Candidat 1 – train direct
        df_cmp_train = (
            df_gold_train
            .select(
                "source", "trip_id",
                "departure_station", "departure_city", "departure_country",
                "arrival_station", "arrival_city", "arrival_country",
                "departure_parent_station", "arrival_parent_station",
                "departure_time", "arrival_time",
                "distance_km", "emissions_co2", "days_of_week", "mode",
            )
            .withColumn("candidate_mode", F.lit("train"))
            .withColumn("candidate_id", F.col("trip_id"))
            .withColumn("correspondence_count", F.lit(0))
            .withColumn("duration_min", train_duration_min)
            .withColumn("comparison_distance_km", F.col("distance_km"))
            .withColumn("comparison_emissions_co2", F.col("emissions_co2"))
        )

        # Meilleur vol par paire d'aéroports
        df_flight_enriched = (
            df_flight.alias("f")
            .join(
                df_emission.select(
                    F.col("emission_ref").cast("long").alias("em_ref"),
                    F.col("emission_gCO2e_per_p_km").alias("co2_per_pkm"),
                ).alias("e"),
                F.col("f.emission_ref").cast("long") == F.col("e.em_ref"),
                "left",
            )
            .select(
                F.col("f.origin_ident").alias("dep_airport_id"),
                F.col("f.dest_ident").alias("arr_airport_id"),
                F.col("f.origin_name").alias("flight_dep_station"),
                F.col("f.dest_name").alias("flight_arr_station"),
                F.round(F.col("f.distance_km"), 2).alias("flight_distance_km"),
                F.round(
                    F.col("f.distance_km") / F.lit(cfg.FLIGHT_CRUISE_SPEED_KMH) * F.lit(60.0)
                    + F.lit(cfg.FLIGHT_AIRPORT_OVERHEAD_MIN), 1
                ).alias("flight_duration_min"),
                F.round(F.col("f.distance_km") * F.col("co2_per_pkm"), 3).alias("flight_emissions_co2"),
            )
        )

        w_flight_pair = (
            Window.partitionBy("dep_airport_id", "arr_airport_id")
            .orderBy(
                F.col("flight_duration_min").asc_nulls_last(),
                F.col("flight_emissions_co2").asc_nulls_last(),
            )
        )
        df_flight_best = (
            df_flight_enriched
            .withColumn("rn", F.row_number().over(w_flight_pair))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        # Candidat 2 – vol depuis gare proche (via stop_matching)
        df_match_dep = df_stop_matching.select(
            F.col("gare_id").alias("dep_gare_id"),
            F.col("airport_id").alias("dep_airport_id"),
            F.col("distance_km").alias("dep_access_km"),
        )
        df_match_arr = df_stop_matching.select(
            F.col("gare_id").alias("arr_gare_id"),
            F.col("airport_id").alias("arr_airport_id"),
            F.col("distance_km").alias("arr_access_km"),
        )

        _norm_cols = [
            "source", "trip_id",
            "departure_station", "departure_city", "departure_country",
            "arrival_station", "arrival_city", "arrival_country",
            "departure_parent_station", "arrival_parent_station",
            "days_of_week",
            "candidate_mode", "candidate_id", "correspondence_count",
            "duration_min", "comparison_distance_km", "comparison_emissions_co2",
        ]

        df_cmp_flight = (
            df_cmp_train.alias("t")
            .join(df_match_dep.alias("md"),
                  F.col("t.departure_parent_station") == F.col("md.dep_gare_id"), "inner")
            .join(df_match_arr.alias("ma"),
                  F.col("t.arrival_parent_station") == F.col("ma.arr_gare_id"), "inner")
            .join(
                df_flight_best.alias("fb"),
                (F.col("md.dep_airport_id") == F.col("fb.dep_airport_id"))
                & (F.col("ma.arr_airport_id") == F.col("fb.arr_airport_id")),
                "inner",
            )
            .withColumn("candidate_mode", F.lit("flight"))
            .withColumn("candidate_id", F.concat_ws("__",
                F.col("fb.dep_airport_id"), F.col("fb.arr_airport_id"),
            ))
            .withColumn("access_km",
                F.round(F.col("md.dep_access_km") + F.col("ma.arr_access_km"), 2))
            .withColumn("correspondence_count", F.lit(2))
            .withColumn("duration_min",
                F.round(
                    F.col("fb.flight_duration_min")
                    + (F.col("access_km") / F.lit(cfg.AIRPORT_ACCESS_SPEED_KMH) * F.lit(60.0)),
                    1,
                ))
            .withColumn("comparison_distance_km",
                F.round(F.col("fb.flight_distance_km") + F.col("access_km"), 2))
            .withColumn("comparison_emissions_co2", F.col("fb.flight_emissions_co2"))
            .select("t.source", "t.trip_id",
                    "t.departure_station", "t.departure_city", "t.departure_country",
                    "t.arrival_station", "t.arrival_city", "t.arrival_country",
                    "t.departure_parent_station", "t.arrival_parent_station",
                    "t.days_of_week",
                    "candidate_mode", "candidate_id", "correspondence_count",
                    "duration_min", "comparison_distance_km", "comparison_emissions_co2")
        )

        df_cmp_train_norm = df_cmp_train.select(_norm_cols)
        df_candidates = df_cmp_train_norm.unionByName(df_cmp_flight)

        n = df_candidates.count()
        self.logger.info(f"  Candidats comparaison : {n:,}")

        (
            df_candidates
            .coalesce(cfg.GOLD_COALESCE_CANDIDATES)
            .write.mode("overwrite")
            .parquet(str(cfg.GOLD_COMPARE_CANDIDATES_PATH))
        )
        self.logger.info(f"  ✓ Sauvegardé : {cfg.GOLD_COMPARE_CANDIDATES_PATH}")

        return df_candidates

    # ──────────────────────────────────────────────────────────────
    # 5. Meilleure option par O/D
    # ──────────────────────────────────────────────────────────────

    def build_compare_best(self, df_candidates: DataFrame) -> DataFrame:
        """
        Sélectionne la meilleure option par paire O/D.

        Critères (dans l'ordre) :
        1. Durée la plus courte (asc_nulls_last)
        2. Moins de correspondances
        3. Moins d'émissions CO2
        """
        self.logger.log_section("GOLD COMPARE BEST – meilleure option", level="INFO")
        cfg = self.config

        w_best = (
            Window.partitionBy(
                "departure_station", "departure_city", "departure_country",
                "arrival_station", "arrival_city", "arrival_country",
            )
            .orderBy(
                F.col("duration_min").asc_nulls_last(),
                F.col("correspondence_count").asc(),
                F.col("comparison_emissions_co2").asc_nulls_last(),
            )
        )

        df_best = (
            df_candidates
            .withColumn("selection_rank", F.row_number().over(w_best))
            .filter(F.col("selection_rank") == 1)
            .drop("selection_rank")
        )

        n = df_best.count()
        self.logger.info(f"  Meilleures options : {n:,} O/D")

        (
            df_best
            .coalesce(cfg.GOLD_COALESCE_BEST)
            .write.mode("overwrite")
            .parquet(str(cfg.GOLD_COMPARE_BEST_PATH))
        )
        self.logger.info(f"  ✓ Sauvegardé : {cfg.GOLD_COMPARE_BEST_PATH}")

        return df_best

    # ──────────────────────────────────────────────────────────────
    # run() – point d'entrée unique
    # ──────────────────────────────────────────────────────────────

    def run(self) -> dict[str, Any]:
        """
        Exécute les 5 agrégations gold dans l'ordre.

        Returns
        -------
        dict avec statut, métriques et dataframes gold produits
        """
        dataframes: dict[str, DataFrame] = {}
        metrics: dict[str, int] = {}

        try:
            silver = self.load_silver()

            df_gold_train = self.build_gold_train(
                silver["train_trip"], silver["localite"], silver["emission"]
            )
            dataframes["gold_train"] = df_gold_train
            metrics["gold_train_rows"] = df_gold_train.count()

            df_gold_flight = self.build_gold_flight(
                silver["flight"], silver["localite"], silver["emission"]
            )
            dataframes["gold_flight"] = df_gold_flight
            metrics["gold_flight_rows"] = df_gold_flight.count()

            df_gold_agg = self.build_gold_agg(df_gold_train, df_gold_flight)
            dataframes["gold_agg"] = df_gold_agg
            metrics["gold_agg_rows"] = df_gold_agg.count()

            df_candidates = self.build_compare_candidates(
                df_gold_train, silver["flight"], silver["emission"], silver["stop_matching"]
            )
            dataframes["gold_compare_candidates"] = df_candidates
            metrics["gold_compare_candidates_rows"] = df_candidates.count()

            df_best = self.build_compare_best(df_candidates)
            dataframes["gold_compare_best"] = df_best
            metrics["gold_compare_best_rows"] = df_best.count()

            return {
                "status": "SUCCESS",
                "metrics": metrics,
                "dataframes": dataframes,
            }

        except Exception as exc:
            self.logger.error(f"Échec GoldAggregator : {exc}")
            return {
                "status": "FAILED",
                "error": str(exc),
                "metrics": metrics,
                "dataframes": dataframes,
            }
