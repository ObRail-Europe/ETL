"""
Transformateur pour les données Back-on-Track Night (BOTN)

Charge les 6 tables BOTN, les join, dédoublonne les trajets en Y, normalise les types et calcule les distances Haversine
"""

from typing import Any

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from transformation.transformers.base_transformer import BaseTransformer
from transformation.utils.spark_functions import (
    haversine_dist,
    replace_blank_with_nulls,
    invalid_coord_expr,
    apply_tz_mapping,
)


class BackOnTrackTransformer(BaseTransformer):
    """
    Charge les 6 fichiers Parquet BOTN, construit un DataFrame dénormalisé,
    résout les agency_id composites, renumérote les stop_sequence des trajets
    en Y, et aligne les types avec MDB
    """

    def get_transformer_name(self) -> str:
        return "backontrack"

    def transform(self, **kwargs: Any) -> dict[str, DataFrame]:
        """
        Charge et transforme les données Back-on-Track Night

        Returns
        -------
        dict[str, DataFrame]
            {'df_botn': DataFrame} — DataFrame BOTN normalisé et aligné avec MDB

        Raises
        ------
        RuntimeError
            Si les fichiers BOTN sont manquants
        """
        cfg = self.config
        self.logger.info("BOTN - étape 1/6: chargement des tables sources et jointures GTFS-like.")

        # chargement des 6 tables BOTN
        self.logger.debug("BOTN: lecture des 6 parquets (agencies, routes, stops, trips, calendar, trip_stop).")
        botn_path = cfg.BOTN_RAW_PATH
        df_agencies = self.spark.read.parquet(str(botn_path / "agencies.parquet"))
        df_routes = self.spark.read.parquet(str(botn_path / "routes.parquet"))
        df_stops = self.spark.read.parquet(str(botn_path / "stops.parquet"))
        df_trips = self.spark.read.parquet(str(botn_path / "trips.parquet"))
        df_calendar = self.spark.read.parquet(str(botn_path / "calendar.parquet"))
        df_stop_times = self.spark.read.parquet(str(botn_path / "trip_stop.parquet"))

        # jointures composites BOTN
        self.logger.debug("BOTN: assemblage du graphe trips→routes→stop_times→stops→agencies→calendar.")

        # drop des colonnes dupliquees dans les tables secondaires pour éviter
        # AMBIGUOUS_REFERENCE (version, countries, etc. présents dans plusieurs tables)
        def _safe_cols(df_right: DataFrame, join_keys: list[str], df_left: DataFrame) -> DataFrame:
            """Garde que la clé de jointure + les colonnes propres à df_right."""
            dupes = set(df_right.columns) & set(df_left.columns) - set(join_keys)
            return df_right.drop(*dupes) if dupes else df_right

        df_botn = df_trips
        df_botn = df_botn.join(F.broadcast(_safe_cols(df_routes.drop("agency_id"), ["route_id"], df_botn)), "route_id", "left")
        df_botn = df_botn.join(_safe_cols(df_stop_times, ["trip_id"], df_botn), "trip_id", "left")
        df_botn = df_botn.join(_safe_cols(df_stops, ["stop_id"], df_botn), "stop_id", "left")
        df_botn = df_botn.join(F.broadcast(_safe_cols(df_agencies, ["agency_id"], df_botn)), "agency_id", "left")
        df_botn = df_botn.join(F.broadcast(_safe_cols(df_calendar, ["service_id"], df_botn)), "service_id", "left")
        df_botn = (
            df_botn
            .withColumn("source", F.lit("BOTN"))
            .withColumnRenamed("stop_cityname_romanized", "city")
            .withColumnRenamed("stop_country", "country")
            .withColumnRenamed("trip_headsign", "trip_headsign_temp")
            .withColumnRenamed("itinerary", "trip_headsign")
            .withColumn("country",
                        F.when(F.col("country") == "UK", "GB").otherwise(F.col("country")))
            .withColumn("departure_time", F.col("departure_time").cast("string"))
            .withColumn("arrival_time", F.col("arrival_time").cast("string"))
        )

        # checkpoint post-jointures (tronque le lineage + matérialise)
        # eager=False : enchaînement linéaire immédiat, évite une action dédiée
        df_botn = df_botn.checkpoint(eager=False)
        self.logger.debug("BOTN: checkpoint post-jointures posé pour limiter le lineage Spark.")

        self.logger.info("BOTN - étape 2/6: normalisation structurelle des trajets et calendriers.")
        self.logger.debug("BOTN: dédoublonnage et renumérotation des trajets en Y.")
        df_botn = self._renumber_stop_sequences(df_botn)

        # normalisation des dates start_date/end_date → YYYYMMDD
        self.logger.debug("BOTN: normalisation des dates start/end au format YYYYMMDD.")
        df_botn = self._normalize_dates(df_botn)

        # masque jours de semaine
        self.logger.debug("BOTN: construction days_of_week et colonnes constantes de compatibilité.")
        day_cols = cfg.DAY_COLS
        df_botn = (
            df_botn
            .withColumn(
                "days_of_week",
                F.concat(*[F.coalesce(F.col(c).cast("string"), F.lit("0")) for c in day_cols])
            )
            .withColumn("parent_station", F.lit(None).cast("string"))
            .withColumn("is_night_train", F.lit(True))
            .withColumn("route_type", F.lit(105))
            .filter(F.col("stop_lat").isNotNull() & F.col("stop_lon").isNotNull())
            .withColumn("arrival_time", F.substring_index(F.col("arrival_time"), " ", -1))
            .withColumn("departure_time", F.substring_index(F.col("departure_time"), " ", -1))
        )

        # casts + nettoyage global
        self.logger.info("BOTN - étape 3/6: nettoyage qualité (types, valeurs vides, coordonnées invalides).")
        self.logger.debug("BOTN: casts techniques et nettoyage global des champs texte.")
        df_botn = (
            df_botn
            .withColumn("stop_lat", F.col("stop_lat").cast("double"))
            .withColumn("stop_lon", F.col("stop_lon").cast("double"))
            .withColumn("stop_sequence", F.expr("try_cast(stop_sequence as int)"))
        )
        df_botn = replace_blank_with_nulls(df_botn)

        # filtre stop_sequence nul et coordonnees invalides
        self.logger.debug("BOTN: retrait des trips avec stop_sequence nul ou coordonnées invalides.")
        df_botn = df_botn.filter(F.col("stop_sequence").isNotNull())
        bad_botn_trips = (
            df_botn
            .filter(invalid_coord_expr("stop_lat", "stop_lon"))
            .select("trip_id")
            .distinct()
        )
        df_botn = df_botn.join(F.broadcast(bad_botn_trips), ["trip_id"], "left_anti")

        # correction des fuseaux horaires
        self.logger.info("BOTN - étape 4/6: enrichissement métier (timezone + opérateurs composites).")
        self.logger.debug("BOTN: correction des fuseaux horaires vers des labels IANA cohérents.")
        df_botn = apply_tz_mapping(df_botn, self.config.TZ_MAPPING)

        # résolution des agency_name composites
        self.logger.debug("BOTN: résolution des agency_id composites (ex: CFM/CFR).")
        df_botn = self._resolve_composite_agency_names(df_botn, df_agencies, df_trips)

        self.logger.info("BOTN - étape 5/6: calcul des distances inter-arrêts et purge des segments invalides.")
        self.logger.debug("BOTN: calcul Haversine inter-arrêts avec facteur de détour rail.")
        w_seq = Window.partitionBy("trip_id").orderBy("stop_sequence")
        df_botn = (
            df_botn
            .withColumn("next_stop_lat", F.lead("stop_lat").over(w_seq))
            .withColumn("next_stop_lon", F.lead("stop_lon").over(w_seq))
            .withColumn(
                "segment_dist_m",
                haversine_dist(
                    F.col("stop_lat"), F.col("stop_lon"),
                    F.col("next_stop_lat"), F.col("next_stop_lon")
                ) * cfg.RAIL_DETOUR_FACTOR
            )
        )

        # checkpoint post-Haversine (tronque le lineage + matérialise)
        # eager=False : pipeline linéaire, matérialisation au prochain job
        df_botn = df_botn.checkpoint(eager=False)
        self.logger.debug("BOTN: checkpoint post-Haversine posé pour stabiliser les étapes suivantes.")

        # filtre segments à distance nulle ou invalide
        self.logger.debug("BOTN: exclusion des trips comportant des segments nuls/incohérents.")
        bad_botn_segments = (
            df_botn
            .filter(
                F.col("next_stop_lat").isNotNull()
                & F.col("next_stop_lon").isNotNull()
                & F.col("segment_dist_m").isNotNull()
                & (F.isnan("segment_dist_m") | (F.col("segment_dist_m") <= 0.0))
            )
            .select("trip_id")
            .distinct()
        )
        df_botn = df_botn.join(F.broadcast(bad_botn_segments), ["trip_id"], "left_anti")

        self.logger.info("BOTN - étape 6/6: alignement final des types avec le schéma MDB.")
        self.logger.debug("BOTN: alignement final des types string pour union propre avec MDB.")
        df_botn = self._align_types_with_mdb(df_botn)

        self.logger.info("BOTN terminé: dataset normalisé prêt pour la phase de merge.")
        return {"df_botn": df_botn}

    # -----------------------------------------------------------------
    # Méthodes privées
    # -----------------------------------------------------------------

    def _renumber_stop_sequences(self, df: DataFrame) -> DataFrame:
        """
        Dédoublonne et renuméro les stop_sequence pour les trajets en Y

        Parameters
        ----------
        df : DataFrame
            DataFrame BOTN avec colonnes trip_id, stop_id, stop_sequence

        Returns
        -------
        DataFrame
            DataFrame avec stop_sequence renumérotés pour les trajets problématiques
        """
        # dédoublonnage explicite par (trip_id, stop_id, stop_sequence)
        df = df.dropDuplicates(["trip_id", "stop_id", "stop_sequence"])

        # trips problématiques : même stop_sequence pour plusieurs stop_id
        # (agrégation SQL valide, sans fenêtre dans un groupBy)
        dup_trip_ids = (
            df.groupBy("trip_id", "stop_sequence")
            .agg(F.count("stop_id").alias("nb_stops"))
            .filter(F.col("nb_stops") > 1)
            .select("trip_id")
            .distinct()
            .withColumn("needs_renumber", F.lit(True))
        )

        # renumérotation par horaire pour les trips en Y
        w_renum = Window.partitionBy("trip_id").orderBy(
            F.coalesce("departure_time", "arrival_time"),
            F.col("stop_sequence"),
            F.col("stop_id"),
        )
        df = (
            df
            .join(F.broadcast(dup_trip_ids), ["trip_id"], "left")
            .withColumn(
                "stop_sequence_new",
                F.when(
                    F.col("needs_renumber").isNotNull(),
                    F.row_number().over(w_renum) - 1
                ).otherwise(F.col("stop_sequence"))
            )
            .drop("needs_renumber")
            .drop("stop_sequence")
            .withColumnRenamed("stop_sequence_new", "stop_sequence")
        )

        return df

    def _normalize_dates(self, df: DataFrame) -> DataFrame:
        """
        Normalise les dates start_date/end_date au format YYYYMMDD

        Parameters
        ----------
        df : DataFrame
            DataFrame avec colonnes start_date et end_date

        Returns
        -------
        DataFrame
            DataFrame avec dates normalisées
        """
        for col_date in ["start_date", "end_date"]:
            df = df.withColumn(
                col_date,
                F.when(
                    F.col(col_date).rlike(r"^\d{4}-\d{2}-\d{2}"),
                    F.regexp_replace(F.substring(F.col(col_date), 1, 10), "-", "")
                ).otherwise(F.col(col_date))
            )
        return df

    def _resolve_composite_agency_names(
        self,
        df: DataFrame,
        df_agencies: DataFrame,
        df_trips: DataFrame
    ) -> DataFrame:
        """
        Résout les agency_id composites (ex: 'CFM/CFR') en noms concaténés

        Parameters
        ----------
        df : DataFrame
            DataFrame BOTN avec colonnes agency_id, agency_name
        df_agencies : DataFrame
            Table agencies BOTN avec colonnes agency_id, agency_name
        df_trips : DataFrame
            Table trips BOTN pour récupérer les agency_id distincts

        Returns
        -------
        DataFrame
            DataFrame avec agency_name résolus pour les IDs composites
        """
        botn_ag_dict = {
            r[0]: r[1]
            for r in df_agencies.select("agency_id", "agency_name").collect()
            if r[0]
        }
        botn_aids = [
            r[0]
            for r in df_trips.select("agency_id").distinct().collect()
            if r[0]
        ]

        resolved_dict: dict[str, str] = {}
        for aid in botn_aids:
            if "/" in aid:
                parts = [p for p in aid.split("/") if p]
                resolved_dict[aid] = " / ".join(
                    [botn_ag_dict.get(p, p) for p in parts]
                )
            else:
                resolved_dict[aid] = botn_ag_dict.get(aid, aid)

        # map natif Catalyst
        botn_map_expr = F.create_map(
            [F.lit(x) for k, v in resolved_dict.items() for x in (k, v)]
        )
        return df.withColumn(
            "agency_name",
            F.coalesce(F.col("agency_name"), botn_map_expr[F.col("agency_id")])
        )

    def _align_types_with_mdb(self, df: DataFrame) -> DataFrame:
        """
        Cast toutes les colonnes string pour alignement avec le schéma MDB

        Parameters
        ----------
        df : DataFrame
            DataFrame BOTN avec types mixtes

        Returns
        -------
        DataFrame
            DataFrame avec toutes les colonnes textuelles en string
        """
        return df.select(*[
            F.col(c).cast("string").alias(c) if c in self.config.BOTN_STRING_COLS else F.col(c)
            for c in df.columns
        ])
