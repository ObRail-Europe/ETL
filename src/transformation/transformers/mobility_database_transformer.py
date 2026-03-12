"""
Transformateur pour les données GTFS Mobility Database (MDB)

Charge et normalise les 7 tables GTFS, les joint, filtre les trainss (par route_type),
calcule les distances Haversine inter-arrêts et reconstruit les calendriers de service
"""

import glob
import os
from typing import Any

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from transformation.transformers.base_transformer import BaseTransformer
from transformation.utils.spark_functions import (
    haversine_dist,
    tree_union,
    replace_blank_with_nulls,
    invalid_coord_expr,
    apply_tz_mapping,
)


class MobilityDatabaseTransformer(BaseTransformer):

    def get_transformer_name(self) -> str:
        return "mobility_database"

    def transform(self, **kwargs: Any) -> dict[str, DataFrame]:
        """
        Returns
        -------
        dict[str, DataFrame]
            {'df_mdb': DataFrame} — DataFrame MDB normalisé et enrichi

        Raises
        ------
        RuntimeError
            Si aucun fichier GTFS est trouvé
        """
        cfg = self.config
        self.logger.info("Début du traitement Mobility Database (GTFS)...")

        # chargement et normalisation des 7 tables GTFS
        df_routes = self._load_and_normalize_gtfs("routes", cfg.GTFS_COLS_ROUTES)
        df_trips = self._load_and_normalize_gtfs("trips", cfg.GTFS_COLS_TRIPS)
        df_stop_times = self._load_and_normalize_gtfs("stop_times", cfg.GTFS_COLS_STOP_TIMES)
        df_stops = self._load_and_normalize_gtfs("stops", cfg.GTFS_COLS_STOPS)
        df_agency = self._load_and_normalize_gtfs("agency", cfg.GTFS_COLS_AGENCY)
        df_calendar = self._load_and_normalize_gtfs("calendar", cfg.GTFS_COLS_CALENDAR)
        df_calendar_dates = self._load_and_normalize_gtfs(
            "calendar_dates", cfg.GTFS_COLS_CALENDAR_DATES
        )

        # vérification des tables critiques
        critical_tables = {
            "routes": df_routes,
            "trips": df_trips,
            "stop_times": df_stop_times,
            "stops": df_stops,
        }
        missing = [name for name, df in critical_tables.items() if df is None]
        if missing:
            raise RuntimeError(
                f"Tables GTFS critiques manquantes: {', '.join(missing)}. "
                f"Aucun fichier trouvé dans {cfg.MDB_RAW_PATH}"
            )

        # # persistence des tables volumineuses pour éviter recalculs
        # self.logger.debug("Persistence des tables volumineuses (trips, stop_times, stops)...")
        # if df_trips is not None:
        #     df_trips = df_trips.persist()
        # if df_stop_times is not None:
        #     df_stop_times = df_stop_times.persist()
        # if df_stops is not None:
        #     df_stops = df_stops.persist()

        self.logger.info("Début des jointures composites MDB...")

        # jointures composites (broadcast des petites tables)
        df_mdb = (
            df_trips
            .join(F.broadcast(df_routes), on=["source", "route_id"], how="left")
            .join(df_stop_times, on=["source", "trip_id"], how="left")
            .join(df_stops, on=["source", "stop_id"], how="left")
            .join(F.broadcast(df_agency), on=["source", "agency_id"], how="left")
            .join(F.broadcast(df_calendar), on=["source", "service_id"], how="left")
        )

        # # checkpoint PRÉCOCE après jointures pour tronquer le lineage 
        # self.logger.debug("Checkpoint précoce post-jointures pour tronquer le lineage...")
        # df_mdb = df_mdb.checkpoint(eager=False)

        # # libération mémoire des tables sources (plus nécessaires)
        # self.logger.debug("Libération mémoire des tables sources...")
        # if df_trips is not None:
        #     df_trips.unpersist()
        # if df_stop_times is not None:
        #     df_stop_times.unpersist()
        # if df_stops is not None:
        #     df_stops.unpersist()

        # casts post-jointure
        self.logger.debug("Casts post-jointure (stop_lat, stop_lon, stop_sequence, route_type)...")
        df_mdb = (
            df_mdb
            .withColumn("stop_lat", F.col("stop_lat").cast("double"))
            .withColumn("stop_lon", F.col("stop_lon").cast("double"))
            .withColumn("stop_sequence", F.expr("try_cast(stop_sequence as int)"))
            .withColumn("route_type", F.expr("try_cast(route_type as int)"))
        )

        # nettoyage global des cellules vides → NULL
        self.logger.debug("Nettoyage des cellules vides → NULL...")
        df_mdb = replace_blank_with_nulls(df_mdb)

        # filtre stop_sequence nul
        self.logger.debug("Filtre des stop_sequence nuls...")
        df_mdb = df_mdb.filter(F.col("stop_sequence").isNotNull())

        # filtre route_type & dedoublonnage
        self.logger.debug("Filtre route_type ferroviaire et dedoublonnage sémantique...")
        df_mdb = (
            df_mdb
            .filter(F.col("route_type").isin(cfg.VALID_ROUTE_TYPES))
            .dropDuplicates(["source", "agency_id", "trip_id", "stop_sequence", "stop_id"])
        )

        # checkpoint post-jointures (tronque le lineage + matérialise)
        # eager=False : chaîne de transformations linéaire, pas de branche immédiate
        df_mdb = df_mdb.checkpoint(eager=False)
        self.logger.debug("Checkpoint MDB post-jointures matérialisé")

        # # persistence avant enrichissements lourds
        # self.logger.debug("Persistence du DataFrame filtré avant enrichissements...")
        # df_mdb = df_mdb.persist()

        # enrichissement route_type 2 (générique)
        self.logger.debug("Enrichissement route_type 2 (générique → spécifique)...")
        df_mdb = self._enrich_route_type(df_mdb)

        # filtre coordonnées invalides (Null Island)
        self.logger.debug("Filtre des coordonnées invalides (Null Island)...")
        bad_trips = (
            df_mdb
            .filter(invalid_coord_expr("stop_lat", "stop_lon"))
            .select("source", "trip_id")
            .distinct()
        )
        df_mdb = df_mdb.join(F.broadcast(bad_trips), ["source", "trip_id"], "left_anti")

        # filtre trips avec un seul arrêt
        self.logger.debug("Filtre des trips à un seul arrêt...")
        # Repartition AVANT window function pour éviter shuffle vers single partition
        df_mdb = df_mdb.repartition("source", "trip_id")
        w_trip = Window.partitionBy("source", "trip_id")
        df_mdb = (
            df_mdb
            .withColumn("_nb_stops", F.count("*").over(w_trip))
            .filter(F.col("_nb_stops") > 1)
            .drop("_nb_stops")
        )

        # normalisation des champs texte et extraction du pays
        self.logger.debug("Normalisation Title Case et extraction du code pays...")
        df_mdb = (
            df_mdb
            .withColumn("arrival_time", F.substring_index(F.col("arrival_time"), " ", -1))
            .withColumn("departure_time", F.substring_index(F.col("departure_time"), " ", -1))
            .withColumn("agency_name", F.initcap(F.col("agency_name")))
            .withColumn("stop_name", F.initcap(F.col("stop_name")))
            .withColumn("_country", F.split(F.col("source"), "/").getItem(0))
        )

        # correction des fuseaux horaires via TZ_MAPPING
        self.logger.debug("Correction des fuseaux horaires CET/UTC → IANA...")
        df_mdb = apply_tz_mapping(df_mdb, self.config.TZ_MAPPING, country_col="_country")

        # Checkpoint avant les opérations de fenêtrage coûteuses
        # Cela tronque la dépendance et libère l'optimiseur Catalyst
        self.logger.debug("Checkpoint pré-enrichissement pour tronquer le lineage...")
        df_mdb = df_mdb.checkpoint(eager=False)

        # propagation d'agence par route
        self.logger.debug("Propagation des agences par route_id...")
        df_mdb = self._propagate_agency(df_mdb)

        # mapping manuel des agences
        self.logger.debug("Application du mapping manuel des agences...")
        df_mdb = self._apply_manual_agency_mapping(df_mdb)

        # calcul Haversine inter-arrêts
        self.logger.debug("Calcul des distances Haversine inter-arrêts...")
        df_mdb = self._compute_segment_distances(df_mdb, partition_cols=["source", "trip_id"])

        # checkpoint post-Haversine (tronque le lineage + matérialise)
        # eager=False : pas de réutilisation multi-branches à ce stade
        df_mdb = df_mdb.checkpoint(eager=False)
        self.logger.debug("Checkpoint MDB post-Haversine matérialisé")

        # # checkpoint post-Haversine (tronque le lineage + matérialise)
        # self.logger.debug("Checkpoint post-Haversine + libération mémoire précédente...")
        # df_mdb_old = df_mdb
        # df_mdb = df_mdb.checkpoint(eager=False)
        # df_mdb_old.unpersist()  # libère le DataFrame persisté précédemment

        #filtre des segments à distance nulle ou invalide
        self.logger.debug("Filtre des segments à distance nulle ou invalide...")
        bad_zero_segments = (
            df_mdb
            .filter(
                F.col("next_stop_lat").isNotNull()
                & F.col("next_stop_lon").isNotNull()
                & F.col("segment_dist_m").isNotNull()
                & (F.isnan("segment_dist_m") | (F.col("segment_dist_m") <= 0.0))
            )
            .select("source", "trip_id")
            .distinct()
        )
        df_mdb = df_mdb.join(F.broadcast(bad_zero_segments), ["source", "trip_id"], "left_anti")

        # reconstruction du calendrier via calendar_dates
        if df_calendar_dates is not None:
            self.logger.debug("Reconstruction des calendriers via calendar_dates...")
            df_mdb = self._reconstruct_calendar(df_mdb, df_calendar_dates)

        # filtre end_date avec stratégie de sauvetage par pays
        self.logger.debug("Filtre end_date avec stratégie de sauvetage par pays...")
        df_mdb = self._filter_end_date(df_mdb)

        # masque jours de semaine + colonnes finales
        self.logger.debug("Construction du masque days_of_week et colonnes finales...")
        day_cols = cfg.DAY_COLS
        df_mdb = (
            df_mdb
            .withColumn(
                "days_of_week",
                F.concat(*[F.coalesce(F.col(c).cast("string"), F.lit("0")) for c in day_cols])
            )
            .drop(*day_cols, "next_stop_lat", "next_stop_lon", "end_date_int")
            .withColumn("city", F.lit(None).cast("string"))
            .withColumn("country", F.upper(F.col("_country")))
            .withColumn("is_night_train", F.col("route_type") == 105)
        )

        self.logger.info("Phase Mobility Database terminée.")
        return {"df_mdb": df_mdb}

    # -----------------------------------------------------------------
    # Méthodes privées
    # -----------------------------------------------------------------

    def _load_and_normalize_gtfs(
        self,
        table_name: str,
        expected_columns: list[str]
    ) -> DataFrame | None:
        """
        Charge tous les fichiers parquet d'une table GTFS et les aligne sur le schéma cible

        Parameters
        ----------
        table_name : str
            Nom de la table GTFS (ex: 'routes', 'trips')
        expected_columns : list[str]
            colonnes attendues après normalisation

        Returns
        -------
        DataFrame ou None
            DataFrame unifié, ou None si aucun fichier trouvé
        """
        self.logger.debug(f"Lecture et alignement natif de '{table_name}'...")
        file_paths = glob.glob(
            str(self.config.MDB_RAW_PATH / "*" / "*" / table_name)
        )

        if not file_paths:
            self.logger.warning(f"Aucun fichier trouvé pour {table_name}")
            return None

        unified_dfs: list[DataFrame] = []
        for path in file_paths:
            parts = path.split(os.sep)
            source_name = f"{parts[-3]}/{parts[-2]}"

            df_temp = self.spark.read.parquet(path)

            # projection immédiate avec nettoyage direct
            exprs: list[F.Column] = [F.lit(source_name).alias("source")]
            
            # colonnes normales (avec lowercase)
            normal_cols = [c for c in expected_columns if c not in ("source", "agency_timezone")]
            for c in normal_cols:
                if c in df_temp.columns:
                    exprs.append(
                        F.lower(F.trim(F.regexp_replace(F.col(c).cast("string"), '["\']', ''))).alias(c)
                    )
                else:
                    exprs.append(F.lit(None).cast("string").alias(c))
            
            # agency_timezone sans lowercase (format IANA)
            if "agency_timezone" in expected_columns:
                if "agency_timezone" in df_temp.columns:
                    exprs.append(
                        F.trim(F.regexp_replace(F.col("agency_timezone").cast("string"), '["\']', '')).alias("agency_timezone")
                    )
                else:
                    exprs.append(F.lit(None).cast("string").alias("agency_timezone"))

            unified_dfs.append(df_temp.select(*exprs))

        df_merged = tree_union(unified_dfs)
        self.logger.debug(f"Table '{table_name}' chargée et normalisée.")
        return df_merged

    def _enrich_route_type(self, df: DataFrame) -> DataFrame:
        """
        remplace le route_type générique 2 par un type spécifique quand disponible

        Parameters
        ----------
        df : DataFrame
            DataFrame MDB avec colonne route_type

        Returns
        -------
        DataFrame
            DataFrame avec route_type enrichi
        """
        # Repartition pour l'agrégation par route
        df_route_types = df.select("source", "route_id", "route_type").distinct()
        df_route_types = df_route_types.repartition("source", "route_id")
        
        w_rt = Window.partitionBy("source", "route_id").orderBy(
            F.when(F.col("route_type") != 2, 0).otherwise(1),
            "route_type"
        )
        df_route_enrich = (
            df_route_types
            .withColumn("rn", F.row_number().over(w_rt))
            .filter(F.col("rn") == 1)
            .select("source", "route_id", F.col("route_type").alias("enriched_route_type"))
        )
        return (
            df.join(F.broadcast(df_route_enrich), ["source", "route_id"], "left")
            .withColumn(
                "route_type",
                F.when(F.col("route_type") == 2, F.col("enriched_route_type"))
                 .otherwise(F.col("route_type"))
            )
            .drop("enriched_route_type")
        )

    def _propagate_agency(self, df: DataFrame) -> DataFrame:
        """
        Propage agency_id/agency_name/agency_timezone depuis la route vers les trips

        Parameters
        ----------
        df : DataFrame
            DataFrame MDB avec colonnes agency_*

        Returns
        -------
        DataFrame
            DataFrame avec agences propagées via route_id
        """
        df_route_agency = (
            df
            .select("source", "route_id", "agency_id", "agency_name", "agency_timezone")
            .groupBy("source", "route_id")
            .agg(
                F.first("agency_id", ignorenulls=True).alias("_route_agency_id"),
                F.first("agency_name", ignorenulls=True).alias("_route_agency_name"),
                F.first("agency_timezone", ignorenulls=True).alias("_route_agency_tz"),
            )
        )

        return (
            df
            .join(F.broadcast(df_route_agency), ["source", "route_id"], "left")
            .withColumn("agency_id", F.coalesce("agency_id", "_route_agency_id"))
            .withColumn("agency_name", F.coalesce("agency_name", "_route_agency_name"))
            .withColumn("agency_timezone", F.coalesce("agency_timezone", "_route_agency_tz"))
            .drop("_route_agency_id", "_route_agency_name", "_route_agency_tz")
        )

    def _apply_manual_agency_mapping(self, df: DataFrame) -> DataFrame:
        """
        Applique le mapping manuel des agences pour les feeds problématiques

        Parameters
        ----------
        df : DataFrame
            DataFrame MDB avec colonnes source, agency_*

        Returns
        -------
        DataFrame
            DataFrame avec agences corrigées pour les sources manuellement mappées
        """
        cfg = self.config.AGENCY_MANUAL_MAPPING
        id_map = F.create_map(
            [F.lit(x) for k, (aid, _, _) in cfg.items() for x in (k, aid)]
        )
        name_map = F.create_map(
            [F.lit(x) for k, (_, aname, _) in cfg.items() for x in (k, aname)]
        )
        tz_map = F.create_map(
            [F.lit(x) for k, (_, _, atz) in cfg.items() for x in (k, atz)]
        )
        return (
            df
            .withColumn("agency_id", F.coalesce(id_map[F.col("source")], F.col("agency_id")))
            .withColumn("agency_name", F.coalesce(name_map[F.col("source")], F.col("agency_name")))
            .withColumn("agency_timezone", F.coalesce(tz_map[F.col("source")], F.col("agency_timezone")))
        )

    def _compute_segment_distances(
        self,
        df: DataFrame,
        partition_cols: list[str]
    ) -> DataFrame:
        """
        calcule la distance Haversine entre arrêts consécutifs avec facteur de détour
        
        Optimisée avec repartition préalable pour éviter la mise en cache sur un seul nœud.

        Parameters
        ----------
        df : DataFrame
            DataFrame avec stop_lat, stop_lon, stop_sequence
        partition_cols : list[str]
            Colonnes de partitionnement pour la fenêtre (ex: ['source', 'trip_id'])

        Returns
        -------
        DataFrame
            DataFrame avec colonnes next_stop_lat, next_stop_lon, segment_dist_m
        """
        # CRITIQUE: Repartition AVANT window function pour éviter shuffle vers single partition
        # Sans cela, Spark remet toutes les données sur un seul nœud = OutOfMemoryError
        self.logger.debug(f"Repartition sur {partition_cols} pour window function Haversine...")
        df_repartitioned = df.repartition(*partition_cols).cache()
        
        w_seq = Window.partitionBy(*partition_cols).orderBy("stop_sequence")
        result = (
            df_repartitioned
            .withColumn("next_stop_lat", F.lead("stop_lat").over(w_seq))
            .withColumn("next_stop_lon", F.lead("stop_lon").over(w_seq))
            .withColumn(
                "segment_dist_m",
                haversine_dist(
                    F.col("stop_lat"), F.col("stop_lon"),
                    F.col("next_stop_lat"), F.col("next_stop_lon")
                ) * self.config.RAIL_DETOUR_FACTOR
            )
        )
        
        # Matérialiser et libérer le cache
        self.logger.debug("Matérialisation des distances Haversine et libération du cache...")
        # eager=True : force la matérialisation avant unpersist du cache intermédiaire
        result = result.checkpoint(eager=True)
        df_repartitioned.unpersist()
        
        return result

    def _reconstruct_calendar(
        self,
        df_mdb: DataFrame,
        df_calendar_dates: DataFrame
    ) -> DataFrame:
        """
        Reconstruit les calendriers manquant à partir de calendar_dates.

        Parameters
        ----------
        df_mdb : DataFrame
            DataFrame MDB avec colonnes calendar (monday..sunday, start_date, end_date)
        df_calendar_dates : DataFrame
            Table calendar_dates GTFS avec colonnes source, service_id, date, exception_type

        Returns
        -------
        DataFrame
            DataFrame avec calendriers reconstruits via coalesce
        """
        cfg = self.config

        df_cal_dates = (
            df_calendar_dates
            .withColumn("exception_type", F.col("exception_type").cast("int"))
            .filter(F.col("exception_type") == 1)
        )

        # normalisation du format de date → YYYYMMDD
        df_cal_dates = (
            df_cal_dates
            .withColumn(
                "date_parsed",
                F.when(
                    F.col("date").rlike(r"^\d{4}-\d{2}-\d{2}"),
                    F.regexp_replace(F.substring(F.col("date"), 1, 10), "-", "")
                )
                .when(F.col("date").rlike(r"^\d{8}$"), F.col("date"))
                .otherwise(None)
            )
            .dropna(subset=["date_parsed"])
        )

        # extraction du jour de la semaine
        df_cal_dates = df_cal_dates.withColumn(
            "dow", F.dayofweek(F.to_date(F.col("date_parsed"), "yyyyMMdd"))
        )

        # zgrégation par (source, service_id)
        agg_exprs: list[F.Column] = [
            F.min("date_parsed").alias("recon_start_date"),
            F.max("date_parsed").alias("recon_end_date"),
        ]
        for name, val in cfg.DOW_MAP:
            agg_exprs.append(
                F.max(F.when(F.col("dow") == val, F.lit(1)).otherwise(F.lit(0)))
                 .alias(f"recon_{name}")
            )

        df_cal_recon = df_cal_dates.groupBy("source", "service_id").agg(*agg_exprs)

        # jointure et coalesce
        df_mdb = df_mdb.join(df_cal_recon, ["source", "service_id"], "left")

        for name, _ in cfg.DOW_MAP:
            df_mdb = df_mdb.withColumn(
                name, F.coalesce(F.col(name), F.col(f"recon_{name}"))
            )

        df_mdb = (
            df_mdb
            .withColumn("start_date", F.coalesce(F.col("start_date"), F.col("recon_start_date")))
            .withColumn("end_date", F.coalesce(F.col("end_date"), F.col("recon_end_date")))
            .drop("recon_start_date", "recon_end_date", *[f"recon_{n}" for n, _ in cfg.DOW_MAP])
        )

        return df_mdb

    def _filter_end_date(self, df: DataFrame) -> DataFrame:
        """
        Filtre les trips expirés avec stratégie de sauvetage par pays

        Ne filtre que les pays ayant ≥500k trips après filtrage, pour ne pas perdre des pays entiers dont les flux sont anciens

        Parameters
        ----------
        df : DataFrame
            DataFrame MDB avec colonnes end_date et _country

        Returns
        -------
        DataFrame
            DataFrame filtré avec colonne end_date_int ajoutée
        """
        cfg = self.config

        df = df.withColumn(
            "end_date_int",
            F.when(F.col("end_date").rlike(r"^\d{8}$"), F.col("end_date").cast("int"))
             .otherwise(None)
        )

        # comptage par pays avant/après filtrage
        cnt_by_country = df.groupBy("_country").agg(
            F.countDistinct("trip_id").alias("orig_trips")
        )
        filtered = df.filter(
            (F.col("end_date_int").isNull())
            | (F.col("end_date_int") >= F.lit(cfg.END_DATE_THRESHOLD))
        )
        cnt_filtered = filtered.groupBy("_country").agg(
            F.countDistinct("trip_id").alias("filtered_trips")
        )
        cnt_cmp = cnt_by_country.join(cnt_filtered, "_country", "left").fillna(0)

        # filtre que les pays avec assez de trips post-filtrage
        countries_to_filter = [
            r[0] for r in cnt_cmp
            .filter(F.col("filtered_trips") >= cfg.END_DATE_COUNTRY_TRIP_THRESHOLD)
            .select("_country")
            .collect()
        ]

        if countries_to_filter:
            df = df.filter(
                F.when(
                    F.col("_country").isin(countries_to_filter),
                    (F.col("end_date_int").isNull()
                     | (F.col("end_date_int") >= F.lit(cfg.END_DATE_THRESHOLD)))
                )
                .otherwise(F.lit(True))
            )

        return df
