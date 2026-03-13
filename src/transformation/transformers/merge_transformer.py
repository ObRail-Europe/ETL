"""
Transformateur de merge, geo-bucketing et construction du schéma en étoile

Fusionne MDB + BOTN (matching L1/L2), enrichit les villes via GeoNames,
construit les tables localite et emission, et sauvegarde les 3 Parquet
"""

from typing import Any

import pycountry
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StructType, StructField, IntegerType
import pyspark.sql.functions as F

from transformation.transformers.base_transformer import BaseTransformer
from transformation.utils.spark_functions import (
    haversine_dist,
    replace_blank_with_nulls,
    apply_tz_mapping,
)


class MergeTransformer(BaseTransformer):

    def get_transformer_name(self) -> str:
        return "merge"

    def transform(self, **kwargs: Any) -> dict[str, DataFrame]:
        """
        Parameters
        ----------
        df_mdb : DataFrame
            DataFrame MDB normalisé (produit par MobilityDatabaseTransformer)
        df_botn : DataFrame
            DataFrame BOTN normalisé (produit par BackOnTrackTransformer)
        df_ember : DataFrame
            DataFrame Ember avec intensité carbone (produit par EmberTransformer)
        df_ademe : DataFrame
            DataFrame ADEME avec émissions aériennes (produit par AdemeTransformer)
        df_airports : DataFrame
            DataFrame aéroports enrichis avec villes (produit par OurAirportsTransformer)

        Returns
        -------
        dict[str, DataFrame]
            {'df_trip': DataFrame, 'df_localite': DataFrame, 'df_emission': DataFrame}
        """
        df_mdb: DataFrame = kwargs["df_mdb"]
        df_botn: DataFrame = kwargs["df_botn"]
        df_ember: DataFrame = kwargs["df_ember"]
        df_ademe: DataFrame = kwargs["df_ademe"]
        df_airports: DataFrame = kwargs["df_airports"]
        cfg = self.config

        self.logger.info("Fusion finale et bucketing géographique...")

        # alignement des colonnes communes
        common_cols = list(set(df_mdb.columns).intersection(set(df_botn.columns)))
        self.logger.debug(f"Colonnes communes MDB/BOTN: {len(common_cols)}")

        # checkpoint : tronque le lineage MDB/BOTN (plan Catalyst trop large sinon)
        # eager=False : pipeline linéaire juste après, évite une action supplémentaire immédiate
        df_mdb_aligned = df_mdb.select(*common_cols).checkpoint(eager=False)
        df_botn_aligned = df_botn.select(*common_cols).checkpoint(eager=False)
        self.logger.debug("Checkpoint MDB/BOTN matérialisé")

        # matching L1 & L2
        df_merged = self._match_botn_mdb(df_mdb_aligned, df_botn_aligned)

        # geo-bucketing GeoNames
        df_final = self._geo_bucket_enrichment(df_merged)

        # correction timezone post-geo
        df_final = apply_tz_mapping(df_final, self.config.TZ_MAPPING)

        # checkpoint df_final post-geo (tronque lineage, lu 3 fois)
        # eager=True : dataset réutilisé dans plusieurs branches
        df_final = df_final.checkpoint(eager=True)
        self.logger.debug("Checkpoint df_final post-geo matérialisé")

        # construction table localite (trips + aéroports) 
        df_localite = self._build_localite_table(df_final, df_airports)

        # checkpoint localite (tronque le lineage)
        # eager=True : réutilisé pour emission + remplacement de FK trip
        df_localite = df_localite.checkpoint(eager=True)
        self.logger.debug("Checkpoint localite matérialisé")

        # construction table emission
        df_emission = self._build_emission_table(df_localite, df_ember, df_ademe)

        # checkpoint emission (tronque le lineage)
        # eager=False : consommation linéaire immédiate ensuite
        df_emission = df_emission.checkpoint(eager=False)
        self.logger.debug("Checkpoint emission matérialisé")

        # remplacement FK dans trip
        df_final = self._replace_fk_in_trips(df_final, df_localite, df_emission)

        # nettoyage final
        df_localite = replace_blank_with_nulls(df_localite)
        df_emission = replace_blank_with_nulls(df_emission)
        df_final = replace_blank_with_nulls(df_final)

        # sauvegarde Parquet
        self._save_parquet(df_final, df_localite, df_emission)

        return {
            "df_trip": df_final,
            "df_localite": df_localite,
            "df_emission": df_emission,
        }

    # -----------------------------------------------------------------
    # Méthodes privées
    # -----------------------------------------------------------------

    def _match_botn_mdb(
        self,
        df_mdb: DataFrame,
        df_botn: DataFrame
    ) -> DataFrame:
        """
        Match BOTN avec MDB à 2 niveaux, conserve les valeurs BOTN pour les matchés

        Niveau 1 : match exact sur trip_short_name.
        Niveau 2 : numéro de train (≥3 chiffres) + premier token agency_name

        Les trips MDB matchés sont retirés et remplacés par leur version BOTN
        (qui contient city, country, is_night_train)
        Les trips BOTN non matchés sont ajoutés tels quels

        Parameters
        ----------
        df_mdb : DataFrame
            DataFrame MDB aligné
        df_botn : DataFrame
            DataFrame BOTN aligné

        Returns
        -------
        DataFrame
            DataFrame fusionné (MDB non matchés + ALL BOTN)
        """
        self.logger.debug("Matching L1: trip_short_name exact...")

        # identifier les trips MDB dont le trip_short_name existe dans BOTN
        botn_tsn = (
            df_botn
            .select("trip_short_name")
            .filter(F.col("trip_short_name").isNotNull())
            .distinct()
        )
        mdb_matched_l1 = (
            df_mdb
            .join(F.broadcast(botn_tsn), "trip_short_name", "inner")
            .select("source", "trip_id")
            .distinct()
        )

        self.logger.debug("Matching L2: num + agency token...")

        # numéro de train + premier token agency_name
        def add_num_agency_cols(df: DataFrame) -> DataFrame:
            return (
                df
                .withColumn(
                    "num_token",
                    F.regexp_extract(F.col("trip_short_name"), r"(\d{3,6})", 1)
                )
                .withColumn(
                    "agency_token",
                    F.when(
                        F.col("agency_name").isNotNull(),
                        F.split(F.col("agency_name"), " ").getItem(0)
                    ).otherwise(F.lit(None))
                )
            )

        botn_tokens = (
            add_num_agency_cols(df_botn)
            .select("num_token", "agency_token")
            .distinct()
        )
        mdb_matched_l2 = (
            add_num_agency_cols(df_mdb)
            .join(F.broadcast(botn_tokens), ["num_token", "agency_token"], "inner")
            .select("source", "trip_id")
            .distinct()
        )

        # tous les trips MDB matchés (L1 + L2)
        all_matched_mdb = mdb_matched_l1.union(mdb_matched_l2).distinct()

        # retrait des trips MDB matchés (remplacés par leur version BOTN)
        df_mdb_unmatched = df_mdb.join(
            F.broadcast(all_matched_mdb), ["source", "trip_id"], "left_anti"
        )

        self.logger.debug("Fusion MDB non matchés + ALL BOTN (valeurs BOTN conservées)...")
        return df_mdb_unmatched.unionByName(df_botn, allowMissingColumns=True)

    def _geo_bucket_enrichment(self, df: DataFrame) -> DataFrame:
        """
        Enrichit les arrêts sans ville via geo-bucketing GeoNames

        Indexe gares et villes par carrés de 0.5°, cherche dans les 9 buckets
        adjacents et sélectionne la ville la plus proche (< 15 km) et la plus peuplée

        Parameters
        ----------
        df : DataFrame
            DataFrame fusionné avec colonnes city, country, stop_lat, stop_lon

        Returns
        -------
        DataFrame
            DataFrame enrichi avec city et country remplis
        """
        cfg = self.config

        self.logger.debug("Chargement GeoNames pour geo-bucketing...")
        df_geonames = (
            self.spark.read.parquet(str(cfg.GEONAMES_RAW_PATH / "cities1000.parquet"))
            .withColumn("lat_bucket", F.floor(F.col("latitude") / cfg.GEO_BUCKET_SIZE))
            .withColumn("lon_bucket", F.floor(F.col("longitude") / cfg.GEO_BUCKET_SIZE))
        )

        # arrêts à enrichir (city null)
        df_stops_to_match = (
            df
            .filter(F.col("city").isNull())
            .select("stop_id", "stop_lat", "stop_lon")
            .distinct()
        )
        self.logger.debug("Génération des 9 buckets adjacents par arrêt...")

        # 9 buckets adjacents (incluant les diagonales)
        offsets = F.array(*[
            F.struct(F.lit(dlat).alias("dlat"), F.lit(dlon).alias("dlon"))
            for dlat in [-1, 0, 1]
            for dlon in [-1, 0, 1]
        ])
        df_stops_buckets = (
            df_stops_to_match
            .withColumn("offset", F.explode(offsets))
            .withColumn(
                "search_lat_b",
                F.floor(F.col("stop_lat") / cfg.GEO_BUCKET_SIZE) + F.col("offset.dlat")
            )
            .withColumn(
                "search_lon_b",
                F.floor(F.col("stop_lon") / cfg.GEO_BUCKET_SIZE) + F.col("offset.dlon")
            )
        )

        # jointure par bucket (broadcast geonames ~130k rows)
        self.logger.debug("Jointure geo-bucketing gares × villes...")
        df_geo_matched = df_stops_buckets.join(
            F.broadcast(df_geonames),
            (df_stops_buckets.search_lat_b == df_geonames.lat_bucket)
            & (df_stops_buckets.search_lon_b == df_geonames.lon_bucket)
        )

        # distance Haversine et filtre < 15 km
        df_geo_matched = (
            df_geo_matched
            .withColumn(
                "dist_m",
                haversine_dist(
                    F.col("stop_lat"), F.col("stop_lon"),
                    F.col("latitude"), F.col("longitude")
                )
            )
            .filter(F.col("dist_m") < cfg.GEO_MAX_DIST_M)
        )

        # ville la plus proche, puis la plus peuplée en cas d'égalité
        w_closest = Window.partitionBy("stop_id").orderBy(
            F.asc("dist_m"), F.desc("population")
        )
        df_closest_city = (
            df_geo_matched
            .withColumn("rn", F.row_number().over(w_closest))
            .filter(F.col("rn") == 1)
            .select(
                "stop_id",
                F.col("asciiname").alias("mapped_city"),
                F.col("country_code").alias("mapped_country")
            )
        )

        self.logger.debug("Application des villes mappées aux arrêts...")
        return (
            df
            .join(df_closest_city, "stop_id", "left")
            .withColumn("city", F.coalesce("city", "mapped_city"))
            .withColumn("country", F.coalesce("country", "mapped_country"))
            .drop("mapped_city", "mapped_country")
        )

    def _build_localite_table(
        self,
        df: DataFrame,
        df_airports: DataFrame,
    ) -> DataFrame:
        """
        Construit la table de dimension localite (city_id, city_name, country_code, country_name)
        Inclut les villes des gares (trips) ET les villes des aéroports
        
        Parameters
        ----------
        df : DataFrame
            DataFrame enrichi avec colonnes city et country
        df_airports : DataFrame
            DataFrame aéroports enrichis avec colonnes city et country_geo

        Returns
        -------
        DataFrame
            Table localite avec clé primaire city_id
        """
        self.logger.debug("Construction de la table localite (trips + aéroports)...")

        # villes des trips
        df_trip_cities = (
            df
            .select(
                F.col("city").alias("city_name"),
                F.col("country").alias("country_code"),
            )
            .distinct()
            .dropna()
        )

        # villes des aéroports
        df_airport_cities = (
            df_airports
            .select(
                F.col("city").alias("city_name"),
                F.col("country_geo").alias("country_code"),
            )
            .distinct()
            .dropna()
        )

        # union et dédupl
        w_city_id = Window.orderBy("country_code", "city_name")
        df_localite = (
            df_trip_cities
            .unionByName(df_airport_cities)
            .distinct()
            .filter(F.length(F.col("country_code")) == 2)
            .withColumn("city_id", F.row_number().over(w_city_id))
        )

        # mapping alpha-2 → nom de pays complet
        a2_name_dict = {
            c.alpha_2: c.name
            for c in pycountry.countries
            if hasattr(c, "alpha_2")
        }
        a2_name_map = F.create_map(
            [F.lit(x) for k, v in a2_name_dict.items() for x in (k, v)]
        )
        df_localite = df_localite.withColumn(
            "country_name", a2_name_map[F.col("country_code")]
        )

        self.logger.debug("Table localite construite.")
        return df_localite

    def _build_emission_table(
        self,
        df_localite: DataFrame,
        df_ember: DataFrame,
        df_ademe: DataFrame,
    ) -> DataFrame:
        """
        Construit la table de dimension emission unifiée (rail + aérien)

        Calcule les émissions ferroviaires (pays × route_type), projette sur
        le schéma unifié (emission_ref, emission_gCO2e_per_p_km, detail), puis
        unionne avec les émissions aériennes ADEME

        Parameters
        ----------
        df_localite : DataFrame
            Table localite avec colonne country_code
        df_ember : DataFrame
            DataFrame Ember avec colonnes country_code, emissions_intensity_gco2_per_kwh
        df_ademe : DataFrame
            DataFrame ADEME avec colonnes emission_ref, emission_gCO2e_per_p_km, detail

        Returns
        -------
        DataFrame
            Table emission unifiée avec colonnes emission_ref, emission_gCO2e_per_p_km,
            detail, et colonnes intermédiaires country_code/route_type pour la jointure trip
        """
        cfg = self.config

        self.logger.debug("Construction de la table emission...")

        # produit cartésien pays × route_type
        route_types_schema = StructType([
            StructField("route_type", IntegerType(), False)
        ])
        route_types_df = self.spark.createDataFrame(
            [(rt,) for rt in cfg.EMISSION_ROUTE_TYPES],
            schema=route_types_schema
        )
        countries_df = df_localite.select("country_code").distinct()
        df_emission = countries_df.crossJoin(route_types_df)

        # jointure avec Ember pour l'intensité carbone
        df_ember_int = df_ember.select(
            "country_code",
            F.col("emissions_intensity_gco2_per_kwh").alias("carbon_intensity_gco2_kwh")
        )
        ademe_max_ref_row = df_ademe.select(
            F.max(F.expr("try_cast(emission_ref as bigint)"))
            .alias("max_emission_ref")
        ).collect()[0]
        ademe_max_ref = ademe_max_ref_row["max_emission_ref"]
        ademe_max_ref = int(ademe_max_ref) if ademe_max_ref is not None else 0
        result = df_ember_int.agg(
            F.avg("carbon_intensity_gco2_kwh").alias("avg_val"),
            F.count("*").alias("cnt")
        ).collect()[0]
        mean_intensity = result["avg_val"] if result["cnt"] > 0 else 60.0

        df_emission = (
            df_emission
            .join(F.broadcast(df_ember_int), "country_code", "left")
            .withColumn(
                "carbon_intensity_gco2_kwh",
                F.coalesce(F.col("carbon_intensity_gco2_kwh"), F.lit(mean_intensity))
            )
            .withColumn("route_type", F.col("route_type").cast("int"))
        )

        # maps catalyst pour consommation et élec
        cons_map = F.create_map(
            [F.lit(x) for k, v in cfg.CONSUMPTION_MAP.items() for x in (k, v)]
        )
        mean_elec = sum(cfg.ELECTRIFICATION.values()) / len(cfg.ELECTRIFICATION)
        elec_map = F.create_map(
            [F.lit(x) for k, v in cfg.ELECTRIFICATION.items() for x in (k, v)]
        )

        df_emission = (
            df_emission
            .withColumn(
                "energy_consumption_kwh_pkm",
                F.coalesce(cons_map[F.col("route_type")], F.lit(0.05))
            )
            .withColumn(
                "electrification_rate",
                F.coalesce(elec_map[F.col("country_code")], F.lit(mean_elec))
            )
        )

        # calcul CO₂ par pkm
        diesel_fixed = cfg.DIESEL_FIXED_GCO2_PKM
        df_emission = (
            df_emission
            .withColumn(
                "co2_per_pkm",
                F.when(
                    F.col("route_type").isin(100, 101, 102, 103, 105),
                    F.col("energy_consumption_kwh_pkm")
                    * F.col("carbon_intensity_gco2_kwh")
                )
                .otherwise(
                    (F.col("electrification_rate")
                     * F.col("energy_consumption_kwh_pkm")
                     * F.col("carbon_intensity_gco2_kwh"))
                    + ((1 - F.col("electrification_rate")) * F.lit(diesel_fixed))
                )
            )
        )

        # schéma unifié : emission_ref, emission_gCO2e_per_p_km, detail
        w_emission_ref = Window.orderBy("country_code", "route_type")
        df_emission = df_emission.withColumn(
            "emission_ref",
            F.row_number().over(w_emission_ref) + F.lit(ademe_max_ref)
        )
        df_emission = df_emission.withColumn(
            "emission_gCO2e_per_p_km", F.round(F.col("co2_per_pkm"), 3)
        )
        df_emission = df_emission.withColumn(
            "detail",
            F.concat(
                F.lit("Rail RT"),
                F.col("route_type").cast("string"),
                F.lit(", "),
                F.col("country_code"),
                F.lit(", "),
                F.round(F.col("energy_consumption_kwh_pkm"), 4).cast("string"),
                F.lit(" kWh/pkm × "),
                F.round(F.col("carbon_intensity_gco2_kwh"), 2).cast("string"),
                F.lit(" gCO2/kWh, elec="),
                F.round(F.col("electrification_rate") * 100, 1).cast("string"),
                F.lit("%, diesel="),
                F.round(
                    (1 - F.col("electrification_rate")) * F.lit(diesel_fixed), 2
                ).cast("string"),
                F.lit(" gCO2/pkm"),
            )
        )

        #union avec les émissions aériennes ADEME
        self.logger.debug("Union des émissions ferroviaires et aériennes ADEME...")
        df_emission = df_emission.unionByName(df_ademe, allowMissingColumns=True)

        self.logger.debug("Table emission construite.")
        return df_emission

    def _replace_fk_in_trips(
        self,
        df: DataFrame,
        df_localite: DataFrame,
        df_emission: DataFrame
    ) -> DataFrame:
        """
        Remplace city/country par les clés étrangères city_id et emission_ref

        Parameters
        ----------
        df : DataFrame
            DataFrame trip enrichi avec colonnes city, country
        df_localite : DataFrame
            Table localite avec colonnes city_name, country_code, city_id
        df_emission : DataFrame
            Table emission avec colonnes country_code, route_type pour la jointure rail

        Returns
        -------
        DataFrame
            DataFrame trip avec FK city_id et emission_ref
        """
        self.logger.debug("Remplacement des FK dans trip...")

        return (
            df
            .join(
                F.broadcast(df_localite),
                (df.city == df_localite.city_name)
                & (df.country == df_localite.country_code),
                "left"
            )
            .join(
                F.broadcast(df_emission),
                ["country_code", "route_type"],
                "left"
            )
            .drop(
                "city", "country", "city_name", "country_code",
                "co2_per_pkm", "energy_consumption_kwh_pkm",
                "carbon_intensity_gco2_kwh", "electrification_rate",
                "emission_id", "emission_gCO2e_per_p_km", "detail",
            )
        )

    def _save_parquet(
        self,
        df_trip: DataFrame,
        df_localite: DataFrame,
        df_emission: DataFrame
    ) -> None:
        """
        Sauvegarde les 3 tables au format Parquet

        Parameters
        ----------
        df_trip : DataFrame
            Table de faits trip
        df_localite : DataFrame
            Table de dimension localite
        df_emission : DataFrame
            Table de dimension emission
        """
        cfg = self.config

        self.logger.info("Lancement de la sauvegarde Parquet...")

        df_localite.coalesce(cfg.DIM_COALESCE_PARTITIONS).write.mode("overwrite").parquet(
            str(cfg.LOCALITE_OUTPUT_PATH)
        )
        self.logger.debug(f"Table localite sauvegardée: {cfg.LOCALITE_OUTPUT_PATH}")

        #sauvegarder uniquement les colonnes du schéma unifié
        df_emission.select(
            "emission_ref", "emission_gCO2e_per_p_km", "detail"
        ).coalesce(cfg.DIM_COALESCE_PARTITIONS).write.mode("overwrite").parquet(
            str(cfg.EMISSION_OUTPUT_PATH)
        )
        self.logger.debug(f"Table emission sauvegardée: {cfg.EMISSION_OUTPUT_PATH}")

        df_trip.coalesce(cfg.TRIP_COALESCE_PARTITIONS).write.mode("overwrite").parquet(
            str(cfg.TRAIN_TRIP_OUTPUT_PATH)
        )
        self.logger.debug(f"Table trip sauvegardée: {cfg.TRAIN_TRIP_OUTPUT_PATH}")

        self.logger.info("Sauvegarde terminée !")
