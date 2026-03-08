"""
Transformateur pour les données OurAirports (trajets aériens européens)

Charge les aéroports européens, enrichit avec les villes GeoNames,
génère tous les trajets possibles (7 types de trajet
par taille d'aéroport et distance), calcule les distances Haversine et
associe chaque trajet à un facteur d'émission ADEME via emission_ref
"""

from typing import Any

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from transformation.transformers.base_transformer import BaseTransformer
from transformation.utils.spark_functions import haversine_dist


class OurAirportsTransformer(BaseTransformer):
    """
    Produit df_airports (enrichis avec villes GeoNames) et df_fly_trip
    (~20M trajets avec emission_ref). La sauvegarde flight.parquet est
    déléguée à main_transformation après ajout des FK city_id
    """

    def get_transformer_name(self) -> str:
        return "ourairports"

    def transform(self, **kwargs: Any) -> dict[str, DataFrame]:
        df_ademe: DataFrame = kwargs["df_ademe"]

        self.logger.info("Chargement OurAirports...")

        df_airports = self._load_and_filter_airports()
        df_airports = self._geo_bucket_city_enrichment(df_airports)
        df_routes = self._build_routes(df_airports)
        df_routes = self._apply_scenario_rules(df_routes)
        df_routes = self._add_emission_ref(df_routes)

        self.logger.info("Phase OurAirports terminée.")
        return {"df_fly_trip": df_routes, "df_airports": df_airports}

    # -----------------------------------------------------------------
    # Méthodes privées
    # ----------------------------------------------------------------

    def _load_and_filter_airports(self) -> DataFrame:
        """charge, filtre et déduplique les aéroports européens"""
        cfg = self.config

        df_airports = self.spark.read.parquet(
            str(cfg.OURAIRPORTS_RAW_PATH / "airports.parquet")
        )
        df_countries = self.spark.read.parquet(
            str(cfg.OURAIRPORTS_RAW_PATH / "countries.parquet")
        )

        self.logger.debug(
            "Aéroports bruts chargés (jointure country_name + filtres Europe/type + "
            "exclusion RU hors zone + dédup ident à suivre)"
        )

        # jointure pour country_name
        df_countries_clean = df_countries.select(
            F.col("code").alias("iso_country"),
            F.col("name").alias("country_name"),
        )
        df_airports = df_airports.join(
            F.broadcast(df_countries_clean), "iso_country", "inner"
        )

        # filtrage pays europeens
        df_airports = df_airports.filter(
            F.col("iso_country").isin(cfg.EUROPEAN_COUNTRIES)
        )

        # filtrage types principaux
        airport_types = ["small_airport", "medium_airport", "large_airport"]
        df_airports = df_airports.filter(F.col("type").isin(airport_types))

        # suppression aéroports russes hors Europe geographique
        df_airports = df_airports.filter(
            ~(
                (F.col("iso_country") == "RU")
                & (
                    (F.col("latitude_deg") < cfg.EUROPE_LAT_MIN)
                    | (F.col("latitude_deg") > cfg.EUROPE_LAT_MAX)
                    | (F.col("longitude_deg") < cfg.EUROPE_LON_MIN)
                    | (F.col("longitude_deg") > cfg.EUROPE_LON_MAX)
                )
            )
        )

        # déduplication sur identifiant unique
        df_airports = df_airports.dropDuplicates(["ident"])

        self.logger.debug(
            "Aéroports européens filtrés et dédupliqués (clés: ident, iso_country, type)"
        )

        return df_airports.select(
            "id", "ident", "type", "name",
            "latitude_deg", "longitude_deg",
            "iso_country", "country_name",
        )

    def _geo_bucket_city_enrichment(self, df_airports: DataFrame) -> DataFrame:
        """
        Enrichit les aéroports avec la ville la plus proche via geo-bucketing GeoNames

        Même pattern que _geo_bucket_enrichment() de MergeTransformer :
        grille de 0.5°, 9 buckets adjacents, Haversine < 15 km
        """
        cfg = self.config

        self.logger.debug("Geo-bucketing GeoNames pour les aéroports...")
        df_geonames = (
            self.spark.read.parquet(str(cfg.GEONAMES_RAW_PATH / "cities1000.parquet"))
            .withColumn("lat_bucket", F.floor(F.col("latitude") / cfg.GEO_BUCKET_SIZE))
            .withColumn("lon_bucket", F.floor(F.col("longitude") / cfg.GEO_BUCKET_SIZE))
        )

        # 9 buckets adjacents par aéroport
        offsets = F.array(*[
            F.struct(F.lit(dlat).alias("dlat"), F.lit(dlon).alias("dlon"))
            for dlat in [-1, 0, 1]
            for dlon in [-1, 0, 1]
        ])
        df_airports_buckets = (
            df_airports
            .withColumn("offset", F.explode(offsets))
            .withColumn(
                "search_lat_b",
                F.floor(F.col("latitude_deg") / cfg.GEO_BUCKET_SIZE) + F.col("offset.dlat")
            )
            .withColumn(
                "search_lon_b",
                F.floor(F.col("longitude_deg") / cfg.GEO_BUCKET_SIZE) + F.col("offset.dlon")
            )
        )

        # jointure par bucket
        self.logger.debug("Jointure geo-bucketing aéroports × villes...")
        df_geo_matched = df_airports_buckets.join(
            F.broadcast(df_geonames),
            (df_airports_buckets.search_lat_b == df_geonames.lat_bucket)
            & (df_airports_buckets.search_lon_b == df_geonames.lon_bucket)
        )

        # distance Haversine et filtre < 15 km
        df_geo_matched = (
            df_geo_matched
            .withColumn(
                "dist_m",
                haversine_dist(
                    F.col("latitude_deg"), F.col("longitude_deg"),
                    F.col("latitude"), F.col("longitude")
                )
            )
            .filter(F.col("dist_m") < cfg.GEO_MAX_DIST_M)
        )

        # ville la plus proche, puis la plus peuplée en cas d'égalité
        w_closest = Window.partitionBy("ident").orderBy(
            F.asc("dist_m"), F.desc("population")
        )
        df_closest_city = (
            df_geo_matched
            .withColumn("rn", F.row_number().over(w_closest))
            .filter(F.col("rn") == 1)
            .select(
                "ident",
                F.col("asciiname").alias("city"),
                F.col("country_code").alias("country_geo"),
            )
        )

        self.logger.debug("Application des villes mappées aux aéroports...")
        return (
            df_airports
            .join(df_closest_city, "ident", "left")
            .withColumn(
                "city",
                F.coalesce("city", "name")
            )
            .withColumn(
                "country_geo",
                F.coalesce("country_geo", "iso_country")
            )
        )

    def _build_routes(self, df_airports: DataFrame) -> DataFrame:
        """cross-join origin × destination + distance Haversine"""
        self.logger.debug("Construction des trajets (cross-join + Haversine)...")
        cfg = self.config

        df_origins = df_airports.select(
            F.col("id").alias("origin_id"),
            F.col("ident").alias("origin_ident"),
            F.col("name").alias("origin_name"),
            F.col("type").alias("origin_type"),
            F.col("latitude_deg").alias("origin_lat"),
            F.col("longitude_deg").alias("origin_lon"),
            F.col("iso_country").alias("origin_country"),
            F.col("city").alias("origin_city"),
            F.col("country_geo").alias("origin_country_geo"),
        )

        df_destinations = df_airports.select(
            F.col("id").alias("dest_id"),
            F.col("ident").alias("dest_ident"),
            F.col("name").alias("dest_name"),
            F.col("type").alias("dest_type"),
            F.col("latitude_deg").alias("dest_lat"),
            F.col("longitude_deg").alias("dest_lon"),
            F.col("iso_country").alias("dest_country"),
            F.col("city").alias("dest_city"),
            F.col("country_geo").alias("dest_country_geo"),
        )

        # cross-join (excluant self-joins)
        df_routes = df_origins.crossJoin(df_destinations).filter(
            F.col("origin_id") != F.col("dest_id")
        )

        # distance Haversine (mètres -> km)
        df_routes = df_routes.withColumn(
            "distance_km",
            haversine_dist(
                F.col("origin_lat"), F.col("origin_lon"),
                F.col("dest_lat"), F.col("dest_lon"),
            ) / 1000.0
        )

        # filtre trajets < 10 km
        df_routes = df_routes.filter(
            F.col("distance_km") >= cfg.FLY_MIN_DISTANCE_KM
        )

        self.logger.debug("Trajets avec distances calculés.")
        return df_routes

    def _apply_scenario_rules(self, df: DataFrame) -> DataFrame:
        """applique les 7 règles du scénario 1 et ajoute trajet_type"""
        self.logger.debug("Application des règles du scénario 1...")

        ot = F.col("origin_type")
        dt = F.col("dest_type")
        dist = F.col("distance_km")

        # définir les 7 règles comme conditions
        rule_small_small = (
            (ot == "small_airport") & (dt == "small_airport") & (dist <= 1000)
        )
        rule_small_medium = (
            (
                ((ot == "small_airport") & (dt == "medium_airport"))
                | ((ot == "medium_airport") & (dt == "small_airport"))
            )
            & (dist <= 1000)
        )
        rule_small_large = (
            (
                ((ot == "small_airport") & (dt == "large_airport"))
                | ((ot == "large_airport") & (dt == "small_airport"))
            )
            & (dist <= 1000)
        )
        rule_medium_medium = (
            (ot == "medium_airport") & (dt == "medium_airport") & (dist <= 5000)
        )
        rule_medium_large = (
            (ot == "medium_airport") & (dt == "large_airport")
        )
        rule_large_medium = (
            (ot == "large_airport") & (dt == "medium_airport") & (dist <= 5000)
        )
        rule_large_large = (
            (ot == "large_airport") & (dt == "large_airport")
        )

        # filtrer : garder uniquement les trajets qui matchent une règle
        any_rule = (
            rule_small_small | rule_small_medium | rule_small_large
            | rule_medium_medium | rule_medium_large
            | rule_large_medium | rule_large_large
        )
        df = df.filter(any_rule)

        # ajouter colonne trajet_type
        df = df.withColumn(
            "trajet_type",
            F.when(rule_small_small, "small_small_1000km")
             .when(rule_small_medium, "small_medium_1000km")
             .when(rule_small_large, "small_large_1000km")
             .when(rule_medium_medium, "medium_medium_5000km")
             .when(rule_medium_large, "medium_large_unlimited")
             .when(rule_large_medium, "large_medium_5000km")
             .when(rule_large_large, "large_large_unlimited")
        )

        # dédup par paire origin-destination
        df = df.dropDuplicates(["origin_id", "dest_id"])

        self.logger.debug(
            "Trajets scénario 1 préparés (règles appliquées + dédup origin_id/dest_id)"
        )
        return df

    def _add_emission_ref(self, df: DataFrame) -> DataFrame:
        """ajoute emission_ref basé sur le mapping trajet_type -> emission_id"""
        cfg = self.config

        # construire le mapping à partir de la config
        mapping_expr = F.lit(None).cast("int")
        for emission_id, trajet_type, _ in cfg.ADEME_EMISSION_WEIGHTS:
            mapping_expr = F.when(
                F.col("trajet_type") == trajet_type, F.lit(emission_id)
            ).otherwise(mapping_expr)

        df = df.withColumn("emission_ref", mapping_expr)

        self.logger.debug("emission_ref ajouté aux trajets.")
        return df
