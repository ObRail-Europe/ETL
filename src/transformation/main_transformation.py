"""
Orchestrateur de transformation - lance les 8 phases séquentiellement

1. MobilityDatabaseTransformer → df_mdb
2. BackOnTrackTransformer → df_botn  
3. EmberTransformer → df_ember
4. AdemeTransformer → df_ademe
5. OurAirportsTransformer → df_airports (enrichis avec villes), df_fly_trip (routes)
6. MergeTransformer → train_trips.parquet, localite.parquet (+ villes aéroport), emission.parquet
7. Enrichissement flight + sauvegarde → flight.parquet (avec FK city_id)
8. StopMatchingTransformer → stop_matching.parquet
"""

from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from common.logging import SparkLogger, PerformanceMonitor
from common.config import BaseConfig
from common.spark_manager import SparkManager

from transformation.config.settings import TransformationConfig
from transformation.transformers.mobility_database_transformer import MobilityDatabaseTransformer
from transformation.transformers.backontrack_transformer import BackOnTrackTransformer
from transformation.transformers.ember_transformer import EmberTransformer
from transformation.transformers.ademe_transformer import AdemeTransformer
from transformation.transformers.merge_transformer import MergeTransformer
from transformation.transformers.ourairports_transformer import OurAirportsTransformer
from transformation.transformers.stop_matching_transformer import StopMatchingTransformer


class DataTransformer:
    """
    Contrairement à l'extraction (qui est parallèle car I/O-bound), la transformation
    est séquentielle : chaque transformateur produit des DataFrames qui sont ensuit utilisés par les suivants
    """

    def __init__(
        self,
        spark: SparkSession,
        spark_manager: SparkManager,
        logger: SparkLogger,
        config: BaseConfig,
        transformation_config: TransformationConfig | None = None
    ):
        """
        Initialise l'orchestrateur avec les dépendances injectées

        Parameters
        ----------
        spark : SparkSession
            Session Spark partagée par toute la pipeline
        spark_manager : SparkManager
            Manager du cycle de vie Spark
        logger : SparkLogger
            Logger pre-configuré pour la transformation
        config : BaseConfig
            Config de base de la pipeline
        transformation_config : TransformationConfig, optional
            Config spécifique à la transfo (si None, charge config par défaut)
        """
        self.spark = spark
        self.spark_manager = spark_manager
        self.logger = logger
        self.config = config
        self.transformation_config = transformation_config or TransformationConfig()
        self.monitor = PerformanceMonitor(self.logger)

    def run(self) -> dict[str, Any]:
        """
        Lance les transformateurs l'un après l'autre et retourne le résultat global

        Returns
        -------
        dict[str, Any]
            Résultat global avec statut et métriques de chaque transformateur

        Notes
        -----
        L'ordre d'exécution est fixe car les transformateurs ont des dépendances :
        MDB, BOTN, Ember et ADEME sont indépendants
        OurAirports dépend de ADEME (pour les emission_ref)
        Merge dépend de MDB+BOTN+Ember+ADEME+OurAirports (pour localite).
        Flight FK dépend de Merge (pour localite)
        StopMatching dépend de Merge (pour les stops) et OurAirports (pour les aéroports)
        
        Si un transformateur echoue, la pipeline s'arrête directement
        """
        self.logger.info(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        self.monitor.start("transformation_totale")
        results: dict[str, Any] = {}

        try:
            # Phase 1 : Mobility Database
            self.logger.log_section("TRANSFORMATION MDB", level="INFO")
            mdb_transformer = MobilityDatabaseTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            mdb_result = mdb_transformer.run()
            results['mobility_database'] = mdb_result
            df_mdb = mdb_result['dataframes']['df_mdb']

            # Phase 2 : Back-on-Track
            self.logger.log_section("TRANSFORMATION BOTN", level="INFO")
            botn_transformer = BackOnTrackTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            botn_result = botn_transformer.run()
            results['backontrack'] = botn_result
            df_botn = botn_result['dataframes']['df_botn']

            # Phase 3 : Ember
            self.logger.log_section("TRANSFORMATION EMBER", level="INFO")
            ember_transformer = EmberTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            ember_result = ember_transformer.run()
            results['ember'] = ember_result
            df_ember = ember_result['dataframes']['df_ember']

            # Phase 4 : ADEME
            self.logger.log_section("TRANSFORMATION ADEME", level="INFO")
            ademe_transformer = AdemeTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            ademe_result = ademe_transformer.run()
            results['ademe'] = ademe_result
            df_ademe = ademe_result['dataframes']['df_ademe']

            # Phase 5 : OurAirports (city matching + routes, sans sauvegarde)
            self.logger.log_section("TRANSFORMATION OURAIRPORTS", level="INFO")
            ourairports_transformer = OurAirportsTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            ourairports_result = ourairports_transformer.run(
                df_ademe=df_ademe
            )
            results['ourairports'] = ourairports_result
            df_airports = ourairports_result['dataframes']['df_airports']
            df_fly_trip = ourairports_result['dataframes']['df_fly_trip']

            # Phase 4 : Merge + Schéma en étoile
            self.logger.log_section("MERGE ET SCHÉMA EN ÉTOILE", level="INFO")
            merge_transformer = MergeTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            merge_result = merge_transformer.run(
                df_mdb=df_mdb,
                df_botn=df_botn,
                df_ember=df_ember,
                df_ademe=df_ademe,
                df_airports=df_airports,
            )
            results['merge'] = merge_result
            df_trip = merge_result['dataframes']['df_trip']
            df_localite = merge_result['dataframes']['df_localite']

            # Phase 7 : Ajout FK city_id dans flight + sauvegarde
            self.logger.log_section("ENRICHISSEMENT FLIGHT + SAUVEGARDE", level="INFO")
            self._enrich_and_save_flight(df_fly_trip, df_localite)

            # Phase 8 : StopMatching 
            self.logger.log_section("STOP MATCHING (GARE ↔ AÉROPORT)", level="INFO")
            stop_matching_transformer = StopMatchingTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            stop_matching_result = stop_matching_transformer.run(
                df_trip=df_trip,
                df_airports=df_airports,
            )
            results['stop_matching'] = stop_matching_result

            overall_status = 'SUCCESS'

        except Exception as e:
            self.logger.error(f"Échec de la transformation: {str(e)}")
            overall_status = 'FAILED'
            results['error'] = str(e)

        duration = self.monitor.stop("transformation_totale")
        self._print_summary(overall_status, results, duration)

        return {
            'status': overall_status,
            'duration_seconds': duration,
            'transformers': results
        }

    def _enrich_and_save_flight(
        self,
        df_fly_trip: Any,
        df_localite: Any,
    ) -> None:
        """
        Ajoute les FK origin_city_id et dest_city_id à flight, puis sauvegarde
        Jointure avec localite sur (city_name, country_code) pour origin et destination
        """
        cfg = self.transformation_config

        # préparer les alias pour éviter les conflits de colonnes
        df_loc_origin = df_localite.select(
            F.col("city_id").alias("origin_city_id"),
            F.col("city_name").alias("_o_city"),
            F.col("country_code").alias("_o_country"),
        )
        df_loc_dest = df_localite.select(
            F.col("city_id").alias("dest_city_id"),
            F.col("city_name").alias("_d_city"),
            F.col("country_code").alias("_d_country"),
        )

        # jointure origin
        df_flight = df_fly_trip.join(
            F.broadcast(df_loc_origin),
            (df_fly_trip.origin_city == df_loc_origin._o_city)
            & (df_fly_trip.origin_country_geo == df_loc_origin._o_country),
            "left"
        ).drop("_o_city", "_o_country")

        # jointure destination
        df_flight = df_flight.join(
            F.broadcast(df_loc_dest),
            (df_flight.dest_city == df_loc_dest._d_city)
            & (df_flight.dest_country_geo == df_loc_dest._d_country),
            "left"
        ).drop("_d_city", "_d_country")

        # supp les colonnes intermediaires de city matching
        df_flight = df_flight.drop(
            "origin_city", "origin_country_geo",
            "dest_city", "dest_country_geo",
        )

        # sauvegarde flight.parquet
        self.logger.info("Sauvegarde flight.parquet...")
        df_flight.coalesce(cfg.FLIGHT_COALESCE_PARTITIONS).write.mode("overwrite").parquet(
            str(cfg.FLIGHT_OUTPUT_PATH)
        )
        self.logger.debug(f"flight sauvegardé : {cfg.FLIGHT_OUTPUT_PATH}")

    def _print_summary(
        self,
        status: str,
        results: dict[str, Any],
        duration: float
    ) -> None:
        """
        Affiche le résumé final dans les logs

        Parameters
        ----------
        status : str
            Statut global (SUCCESS, FAILED)
        results : dict[str, Any]
            Résultats de chaque transformateur
        duration : float
            Durée totale (en secondes)
        """
        self.logger.log_section("RÉSUMÉ DE LA TRANSFORMATION", level="INFO")

        transformer_count = sum(
            1 for k, v in results.items()
            if isinstance(v, dict) and v.get('status') == 'SUCCESS'
        )

        metrics: dict[str, str | float | int] = {
            'Statut global': status,
            'Transformateurs réussis': transformer_count,
            'Durée totale': f"{duration:.2f}s",
        }
        self.logger.log_metrics(metrics, prefix="la transformation")
        self.logger.log_section(f"TRANSFORMATION TERMINÉE - {status}", level="INFO")


if __name__ == '__main__':
    raise RuntimeError(
        "ERREUR: Ce module ne doit pas être exécuté directement!\n"
        "\n"
        "Le module de transformation est intégré à la pipeline ETL principale.\n"
        "Utilisez plutôt:\n"
        "  python src/pipeline.py [--phases transform]\n"
        "\n"
        "Pour plus d'informations, consultez src/pipeline.py"
    )
