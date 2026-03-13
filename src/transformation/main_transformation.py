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
        self.logger.info("Démarrage transformation: exécution séquentielle des 8 phases avec arrêt immédiat en cas d'échec.")

        self.monitor.start("transformation_totale")
        results: dict[str, Any] = {}

        try:
            self.logger.info("Étape 1/8 - Mobility Database: préparation des tables GTFS normalisées.")
            self.logger.log_section("TRANSFORMATION MDB", level="INFO")
            mdb_transformer = MobilityDatabaseTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            mdb_result = mdb_transformer.run()
            results['mobility_database'] = mdb_result
            df_mdb = mdb_result['dataframes']['df_mdb']
            self.logger.info("Étape 1/8 terminée - Mobility Database prêt pour la fusion.")

            self.logger.info("Étape 2/8 - BackOnTrack: harmonisation BOTN et calcul des segments.")
            self.logger.log_section("TRANSFORMATION BOTN", level="INFO")
            botn_transformer = BackOnTrackTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            botn_result = botn_transformer.run()
            results['backontrack'] = botn_result
            df_botn = botn_result['dataframes']['df_botn']
            self.logger.info("Étape 2/8 terminée - BackOnTrack aligné avec le schéma MDB.")

            self.logger.info("Étape 3/8 - Ember: préparation des intensités carbone par pays.")
            self.logger.log_section("TRANSFORMATION EMBER", level="INFO")
            ember_transformer = EmberTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            ember_result = ember_transformer.run()
            results['ember'] = ember_result
            df_ember = ember_result['dataframes']['df_ember']
            self.logger.info("Étape 3/8 terminée - Intensités Ember disponibles.")

            self.logger.info("Étape 4/8 - ADEME: calcul des facteurs d'émission aérien.")
            self.logger.log_section("TRANSFORMATION ADEME", level="INFO")
            ademe_transformer = AdemeTransformer(
                spark=self.spark,
                logger=self.logger,
                config=self.transformation_config
            )
            ademe_result = ademe_transformer.run()
            results['ademe'] = ademe_result
            df_ademe = ademe_result['dataframes']['df_ademe']
            self.logger.info("Étape 4/8 terminée - Facteurs ADEME prêts.")

            self.logger.info("Étape 5/8 - OurAirports: enrichissement villes + génération des routes aériennes.")
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
            self.logger.info("Étape 5/8 terminée - Aéroports enrichis et trajets flight générés.")

            self.logger.info("Étape 6/8 - Merge: fusion MDB/BOTN et construction du schéma en étoile.")
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
            self.logger.info("Étape 6/8 terminée - Tables trip/localite/emission prêtes.")

            self.logger.info("Étape 7/8 - Flight: ajout des FK city_id puis écriture du parquet final.")
            self.logger.log_section("ENRICHISSEMENT FLIGHT + SAUVEGARDE", level="INFO")
            self._enrich_and_save_flight(df_fly_trip, df_localite)
            self.logger.info("Étape 7/8 terminée - flight.parquet sauvegardé.")

            self.logger.info("Étape 8/8 - StopMatching: rapprochement gare ↔ aéroport.")
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
            self.logger.info("Étape 8/8 terminée - stop_matching.parquet produit.")

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
        self.logger.info("Flight enrichissement: jointure des villes origine/destination vers city_id.")

        # Prépare les aliases pour distinguer clairement les jointures origine/destination.
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

        self.logger.debug("Flight enrichissement: jointure FK origin_city_id en cours...")
        df_flight = df_fly_trip.join(
            F.broadcast(df_loc_origin),
            (df_fly_trip.origin_city == df_loc_origin._o_city)
            & (df_fly_trip.origin_country_geo == df_loc_origin._o_country),
            "left"
        ).drop("_o_city", "_o_country")

        self.logger.debug("Flight enrichissement: jointure FK dest_city_id en cours...")
        df_flight = df_flight.join(
            F.broadcast(df_loc_dest),
            (df_flight.dest_city == df_loc_dest._d_city)
            & (df_flight.dest_country_geo == df_loc_dest._d_country),
            "left"
        ).drop("_d_city", "_d_country")

        self.logger.debug("Flight enrichissement: suppression des colonnes techniques intermédiaires.")
        df_flight = df_flight.drop(
            "origin_city", "origin_country_geo",
            "dest_city", "dest_country_geo",
        )

        self.logger.info("Flight enrichissement: écriture de flight.parquet...")
        df_flight.coalesce(cfg.FLIGHT_COALESCE_PARTITIONS).write.mode("overwrite").parquet(
            str(cfg.FLIGHT_OUTPUT_PATH)
        )
        self.logger.debug(f"Flight enrichissement terminé: fichier sauvegardé dans {cfg.FLIGHT_OUTPUT_PATH}")

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

        typed_results: dict[str, dict[str, Any]] = {
            name: value for name, value in results.items() if isinstance(value, dict)
        }
        transformer_count = sum(
            1 for value in typed_results.values()
            if value.get('status') == 'SUCCESS'
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
