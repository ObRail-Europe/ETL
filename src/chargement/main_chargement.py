"""
Orchestrateur de chargement gold – Phase 3 de la pipeline ETL.

Étapes :
1. Initialise le schéma PostgreSQL (tables, partitions, index, vues) via init.sql
2. Agrège les données silver en 5 tables gold (via GoldAggregator)
3. Charge chaque table gold dans PostgreSQL (via PostgresLoader / JDBC)
"""

from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession

from common.logging import SparkLogger, PerformanceMonitor
from common.config import BaseConfig
from common.spark_manager import SparkManager

from chargement.config.settings import ChargementConfig
from chargement.loaders.gold_aggregator import GoldAggregator
from chargement.loaders.postgres_loader import PostgresLoader


class DataLoader:
    """
    Orchestrateur de la phase 3 : agrégation gold + chargement PostgreSQL.

    Séquentiel (pas de parallélisme) car les agrégations ont des dépendances :
    train → flight → union → candidats → best, puis push PG dans l'ordre.
    """

    def __init__(
        self,
        spark: SparkSession,
        spark_manager: SparkManager,
        logger: SparkLogger,
        config: BaseConfig,
        chargement_config: ChargementConfig | None = None,
    ):
        """
        Initialise l'orchestrateur avec les dépendances injectées par la pipeline.

        Parameters
        ----------
        spark : SparkSession
            Session Spark partagée par toute la pipeline
        spark_manager : SparkManager
            Manager du cycle de vie Spark
        logger : SparkLogger
            Logger pré-configuré pour le chargement
        config : BaseConfig
            Config de base de la pipeline
        chargement_config : ChargementConfig, optional
            Config spécifique au chargement (si None, charge config par défaut)
        """
        self.spark = spark
        self.spark_manager = spark_manager
        self.logger = logger
        self.config = config
        self.chargement_config = chargement_config or ChargementConfig()
        self.monitor = PerformanceMonitor(self.logger)

    def run(self) -> dict[str, Any]:
        """
        Exécute la phase 3 complète :
        1. Initialisation du schéma PG
        2. Agrégation des 5 tables gold
        3. Chargement dans PostgreSQL

        Returns
        -------
        dict avec statut global, durée et métriques par étape
        """
        self.logger.info(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("Démarrage chargement: initialisation PG, agrégation gold, puis publication incrémentale.")
        self.monitor.start("chargement_total")

        results: dict[str, Any] = {}
        cfg = self.chargement_config

        try:
            # On initialise le schéma avant tout chargement pour garantir
            # que les tables/partitions attendues existent déjà côté PostgreSQL.
            self.logger.log_section("INIT SCHÉMA POSTGRESQL", level="INFO")
            self.logger.info("Chargement - étape 1/4: préparation du schéma PostgreSQL (DDL + partitions + vues).")
            pg_loader = PostgresLoader(
                logger=self.logger,
                config=cfg,
            )
            schema_result = pg_loader.init_schema()
            results["init_schema"] = schema_result

            if schema_result["status"] == "FAILED":
                raise RuntimeError(f"Échec init_schema : {schema_result.get('error')}")

            # Les DataFrames gold sont calculés avant la phase JDBC pour garder
            # un enchaînement clair entre préparation et persistance.
            self.logger.log_section("AGRÉGATION GOLD", level="INFO")
            self.logger.info("Chargement - étape 2/4: construction des DataFrames gold depuis les sorties silver.")
            aggregator = GoldAggregator(
                spark=self.spark,
                logger=self.logger,
                config=cfg,
            )
            agg_result = aggregator.run()
            results["aggregation"] = agg_result

            if agg_result["status"] == "FAILED":
                raise RuntimeError(f"Échec agrégation gold : {agg_result.get('error')}")

            dfs = agg_result["dataframes"]

            # Le chargement est fait table par table pour isoler les erreurs
            # et garder une traçabilité simple dans les logs d'exécution.
            self.logger.log_section("CHARGEMENT POSTGRESQL", level="INFO")
            self.logger.info("Chargement - étape 3/4: upsert des tables gold dans PostgreSQL via staging.")

            # L'ordre est explicite pour pouvoir facilement l'ajuster si des
            # dépendances apparaissent plus tard (FK, vues matérialisées, etc.).
            tables = [
                ("gold_routes",                    dfs["gold_agg"]),
                ("gold_compare_best",              dfs["gold_compare_best"]),
            ]

            load_results: dict[str, Any] = {}
            for table_name, df in tables:
                self.logger.info(f"Chargement table: démarrage upsert incrémental pour {table_name}.")
                # On force une valeur par défaut pour respecter la contrainte
                # NOT NULL sur departure_country côté PostgreSQL.
                df = df.fillna({"departure_country": "XX"})
                # L'upsert ne remplace que les partitions concernées par le run,
                # ce qui évite de réécrire inutilement tout l'historique.
                load_result = pg_loader.upsert_via_staging(df, table_name)
                load_results[table_name] = load_result
                self.logger.info(
                    f"Chargement table: {table_name} terminé avec statut {load_result.get('status', 'UNKNOWN')}."
                )

            results["loading"] = load_results

            # Les index et ANALYZE sont lancés après insertion massive, car
            # c'est plus stable et généralement plus rapide qu'en flux continu.
            self.logger.log_section("POST-CHARGEMENT (INDEX & ANALYZE)", level="INFO")
            self.logger.info("Chargement - étape 4/4: optimisation post-load (index + ANALYZE).")
            post_result = pg_loader.post_load()
            results["post_load"] = post_result

            if post_result["status"] == "FAILED":
                raise RuntimeError(f"Échec post_load : {post_result.get('error')}")

            overall_status = "SUCCESS"

        except Exception as exc:
            self.logger.error(f"Échec du chargement : {str(exc)}")
            overall_status = "FAILED"
            results["error"] = str(exc)

        duration = self.monitor.stop("chargement_total")
        self._print_summary(overall_status, results, duration)

        return {
            "status": overall_status,
            "duration_seconds": duration,
            "steps": results,
        }

    def _print_summary(
        self,
        status: str,
        results: dict[str, Any],
        duration: float,
    ) -> None:
        """Affiche le résumé final dans les logs."""
        self.logger.log_section("RÉSUMÉ DU CHARGEMENT", level="INFO")

        # Ce compteur donne une vision rapide du niveau de complétion réel,
        # même si le détail complet reste disponible dans results["loading"].
        load_results: dict[str, dict[str, Any]] = results.get("loading", {})
        tables_ok = sum(
            1 for r in load_results.values()
            if r.get("status") == "SUCCESS"
        )

        metrics: dict[str, str | float | int] = {
            "Statut global":          status,
            "Tables chargées":        f"{tables_ok}/{len(load_results)}",
            "Durée totale":           f"{duration:.2f}s",
        }
        self.logger.log_metrics(metrics, prefix="le chargement")
        self.logger.log_section(f"CHARGEMENT TERMINÉ – {status}", level="INFO")


if __name__ == "__main__":
    raise RuntimeError(
        "ERREUR: Ce module ne doit pas être exécuté directement!\n"
        "\n"
        "Le module de chargement est intégré à la pipeline ETL principale.\n"
        "Utilisez plutôt:\n"
        "  python src/pipeline.py [--phases load]\n"
        "\n"
        "Pour plus d'informations, consultez src/pipeline.py"
    )
