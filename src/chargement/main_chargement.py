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
        self.monitor.start("chargement_total")

        results: dict[str, Any] = {}
        cfg = self.chargement_config

        try:
            # ── Étape 1 : Initialisation du schéma PostgreSQL ──────────────
            self.logger.log_section("INIT SCHÉMA POSTGRESQL", level="INFO")
            pg_loader = PostgresLoader(
                logger=self.logger,
                config=cfg,
            )
            schema_result = pg_loader.init_schema()
            results["init_schema"] = schema_result

            if schema_result["status"] == "FAILED":
                raise RuntimeError(f"Échec init_schema : {schema_result.get('error')}")

            # ── Étape 2 : Agrégation gold ───────────────────────────────────
            self.logger.log_section("AGRÉGATION GOLD", level="INFO")
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

            # ── Étape 3 : Chargement dans PostgreSQL ───────────────────────
            self.logger.log_section("CHARGEMENT POSTGRESQL", level="INFO")

            # Ordre de chargement (pas de FK entre les tables gold)
            tables = [
                ("gold_routes",                    dfs["gold_agg"]),
                ("gold_compare_best",              dfs["gold_compare_best"]),
            ]

            load_results: dict[str, Any] = {}
            for table_name, df in tables:
                # Remplace les NULL dans departure_country par 'XX' (contrainte NOT NULL en PG)
                df = df.fillna({"departure_country": "XX"})
                # On tronque d'abord pour éviter les doublons sur les re-runs
                pg_loader.truncate_table(table_name)
                load_result = pg_loader.load_dataframe(df, table_name, mode="append")
                load_results[table_name] = load_result

            results["loading"] = load_results
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

        # Compte les tables chargées avec succès
        load_results = results.get("loading", {})
        tables_ok = sum(
            1 for r in load_results.values()
            if isinstance(r, dict) and r.get("status") == "SUCCESS"
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
