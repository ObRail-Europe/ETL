"""
PostgresLoader – initialise le schéma et charge les données gold dans PostgreSQL.

Deux étapes :
1. init_schema()  : exécute scripts/init.sql (tables partitionnées, index, vues)
2. load_dataframe() : push un DataFrame Spark vers PG via JDBC
"""

import psycopg2
from psycopg2 import sql
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame

from common.logging import SparkLogger
from chargement.config.settings import ChargementConfig


class PostgresLoader:
    """
    Gère le schéma PostgreSQL et le chargement des DataFrames gold.

    Utilise psycopg2 pour les DDL (CREATE TABLE, CREATE INDEX, CREATE VIEW)
    et Spark JDBC pour le chargement des données (plus performant pour les gros volumes).
    """

    def __init__(self, logger: SparkLogger, config: ChargementConfig):
        self.logger = logger
        self.config = config

    # ──────────────────────────────────────────────────────────────
    # Connexion psycopg2
    # ──────────────────────────────────────────────────────────────

    def _get_connection(self):
        """Ouvre une connexion psycopg2 vers PostgreSQL."""
        cfg = self.config
        return psycopg2.connect(
            host=cfg.PG_HOST,
            port=int(cfg.PG_PORT),
            dbname=cfg.PG_DB,
            user=cfg.PG_USER,
            password=cfg.PG_PASSWORD,
        )

    # ──────────────────────────────────────────────────────────────
    # Initialisation du schéma
    # ──────────────────────────────────────────────────────────────

    def init_schema(self) -> dict[str, Any]:
        """
        Exécute le script SQL d'initialisation (tables, partitions, index, vues).

        Returns
        -------
        dict avec statut et éventuelles erreurs
        """
        sql_path: Path = self.config.SQL_INIT_SCRIPT
        self.logger.debug(f"Initialisation du schéma PG depuis : {sql_path}")

        if not sql_path.exists():
            msg = f"Script SQL introuvable : {sql_path}"
            self.logger.error(msg)
            return {"status": "FAILED", "error": msg}

        sql_content = sql_path.read_text(encoding="utf-8")

        try:
            conn = self._get_connection()
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    cur.execute(sql_content)
                    conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            self.logger.info("Schéma PostgreSQL initialisé.")
            return {"status": "SUCCESS"}

        except Exception as exc:
            self.logger.error(f"Erreur d'initialisation du schéma : {exc}")
            return {"status": "FAILED", "error": str(exc)}

    # ──────────────────────────────────────────────────────────────
    # Chargement des DataFrames via JDBC
    # ──────────────────────────────────────────────────────────────

    def load_dataframe(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> dict[str, Any]:
        """
        Écrit un DataFrame gold dans une table PostgreSQL via JDBC.

        Parameters
        ----------
        df : DataFrame
            DataFrame Spark à charger
        table_name : str
            Nom de la table PostgreSQL cible (ex: 'gold_routes')
        mode : str
            "append" (défaut) ou "overwrite"

        Returns
        -------
        dict avec statut et nombre de lignes insérées
        """
        cfg = self.config
        self.logger.debug(f"Chargement JDBC → {table_name} (mode={mode})…")

        try:
            (
                df.write
                .format("jdbc")
                .option("url", cfg.pg_jdbc_url())
                .option("dbtable", table_name)
                .option("user", cfg.PG_USER)
                .option("password", cfg.PG_PASSWORD)
                .option("driver", "org.postgresql.Driver")
                .option("batchsize", cfg.JDBC_BATCH_SIZE)
                # rewriteBatchedStatements accélère massivement les INSERT en lot
                .option("rewriteBatchedStatements", "true")
                .mode(mode)
                .save()
            )

            self.logger.info(f"{table_name} chargée.")
            return {"status": "SUCCESS"}

        except Exception as exc:
            self.logger.error(f"Erreur JDBC vers {table_name} : {exc}")
            return {"status": "FAILED", "error": str(exc)}

    # ──────────────────────────────────────────────────────────────
    # Truncate (réinitialisation avant rechargement)
    # ──────────────────────────────────────────────────────────────

    def truncate_table(self, table_name: str) -> bool:
        """
        Vide une table PostgreSQL (TRUNCATE … CASCADE).

        Utile pour recharger une table sans DROP/CREATE.
        Returns True si succès.
        """
        self.logger.debug(f"TRUNCATE {table_name}…")
        try:
            conn = self._get_connection()
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("TRUNCATE TABLE {} CASCADE").format(
                        sql.Identifier(table_name)
                    ))
            finally:
                conn.close()
            self.logger.debug(f"{table_name} vidée")
            return True
        except Exception as exc:
            self.logger.error(f"Erreur TRUNCATE {table_name} : {exc}")
            return False
