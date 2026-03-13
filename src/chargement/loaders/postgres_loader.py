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
    # Post-chargement : index & ANALYZE
    # ──────────────────────────────────────────────────────────────

    def post_load(self) -> dict[str, Any]:
        """
        Exécute le script post-chargement : création des index (IF NOT EXISTS)
        et ANALYZE sur les tables gold.

        Doit être appelé après tous les chargements JDBC pour que les index
        soient construits sur les données finales (plus rapide qu'un index
        incrémental pendant l'insert).

        Returns
        -------
        dict avec statut et éventuelles erreurs
        """
        sql_path: Path = self.config.SQL_POST_LOAD_SCRIPT
        self.logger.debug(f"Post-chargement PG depuis : {sql_path}")

        if not sql_path.exists():
            msg = f"Script post-load introuvable : {sql_path}"
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

            self.logger.info("Index et ANALYZE post-chargement appliqués.")
            return {"status": "SUCCESS"}

        except Exception as exc:
            self.logger.error(f"Erreur post-chargement : {exc}")
            return {"status": "FAILED", "error": str(exc)}

    # ──────────────────────────────────────────────────────────────
    # Upsert via table de staging
    # ──────────────────────────────────────────────────────────────

    def upsert_via_staging(
        self,
        df: DataFrame,
        table_name: str,
    ) -> dict[str, Any]:
        """
        Upsert incrémental par source de données.

        Algorithme :
        1. Écrit df dans {table}_staging via JDBC (overwrite – table plate, sans partition)
        2. Récupère les valeurs de source distinctes présentes dans le staging
        3. DELETE FROM {table} WHERE source IN (...) → supprime uniquement
           les sources affectées par ce run
        4. INSERT INTO {table} SELECT * FROM {table}_staging
        5. DROP TABLE {table}_staging

        Résultat : les sources non concernées par ce run conservent leurs données.
        """
        staging = f"{table_name}_staging"
        self.logger.debug(f"Upsert {table_name} via staging {staging}…")

        # Étape 1 : staging
        load_result = self.load_dataframe(df, staging, mode="overwrite")
        if load_result["status"] == "FAILED":
            return load_result

        try:
            conn = self._get_connection()
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    # Étape 2 : sources présentes dans le staging
                    cur.execute(
                        sql.SQL("SELECT DISTINCT source FROM {}").format(
                            sql.Identifier(staging)
                        )
                    )
                    sources = [r[0] for r in cur.fetchall() if r[0] is not None]

                    if not sources:
                        self.logger.warning(
                            f"Staging {staging} vide ou sans colonne source – upsert ignoré."
                        )
                        cur.execute(
                            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging))
                        )
                        conn.commit()
                        return {"status": "SUCCESS", "deleted": 0, "inserted": 0}

                    self.logger.debug(
                        f"{table_name} : mise à jour pour {len(sources)} source(s) : {sources}"
                    )

                    # Étape 3 : suppression des lignes des sources concernées
                    placeholders = sql.SQL(", ").join(
                        sql.Placeholder() for _ in sources
                    )
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE source IN ({})")
                        .format(sql.Identifier(table_name), placeholders),
                        sources,
                    )
                    deleted = cur.rowcount

                    # Étape 4 : insertion depuis le staging
                    cur.execute(
                        sql.SQL("INSERT INTO {} SELECT * FROM {}").format(
                            sql.Identifier(table_name),
                            sql.Identifier(staging),
                        )
                    )
                    inserted = cur.rowcount

                    # Étape 5 : suppression du staging
                    cur.execute(
                        sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging))
                    )

                    conn.commit()

            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            self.logger.info(
                f"{table_name} : {deleted} ligne(s) remplacées, {inserted} insérée(s)."
            )
            return {"status": "SUCCESS", "deleted": deleted, "inserted": inserted}

        except Exception as exc:
            self.logger.error(f"Erreur upsert {table_name} : {exc}")
            return {"status": "FAILED", "error": str(exc)}

    # ──────────────────────────────────────────────────────────────
    # Truncate (réinitialisation complète – gardé pour usage explicite)
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
