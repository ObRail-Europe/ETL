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

    def _get_connection(self):
        """
        Ouvre une connexion psycopg2 vers PostgreSQL.

        On utilise une connexion dédiée par opération pour limiter les effets
        de bord entre étapes (schema init, upsert, post-load).
        """
        cfg = self.config
        return psycopg2.connect(
            host=cfg.PG_HOST,
            port=int(cfg.PG_PORT),
            dbname=cfg.PG_DB,
            user=cfg.PG_USER,
            password=cfg.PG_PASSWORD,
        )

    def init_schema(self) -> dict[str, Any]:
        """
        Exécute le script SQL d'initialisation (tables, partitions, index, vues).

        Returns
        -------
        dict avec statut et éventuelles erreurs
        """
        sql_path: Path = self.config.SQL_INIT_SCRIPT
        self.logger.debug(f"PG init_schema: lecture du script SQL depuis {sql_path}")

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
                    self.logger.debug("PG init_schema: exécution du script DDL dans une transaction dédiée.")
                    cur.execute(sql_content)
                    conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            self.logger.info("PG init_schema terminé: schéma, partitions, index et vues initialisés.")
            return {"status": "SUCCESS"}

        except Exception as exc:
            self.logger.error(f"Erreur d'initialisation du schéma : {exc}")
            return {"status": "FAILED", "error": str(exc)}

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
        self.logger.debug(f"PG JDBC: écriture vers {table_name} (mode={mode}, batch={cfg.JDBC_BATCH_SIZE}).")

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
                # Ce flag réduit fortement le coût des inserts en lot lors de
                # charges volumineuses.
                .option("rewriteBatchedStatements", "true")
                .mode(mode)
                .save()
            )

            self.logger.info(f"PG JDBC terminé: table {table_name} écrite avec succès.")
            return {"status": "SUCCESS"}

        except Exception as exc:
            self.logger.error(f"Erreur JDBC vers {table_name} : {exc}")
            return {"status": "FAILED", "error": str(exc)}

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
        self.logger.debug(f"PG post_load: lecture du script d'optimisation depuis {sql_path}")

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
                    self.logger.debug("PG post_load: exécution des index et ANALYZE dans une transaction contrôlée.")
                    cur.execute(sql_content)
                    conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            self.logger.info("PG post_load terminé: index et statistiques ANALYZE appliqués.")
            return {"status": "SUCCESS"}

        except Exception as exc:
            self.logger.error(f"Erreur post-chargement : {exc}")
            return {"status": "FAILED", "error": str(exc)}

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
        self.logger.debug(f"PG upsert: début pour {table_name} via staging temporaire {staging}.")

        # On passe par une table de staging pour garder une transaction SQL
        # lisible et atomique côté PostgreSQL.
        load_result = self.load_dataframe(df, staging, mode="overwrite")
        if load_result["status"] == "FAILED":
            self.logger.error(f"PG upsert: échec de chargement staging {staging} pour {table_name}.")
            return load_result

        try:
            conn = self._get_connection()
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    # On cible uniquement les sources présentes dans ce run
                    # afin de préserver les autres données historiques.
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
                        f"PG upsert: {table_name} - {len(sources)} source(s) concernée(s) dans le run: {sources}"
                    )

                    # On supprime d'abord les lignes impactées pour conserver
                    # une sémantique d'upsert simple et déterministe.
                    placeholders = sql.SQL(", ").join(
                        sql.Placeholder() for _ in sources
                    )
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE source IN ({})")
                        .format(sql.Identifier(table_name), placeholders),
                        sources,
                    )
                    deleted = cur.rowcount
                    self.logger.debug(f"PG upsert: {table_name} - {deleted} ligne(s) supprimée(s) avant réinsertion.")

                    # Le rechargement depuis staging garantit un état cohérent
                    # pour l'ensemble des sources ciblées.
                    cur.execute(
                        sql.SQL("INSERT INTO {} SELECT * FROM {}").format(
                            sql.Identifier(table_name),
                            sql.Identifier(staging),
                        )
                    )
                    inserted = cur.rowcount
                    self.logger.debug(f"PG upsert: {table_name} - {inserted} ligne(s) réinsérée(s) depuis staging.")

                    # Le nettoyage explicite de la staging évite d'accumuler
                    # des artefacts entre exécutions.
                    cur.execute(
                        sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging))
                    )

                    conn.commit()

            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            self.logger.info(f"PG upsert terminé: {table_name} ({deleted} supprimées, {inserted} insérées).")
            return {"status": "SUCCESS", "deleted": deleted, "inserted": inserted}

        except Exception as exc:
            self.logger.error(f"Erreur upsert {table_name} : {exc}")
            return {"status": "FAILED", "error": str(exc)}

    def truncate_table(self, table_name: str) -> bool:
        """
        Vide une table PostgreSQL (TRUNCATE … CASCADE).

        Utile pour recharger une table sans DROP/CREATE.
        Returns True si succès.
        """
        self.logger.debug(f"PG truncate: demande de vidage complet de {table_name}.")
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
            self.logger.debug(f"PG truncate terminé: table {table_name} vidée.")
            return True
        except Exception as exc:
            self.logger.error(f"Erreur TRUNCATE {table_name} : {exc}")
            return False
