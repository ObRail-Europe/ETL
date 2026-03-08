"""
======================================================================
ObRail Europe – ETL  |  Chargement PostgreSQL
Script  : load_to_postgres.py
Usage   : spark-submit --driver-class-path /path/to/postgresql.jar load_to_postgres.py
======================================================================

Pré-requis :
  - Variables d'environnement dans .env (copiées dans l'env Shell ou chargées via python-dotenv)
  - Driver JDBC PostgreSQL disponible (postgresql-42.x.x.jar)
  - Table `railway_trips` déjà créée via sccripts/init.sql

Variables .env attendues :
  PG_HOST      = localhost
  PG_PORT      = 5432
  PG_DB        = obrail
  PG_USER      = obrail_user
  PG_PASSWORD  = s3cr3t
  JDBC_JAR     = /opt/jars/postgresql-42.7.3.jar
  INPUT_PATH   = ./data/processed/railway_trips   (dossier Parquet/CSV Spark)
  INPUT_FORMAT = parquet   (ou csv, delta, etc.)
  BATCH_SIZE   = 50000
"""

import os
import sys
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, DateType
)

# ──────────────────────────────────────────────────────────────
# 0. Configuration & logging
# ──────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("obrail.load")


def _require(var: str) -> str:
    """Lit une variable d'environnement obligatoire."""
    value = os.getenv(var)
    if not value:
        log.error("Variable d'environnement manquante : %s", var)
        sys.exit(1)
    return value


PG_HOST      = os.getenv("PG_HOST",      "localhost")
PG_PORT      = os.getenv("PG_PORT",      "5432")
PG_DB        = _require("PG_DB")
PG_USER      = _require("PG_USER")
PG_PASSWORD  = _require("PG_PASSWORD")
JDBC_JAR     = os.getenv("JDBC_JAR",     "/opt/jars/postgresql-42.7.3.jar")
INPUT_PATH   = os.getenv("INPUT_PATH",   "./data/processed/railway_trips")
INPUT_FORMAT = os.getenv("INPUT_FORMAT", "parquet")
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", "50000"))

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# ──────────────────────────────────────────────────────────────
# 1. SparkSession
# ──────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ObRail-ETL-Load")
        .config("spark.jars", JDBC_JAR)
        # Optimisations mémoire pour les gros volumes
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.executor.memory",        "8g")
        .config("spark.driver.memory",          "4g")
        # Active la réécriture de prédicats sur les partitions
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .getOrCreate()
    )


# ──────────────────────────────────────────────────────────────
# 2. Lecture du DataFrame transformé
# ──────────────────────────────────────────────────────────────
def read_dataframe(spark: SparkSession):
    log.info("Lecture des données depuis : %s (format=%s)", INPUT_PATH, INPUT_FORMAT)

    reader = spark.read.format(INPUT_FORMAT)

    if INPUT_FORMAT == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(INPUT_PATH)
    log.info("Schéma chargé : %d colonnes, %d partitions Spark",
             len(df.columns), df.rdd.getNumPartitions())
    return df


# ──────────────────────────────────────────────────────────────
# 3. Nettoyage / harmonisation avant chargement
# ──────────────────────────────────────────────────────────────
def clean_dataframe(df):
    """
    Applique les corrections finales pour garantir la cohérence
    avec le schéma PostgreSQL (init.sql).
    """
    log.info("Nettoyage et harmonisation du DataFrame...")

    # --- departure_country : valeur non-nulle obligatoire (clé de partition PG) ---
    df = df.withColumn(
        "departure_country",
        F.when(F.col("departure_country").isNull(), F.lit("XX"))
         .otherwise(F.upper(F.trim(F.col("departure_country"))))
    )

    # --- arrival_country : mise en majuscules ---
    df = df.withColumn(
        "arrival_country",
        F.upper(F.trim(F.col("arrival_country")))
    )

    # --- is_night_train : cast sûr vers boolean ---
    if str(df.schema["is_night_train"].dataType) == "StringType()":
        df = df.withColumn(
            "is_night_train",
            F.col("is_night_train").cast("boolean")
        )
    df = df.withColumn(
        "is_night_train",
        F.coalesce(F.col("is_night_train"), F.lit(False))
    )

    # --- route_type : smallint safe ---
    df = df.withColumn("route_type", F.col("route_type").cast("short"))

    # --- Suppression des lignes sans trip_id (obligatoire) ---
    before = df.count()
    df = df.filter(F.col("trip_id").isNotNull())
    after  = df.count()
    if before != after:
        log.warning("Suppression de %d lignes sans trip_id", before - after)

    log.info("DataFrame prêt : %d lignes", after)
    return df


# ──────────────────────────────────────────────────────────────
# 4. Écriture JDBC vers PostgreSQL
#    Stratégie : repartitionnement par pays + chargement parallèle
# ──────────────────────────────────────────────────────────────
def write_to_postgres(df):
    """
    Écrit le DataFrame dans la table `railway_trips` de PostgreSQL.

    Optimisations :
    - repartition par departure_country → aligne sur les partitions PG
    - numPartitions JDBC → parallélise les connexions
    - batchsize élevé → réduit la latence réseau
    - mode "append" → accumulation sans écrasement
    """
    log.info("Repartitionnement par departure_country avant chargement JDBC...")
    df = df.repartition("departure_country")

    num_jdbc_partitions = min(df.rdd.getNumPartitions(), 20)

    jdbc_options = {
        "url":            JDBC_URL,
        "dbtable":        "railway_trips",
        "user":           PG_USER,
        "password":       PG_PASSWORD,
        "driver":         "org.postgresql.Driver",
        "batchsize":      str(BATCH_SIZE),
        "numPartitions":  str(num_jdbc_partitions),
        "rewriteBatchedStatements": "true",
    }

    log.info(
        "Chargement vers PostgreSQL | url=%s | table=railway_trips | numPartitions=%s | batchsize=%s",
        JDBC_URL, num_jdbc_partitions, BATCH_SIZE
    )

    (
        df.write
          .format("jdbc")
          .options(**jdbc_options)
          .mode("append")
          .save()
    )

    log.info("Chargement terminé avec succès.")


# ──────────────────────────────────────────────────────────────
# 5. Point d'entrée
# ──────────────────────────────────────────────────────────────
def main():
    log.info("=== ObRail ETL – Démarrage du chargement PostgreSQL ===")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")   # réduit le verbeux Spark

    try:
        df = read_dataframe(spark)
        df = clean_dataframe(df)
        write_to_postgres(df)
        log.info("=== Chargement terminé avec succès ===")
    except Exception as exc:
        log.exception("Erreur critique lors du chargement : %s", exc)
        sys.exit(2)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
