"""Fabrique de SparkSession partagée par tous les jobs."""

from __future__ import annotations

from pyspark.sql import SparkSession

from taxi_pipeline.config import Settings


def build_spark(app_name: str, settings: Settings | None = None) -> SparkSession:
    """SparkSession configurée pour Delta Lake sur MinIO (s3a).

    Les JAR (delta, hadoop-aws, driver Postgres) sont copiés dans l'image Docker
    au build : aucun téléchargement Maven au runtime, donc des exécutions
    reproductibles et hors-ligne.
    """
    settings = settings or Settings.from_env()
    return (
        SparkSession.builder.appName(app_name)
        # Delta Lake : transactions ACID, overwrite partiel (replaceWhere), time travel
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Connecteur S3A vers MinIO
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", settings.s3_endpoint.startswith("https"))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Volume modeste (quelques millions de lignes) : 200 partitions de shuffle
        # par défaut créeraient surtout des petites tâches et des petits fichiers.
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
