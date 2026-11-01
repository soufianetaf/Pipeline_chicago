"""Job Spark bronze → silver pour une fenêtre de dates.

    spark-submit silver_job.py --start 2023-01-01 --end 2023-02-01
"""

from __future__ import annotations

from pyspark import StorageLevel
from pyspark.sql import functions as F

from taxi_pipeline.config import BRONZE, BRONZE_TRIPS, SILVER, SILVER_REJECTED, SILVER_TRIPS, Settings
from taxi_pipeline.jobs.common import parse_window, setup_logging
from taxi_pipeline.schemas import BRONZE_TRIPS_SCHEMA
from taxi_pipeline.spark import build_spark
from taxi_pipeline.transformations.silver import bronze_to_silver


def main(argv: list[str] | None = None) -> None:
    log = setup_logging()
    window = parse_window(argv, description=__doc__)
    settings = Settings.from_env()
    spark = build_spark("silver_taxi_trips", settings)

    try:
        bronze_uri = settings.lake_uri(BRONZE, BRONZE_TRIPS)
        raw = (
            spark.read.option("header", True)
            .option("basePath", bronze_uri)
            .schema(BRONZE_TRIPS_SCHEMA)
            .csv(bronze_uri)
            # Filtre sur la colonne de partition : Spark ne lit que les jours utiles.
            .where(window.sql_filter("trip_start_date"))
            # Lineage ligne à ligne : de quel fichier bronze vient chaque trajet.
            .withColumn("_source_file", F.col("_metadata.file_path"))
            # Relu deux fois (lignes valides + rejetées) : on évite de relire S3.
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        valid, rejected = bronze_to_silver(raw)

        # replaceWhere : seules les partitions de la fenêtre sont remplacées,
        # de façon atomique. Relancer un mois ne touche pas aux autres mois.
        (
            valid.repartition("trip_date")
            .write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", window.sql_filter("trip_date"))
            .partitionBy("trip_date")
            .save(settings.lake_uri(SILVER, SILVER_TRIPS))
        )
        (
            rejected.coalesce(1)
            .write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", window.sql_filter("trip_start_date"))
            .partitionBy("trip_start_date")
            .save(settings.lake_uri(SILVER, SILVER_REJECTED))
        )

        reasons = rejected.groupBy("rejection_reason").count().collect()
        log.info(
            "Silver [%s, %s) : %d lignes bronze, rejets par motif : %s",
            window.start,
            window.end,
            raw.count(),
            {r["rejection_reason"]: r["count"] for r in reasons},
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
