"""Job Spark de contrôle qualité du silver pour une fenêtre de dates.

Les résultats sont historisés dans une table Delta (journal d'audit), puis la
tâche échoue si un contrôle bloquant n'est pas respecté.
"""

from __future__ import annotations

from dataclasses import asdict

from pyspark.sql import functions as F

from taxi_pipeline.config import QUALITY, QUALITY_RESULTS, SILVER, SILVER_REJECTED, SILVER_TRIPS, Settings
from taxi_pipeline.jobs.common import parse_window, setup_logging
from taxi_pipeline.quality.checks import raise_on_errors, run_silver_checks
from taxi_pipeline.spark import build_spark


def main(argv: list[str] | None = None) -> None:
    log = setup_logging()
    window = parse_window(argv, description=__doc__)
    settings = Settings.from_env()
    spark = build_spark("quality_silver", settings)

    try:
        trips = spark.read.format("delta").load(settings.lake_uri(SILVER, SILVER_TRIPS)).where(
            window.sql_filter("trip_date")
        )
        rejected = spark.read.format("delta").load(settings.lake_uri(SILVER, SILVER_REJECTED)).where(
            window.sql_filter("trip_start_date")
        )

        results = run_silver_checks(trips, rejected, window.start, window.end, settings.max_rejected_ratio)
        for r in results:
            log.info("[%s] %-22s %s observé=%s attendu=%s", r.severity, r.check, "OK" if r.passed else "KO", r.observed, r.expected)

        (
            spark.createDataFrame([{**asdict(r), "observed": float(r.observed)} for r in results])
            .withColumn("layer", F.lit(SILVER))
            .withColumn("window_start", F.lit(window.start))
            .withColumn("window_end", F.lit(window.end))
            .withColumn("checked_at", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .save(settings.lake_uri(QUALITY, QUALITY_RESULTS))
        )

        raise_on_errors(results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
