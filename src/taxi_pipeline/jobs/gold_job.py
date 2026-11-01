"""Job Spark silver → gold.

Le gold est recalculé intégralement à partir de tout le silver : les agrégats
(rangs, parts) dépendent de l'ensemble des données, et le volume reste faible.
"""

from __future__ import annotations

from taxi_pipeline.config import BRONZE, BRONZE_COMMUNITY_AREAS, GOLD, SILVER, SILVER_TRIPS, Settings
from taxi_pipeline.jobs.common import setup_logging
from taxi_pipeline.schemas import COMMUNITY_AREAS_SCHEMA
from taxi_pipeline.spark import build_spark
from taxi_pipeline.transformations import gold


def main() -> None:
    log = setup_logging()
    settings = Settings.from_env()
    spark = build_spark("gold_taxi_kpis", settings)

    try:
        trips = spark.read.format("delta").load(settings.lake_uri(SILVER, SILVER_TRIPS)).cache()
        community_areas = (
            spark.read.option("header", True)
            .schema(COMMUNITY_AREAS_SCHEMA)
            .csv(settings.lake_uri(BRONZE, BRONZE_COMMUNITY_AREAS))
        )

        tables = {
            "daily_kpis": gold.daily_kpis(trips),
            "pickup_zones": gold.pickup_zones(trips, community_areas),
            "hourly_demand": gold.hourly_demand(trips),
            "payment_breakdown": gold.payment_breakdown(trips),
        }
        for name, df in tables.items():
            (
                df.coalesce(1)
                .write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(settings.lake_uri(GOLD, name))
            )
            log.info("Gold %s écrit", name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
