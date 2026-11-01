"""Job Spark d'exposition : copie le gold (et le journal qualité) dans Postgres.

Le datalake reste la source de vérité ; Postgres n'est qu'une couche de
service SQL pour les outils BI (Metabase, Excel, Power BI…).
"""

from __future__ import annotations

from taxi_pipeline.config import GOLD, GOLD_TABLES, QUALITY, QUALITY_RESULTS, Settings
from taxi_pipeline.jobs.common import setup_logging
from taxi_pipeline.spark import build_spark


def main() -> None:
    log = setup_logging()
    settings = Settings.from_env()
    spark = build_spark("export_warehouse", settings)

    sources = {name: settings.lake_uri(GOLD, name) for name in GOLD_TABLES}
    sources["data_quality_checks"] = settings.lake_uri(QUALITY, QUALITY_RESULTS)

    try:
        for table, uri in sources.items():
            target = f"{settings.warehouse_schema}.{table}"
            (
                spark.read.format("delta")
                .load(uri)
                .write.mode("overwrite")
                # truncate : la table (et les vues qui en dépendent) est conservée,
                # seules les lignes sont remplacées.
                .option("truncate", "true")
                .jdbc(
                    settings.warehouse_jdbc_url,
                    target,
                    properties={
                        "user": settings.warehouse_user,
                        "password": settings.warehouse_password,
                        "driver": "org.postgresql.Driver",
                    },
                )
            )
            log.info("Exporté %s -> %s", uri, target)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
