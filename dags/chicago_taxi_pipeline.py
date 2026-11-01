"""
### Pipeline médaillon — Chicago Taxi Trips

Un run par mois (`@monthly`, `catchup=True`) : chaque run traite **son**
`data_interval` et uniquement lui. Toutes les tâches sont idempotentes, donc
relancer un mois (ou faire un backfill) donne toujours le même résultat.

```
ingest_reference ┐
ingest_bronze ───┴─► silver ─► quality_silver ─► gold ─► export_warehouse ─► create_views
```

- **bronze** : CSV brut de l'API SODA, partitionné par jour (MinIO)
- **silver** : Delta typé, validé, dédoublonné + table de rejets
- **gold** : tables de KPIs en Delta, recalculées sur tout le silver
- **exposition** : copie dans Postgres (`analytics.*`) + vues SQL pour la BI
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

JOBS_DIR = "/opt/airflow/src/taxi_pipeline/jobs"
REFERENCE_FILE = "/opt/airflow/data/reference/community_areas.csv"
WINDOW_ARGS = ["--start", "{{ data_interval_start | ds }}", "--end", "{{ data_interval_end | ds }}"]

# Période traitée : T1 2023 par défaut (≈ 1,45 M trajets).
START = pendulum.parse(os.getenv("PIPELINE_START_DATE", "2023-01-01"), tz="UTC")
END = pendulum.parse(os.getenv("PIPELINE_END_DATE", "2023-03-31"), tz="UTC")


def spark_job(task_id: str, script: str, application_args: list[str] | None = None) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_local",
        application=f"{JOBS_DIR}/{script}",
        application_args=application_args,
        name=f"taxi_{task_id}",
        driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        # Une seule session Spark à la fois sur la machine pour maîtriser la RAM.
        pool="spark",
    )


with DAG(
    dag_id="chicago_taxi_medallion",
    description="Chicago Taxi Trips : SODA -> bronze -> silver -> gold -> Postgres",
    doc_md=__doc__,
    start_date=START,
    end_date=END,
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["chicago-taxi", "medallion", "spark"],
) as dag:

    @task
    def ingest_reference() -> str:
        # Imports dans la tâche : le parsing du DAG par le scheduler reste léger.
        from pathlib import Path

        from taxi_pipeline.ingestion.bronze import ingest_reference as run

        return run(Path(REFERENCE_FILE))

    @task(execution_timeout=timedelta(hours=1))
    def ingest_bronze(data_interval_start=None, data_interval_end=None) -> dict:
        from taxi_pipeline.ingestion.bronze import ingest_trips

        return ingest_trips(data_interval_start.date(), data_interval_end.date())

    silver = spark_job("silver", "silver_job.py", WINDOW_ARGS)
    quality = spark_job("quality_silver", "quality_job.py", WINDOW_ARGS)
    gold = spark_job("gold", "gold_job.py")
    export = spark_job("export_warehouse", "export_job.py")

    views = SQLExecuteQueryOperator(
        task_id="create_views",
        conn_id="warehouse",
        sql="views.sql",
    )

    [ingest_reference(), ingest_bronze()] >> silver >> quality >> gold >> export >> views
