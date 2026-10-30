"""Configuration centralisée du pipeline.

Toute la configuration vient des variables d'environnement (principe 12-factor) :
le même code tourne en local, en CI ou sur un cloud, seul l'environnement change.
Les valeurs par défaut correspondent à la stack docker-compose locale.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    # Stockage objet (MinIO, compatible S3)
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    bucket: str

    # Source : API SODA du portail open data de Chicago
    soda_base_url: str
    soda_dataset_id: str
    soda_app_token: str | None
    soda_page_size: int
    soda_max_workers: int

    # Entrepôt SQL exposé à la BI
    warehouse_jdbc_url: str
    warehouse_user: str
    warehouse_password: str
    warehouse_schema: str

    # Qualité : part maximale de lignes rejetées tolérée en silver
    max_rejected_ratio: float

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            s3_endpoint=_env("S3_ENDPOINT", "http://minio:9000"),
            s3_access_key=_env("S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key=_env("S3_SECRET_KEY", "minioadmin123"),
            bucket=_env("DATALAKE_BUCKET", "datalake"),
            soda_base_url=_env("SODA_BASE_URL", "https://data.cityofchicago.org"),
            soda_dataset_id=_env("SODA_DATASET_ID", "wrvz-psew"),
            soda_app_token=os.getenv("SODA_APP_TOKEN") or None,
            soda_page_size=int(_env("SODA_PAGE_SIZE", "50000")),
            soda_max_workers=int(_env("SODA_MAX_WORKERS", "4")),
            warehouse_jdbc_url=_env("WAREHOUSE_JDBC_URL", "jdbc:postgresql://postgres:5432/warehouse"),
            warehouse_user=_env("WAREHOUSE_USER", "analytics"),
            warehouse_password=_env("WAREHOUSE_PASSWORD", "analytics"),
            warehouse_schema=_env("WAREHOUSE_SCHEMA", "analytics"),
            max_rejected_ratio=float(_env("MAX_REJECTED_RATIO", "0.05")),
        )

    # --- Chemins du datalake -------------------------------------------------
    # Clés objet (boto3) et URI Spark (s3a://) partagent la même arborescence.

    def lake_key(self, layer: str, table: str) -> str:
        return f"{layer}/{table}"

    def lake_uri(self, layer: str, table: str) -> str:
        return f"s3a://{self.bucket}/{self.lake_key(layer, table)}"


# Noms des couches et des tables : une seule source de vérité.
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
QUALITY = "quality"

BRONZE_TRIPS = "taxi_trips"
BRONZE_COMMUNITY_AREAS = "reference/community_areas"
SILVER_TRIPS = "taxi_trips"
SILVER_REJECTED = "taxi_trips_rejected"
QUALITY_RESULTS = "check_results"
GOLD_TABLES = ("daily_kpis", "pickup_zones", "hourly_demand", "payment_breakdown")
