from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import SparkSession

from taxi_pipeline.schemas import SOURCE_COLUMNS


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def raw_trip(**overrides) -> dict:
    """Une ligne bronze valide (tout en texte, comme le CSV source)."""
    row = {c: None for c in SOURCE_COLUMNS}
    row.update(
        trip_id="t1",
        taxi_id="taxi-a",
        trip_start_timestamp="2023-01-02T08:00:00.000",
        trip_end_timestamp="2023-01-02T08:15:00.000",
        trip_seconds="900",
        trip_miles="3.2",
        pickup_community_area="8",
        dropoff_community_area="32",
        fare="12.50",
        tips="2.00",
        tolls="0",
        extras="1",
        trip_total="15.50",
        payment_type=" Credit Card ",
        company="Flash Cab",
        pickup_centroid_latitude="41.89",
        pickup_centroid_longitude="-87.62",
        trip_start_date=date(2023, 1, 2),
        _source_file="s3a://datalake/bronze/taxi_trips/trip_start_date=2023-01-02/part-00000.csv",
    )
    row.update(overrides)
    return row


@pytest.fixture
def make_raw(spark):
    def _make(*rows: dict):
        columns = [*SOURCE_COLUMNS, "trip_start_date", "_source_file"]
        schema = ", ".join(
            f"{c} {'date' if c == 'trip_start_date' else 'string'}" for c in columns
        )
        return spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], schema)

    return _make
