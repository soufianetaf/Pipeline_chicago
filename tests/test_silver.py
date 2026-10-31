from datetime import date
from decimal import Decimal

import pytest

from conftest import raw_trip
from taxi_pipeline.transformations.silver import SILVER_COLUMNS, bronze_to_silver


def reasons(rejected):
    return {r["trip_id"]: r["rejection_reason"] for r in rejected.collect()}


def test_valid_trip_is_typed_and_enriched(make_raw):
    valid, rejected = bronze_to_silver(make_raw(raw_trip()))

    assert rejected.count() == 0
    row = valid.collect()[0]
    assert valid.columns == SILVER_COLUMNS
    assert row["trip_date"] == date(2023, 1, 2)
    assert row["pickup_hour"] == 8
    assert row["day_of_week"] == 1  # 2 janvier 2023 = lundi
    assert row["trip_minutes"] == 15.0
    assert row["fare"] == Decimal("12.50")
    assert row["payment_type"] == "Credit Card"  # espaces supprimés


def test_missing_labels_become_unknown(make_raw):
    valid, _ = bronze_to_silver(make_raw(raw_trip(payment_type="  ", company=None)))
    row = valid.collect()[0]
    assert (row["payment_type"], row["company"]) == ("Unknown", "Unknown")


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"trip_id": None}, "missing_trip_id"),
        ({"trip_start_timestamp": "not-a-date"}, "invalid_start_timestamp"),
        ({"trip_start_date": date(2023, 1, 3)}, "start_outside_partition"),
        ({"trip_end_timestamp": "2023-01-02T07:00:00.000"}, "invalid_end_timestamp"),
        ({"trip_seconds": "-5"}, "invalid_duration"),
        ({"trip_miles": "9999"}, "invalid_distance"),
        ({"fare": "-1"}, "invalid_amount"),
        ({"trip_total": "5000"}, "invalid_amount"),
    ],
)
def test_invalid_trips_are_rejected_with_reason(make_raw, overrides, reason):
    valid, rejected = bronze_to_silver(make_raw(raw_trip(**{"trip_id": "bad", **overrides})))

    assert valid.count() == 0
    assert list(reasons(rejected).values()) == [reason]


def test_duplicates_keep_one_row_and_trace_the_others(make_raw):
    older = raw_trip(_source_file="s3a://x/part-00000.csv")
    newer = raw_trip(_source_file="s3a://x/part-00001.csv")

    valid, rejected = bronze_to_silver(make_raw(older, newer))

    assert [r["_source_file"] for r in valid.collect()] == ["s3a://x/part-00001.csv"]
    assert [r["rejection_reason"] for r in rejected.collect()] == ["duplicate_trip_id"]


def test_no_row_is_lost(make_raw):
    rows = [raw_trip(trip_id=f"t{i}") for i in range(5)] + [raw_trip(trip_id="bad", fare="abc")]
    valid, rejected = bronze_to_silver(make_raw(*rows))
    assert valid.count() + rejected.count() == len(rows)
