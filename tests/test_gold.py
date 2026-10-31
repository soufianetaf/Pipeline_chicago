from decimal import Decimal

import pytest

from conftest import raw_trip
from taxi_pipeline.transformations import gold
from taxi_pipeline.transformations.silver import bronze_to_silver


@pytest.fixture
def trips(make_raw):
    rows = [
        raw_trip(trip_id="a", pickup_community_area="8", fare="10", tips="2", trip_total="12"),
        raw_trip(trip_id="b", pickup_community_area="8", fare="20", tips="0", trip_total="20"),
        raw_trip(trip_id="c", pickup_community_area="32", fare="30", tips="4", trip_total="34"),
        raw_trip(trip_id="d", pickup_community_area=None, fare="40", tips="0", trip_total="40"),
    ]
    valid, _ = bronze_to_silver(make_raw(*rows))
    return valid


@pytest.fixture
def community_areas(spark):
    return spark.createDataFrame([("8", "Near North Side"), ("32", "Loop")], "community_area string, community_name string")


def test_daily_kpis(trips):
    row = gold.daily_kpis(trips).collect()[0]
    assert row["nb_trips"] == 4
    assert row["total_revenue"] == Decimal("106.00")
    assert row["tip_rate"] == pytest.approx(6 / 100)


def test_pickup_zones_are_named_ranked_and_keep_unknown_area(trips, community_areas):
    zones = {r["community_name"]: r for r in gold.pickup_zones(trips, community_areas).collect()}

    assert zones["Near North Side"]["nb_trips"] == 2
    assert zones["Near North Side"]["rank"] == 1
    assert zones["Near North Side"]["share_of_trips"] == 0.5
    assert zones[gold.OUTSIDE_CHICAGO]["community_area"] is None


def test_hourly_demand(trips):
    row = gold.hourly_demand(trips).collect()[0]
    assert (row["day_name"], row["pickup_hour"], row["nb_trips"]) == ("Lundi", 8, 4)
    assert row["avg_trips_per_day"] == 4.0


def test_payment_breakdown_shares_sum_to_one(trips):
    shares = [r["share_of_trips"] for r in gold.payment_breakdown(trips).collect()]
    assert sum(shares) == pytest.approx(1.0)
