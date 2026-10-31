from datetime import date

import pytest

from conftest import raw_trip
from taxi_pipeline.quality.checks import DataQualityError, raise_on_errors, run_silver_checks
from taxi_pipeline.transformations.silver import bronze_to_silver


def checks_by_name(results):
    return {r.check: r for r in results}


def test_clean_day_passes(make_raw):
    valid, rejected = bronze_to_silver(make_raw(raw_trip(trip_id="a"), raw_trip(trip_id="b")))
    results = run_silver_checks(valid, rejected, date(2023, 1, 2), date(2023, 1, 3), max_rejected_ratio=0.05)

    assert all(r.passed for r in results)
    raise_on_errors(results)


def test_too_many_rejections_blocks_the_pipeline(make_raw):
    valid, rejected = bronze_to_silver(make_raw(raw_trip(trip_id="a"), raw_trip(trip_id="b", fare="-3")))
    results = run_silver_checks(valid, rejected, date(2023, 1, 2), date(2023, 1, 3), max_rejected_ratio=0.05)

    assert checks_by_name(results)["rejected_ratio"].observed == 0.5
    with pytest.raises(DataQualityError, match="rejected_ratio"):
        raise_on_errors(results)


def test_missing_days_is_only_a_warning(make_raw):
    valid, rejected = bronze_to_silver(make_raw(raw_trip()))
    results = run_silver_checks(valid, rejected, date(2023, 1, 1), date(2023, 1, 8), max_rejected_ratio=0.05)

    assert not checks_by_name(results)["days_complete"].passed
    raise_on_errors(results)  # ne lève pas
