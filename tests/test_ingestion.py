from datetime import date

from taxi_pipeline.ingestion.bronze import iter_days
from taxi_pipeline.ingestion.soda import count_csv_rows


def test_count_csv_rows_handles_quoted_newlines():
    content = b'"trip_id","company"\n"a","Flash\nCab"\n"b","Globe"\n'
    header, rows = count_csv_rows(content)
    assert header == ["trip_id", "company"]
    assert rows == 2


def test_count_csv_rows_header_only():
    assert count_csv_rows(b'"trip_id"\n') == (["trip_id"], 0)


def test_iter_days_is_half_open():
    days = iter_days(date(2023, 1, 30), date(2023, 2, 2))
    assert days == [date(2023, 1, 30), date(2023, 1, 31), date(2023, 2, 1)]
