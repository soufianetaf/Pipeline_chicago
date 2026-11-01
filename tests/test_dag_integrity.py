"""Le DAG se charge sans erreur et a la forme attendue (test classique en CI)."""

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_pipeline_structure(dagbag):
    dag = dagbag.dags.get("chicago_taxi_medallion")
    assert dag is not None
    assert dag.catchup and dag.max_active_runs == 1
    assert dag.get_task("silver").upstream_task_ids == {"ingest_bronze", "ingest_reference"}
    assert dag.get_task("gold").upstream_task_ids == {"quality_silver"}
    assert dag.get_task("create_views").upstream_task_ids == {"export_warehouse"}
