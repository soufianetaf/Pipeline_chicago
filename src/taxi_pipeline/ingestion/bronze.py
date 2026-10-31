"""Ingestion de la source vers la couche bronze.

Règles du bronze :
- les données sont stockées telles que renvoyées par la source (CSV, non modifiées) ;
- une partition par jour de départ : `trip_start_date=YYYY-MM-DD/` ;
- chaque partition est réécrite entièrement à chaque exécution (idempotence) ;
- un `_manifest.json` trace l'origine et le volume de chaque partition (lineage).
  Il est écrit en dernier et sert de marqueur de complétude.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from taxi_pipeline.config import BRONZE, BRONZE_COMMUNITY_AREAS, BRONZE_TRIPS, Settings
from taxi_pipeline.ingestion.soda import SodaClient
from taxi_pipeline.ingestion.storage import ObjectStore
from taxi_pipeline.schemas import SOURCE_COLUMNS

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DayResult:
    day: str
    rows: int
    pages: int


def iter_days(start: date, end: date) -> list[date]:
    """Jours de l'intervalle semi-ouvert [start, end), convention Airflow."""
    return [start + timedelta(days=i) for i in range((end - start).days)]


def ingest_day(day: date, client: SodaClient, store: ObjectStore, settings: Settings) -> DayResult:
    prefix = f"{settings.lake_key(BRONZE, BRONZE_TRIPS)}/trip_start_date={day.isoformat()}/"

    # Idempotence : on repart d'une partition vide, une relance ne crée pas de doublons.
    store.delete_prefix(prefix)

    rows = pages = 0
    for page in client.iter_day_pages(day):
        store.put_bytes(f"{prefix}part-{pages:05d}.csv", page.content)
        rows += page.rows
        pages += 1

    store.put_json(
        f"{prefix}_manifest.json",
        {
            "source": client.url,
            "trip_start_date": day.isoformat(),
            "rows": rows,
            "pages": pages,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    log.info("Bronze %s : %d lignes en %d page(s)", day, rows, pages)
    return DayResult(day=day.isoformat(), rows=rows, pages=pages)


def ingest_trips(start: date, end: date, settings: Settings | None = None) -> dict:
    """Ingère les trajets démarrés dans [start, end) et renvoie un résumé."""
    settings = settings or Settings.from_env()
    store = ObjectStore(settings)
    store.ensure_bucket()
    client = SodaClient(
        base_url=settings.soda_base_url,
        dataset_id=settings.soda_dataset_id,
        expected_columns=SOURCE_COLUMNS,
        app_token=settings.soda_app_token,
        page_size=settings.soda_page_size,
    )

    # Les appels HTTP dominent le temps d'ingestion : on parallélise par jour,
    # avec un nombre de workers limité pour rester correct envers l'API publique.
    days = iter_days(start, end)
    with ThreadPoolExecutor(max_workers=settings.soda_max_workers) as pool:
        results = list(pool.map(lambda d: ingest_day(d, client, store, settings), days))

    summary = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": len(results),
        "rows": sum(r.rows for r in results),
        "empty_days": [r.day for r in results if r.rows == 0],
    }
    log.info("Résumé ingestion : %s", summary)
    if not results or summary["rows"] == 0:
        raise RuntimeError(f"Aucune donnée ingérée pour [{start}, {end})")
    return summary


def ingest_reference(reference_file: Path, settings: Settings | None = None) -> str:
    """Dépose le référentiel des community areas (fichier versionné) dans le bronze."""
    settings = settings or Settings.from_env()
    store = ObjectStore(settings)
    store.ensure_bucket()
    key = f"{settings.lake_key(BRONZE, BRONZE_COMMUNITY_AREAS)}/{reference_file.name}"
    store.put_bytes(key, reference_file.read_bytes())
    return key
