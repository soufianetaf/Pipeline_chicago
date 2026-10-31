"""Client minimal pour l'API SODA (Socrata Open Data API)."""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)


class SchemaDriftError(RuntimeError):
    """Levée quand l'en-tête renvoyé par la source ne correspond plus au contrat."""


@dataclass(frozen=True)
class Page:
    content: bytes
    rows: int
    offset: int


def count_csv_rows(content: bytes) -> tuple[list[str], int]:
    """Renvoie (en-tête, nombre de lignes de données) d'un CSV.

    On passe par le module csv plutôt que de compter les retours à la ligne :
    un champ entre guillemets peut légalement contenir un saut de ligne.
    """
    reader = csv.reader(io.StringIO(content.decode("utf-8")))
    header = next(reader, [])
    return header, sum(1 for _ in reader)


class SodaClient:
    def __init__(
        self,
        base_url: str,
        dataset_id: str,
        expected_columns: tuple[str, ...],
        app_token: str | None = None,
        page_size: int = 50_000,
        timeout: tuple[int, int] = (10, 300),
    ) -> None:
        self.url = f"{base_url.rstrip('/')}/resource/{dataset_id}.csv"
        self.expected_columns = list(expected_columns)
        self.page_size = page_size
        self.timeout = timeout

        # Retries HTTP avec backoff exponentiel : l'API publique renvoie
        # régulièrement des 429 (throttling) ou des 5xx transitoires.
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        # Un app token (gratuit) relève les quotas ; il n'est jamais versionné.
        if app_token:
            self.session.headers["X-App-Token"] = app_token

    def iter_day_pages(self, day: date) -> Iterator[Page]:
        """Itère sur les pages CSV des trajets démarrés pendant `day`.

        Le découpage par jour garde des requêtes rapides (l'API est lente sur les
        gros offsets) et donne une unité naturelle d'idempotence. Le tri sur
        (trip_start_timestamp, trip_id) rend la pagination par offset stable.
        """
        next_day = day + timedelta(days=1)
        where = (
            f"trip_start_timestamp >= '{day.isoformat()}T00:00:00' "
            f"AND trip_start_timestamp < '{next_day.isoformat()}T00:00:00'"
        )
        offset = 0
        while True:
            params = {
                "$where": where,
                "$order": "trip_start_timestamp,trip_id",
                "$limit": self.page_size,
                "$offset": offset,
            }
            response = self.session.get(self.url, params=params, timeout=self.timeout)
            response.raise_for_status()

            header, rows = count_csv_rows(response.content)
            if header != self.expected_columns:
                raise SchemaDriftError(
                    f"En-tête inattendu pour {day}: {header} (attendu {self.expected_columns})"
                )

            log.info("SODA %s offset=%d -> %d lignes", day, offset, rows)
            yield Page(content=response.content, rows=rows, offset=offset)

            if rows < self.page_size:
                return
            offset += self.page_size
