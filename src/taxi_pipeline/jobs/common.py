"""Utilitaires communs aux points d'entrée spark-submit."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Window:
    """Intervalle de traitement semi-ouvert [start, end), aligné sur data_interval d'Airflow."""

    start: date
    end: date

    def sql_filter(self, column: str) -> str:
        return f"{column} >= '{self.start.isoformat()}' AND {column} < '{self.end.isoformat()}'"


def parse_window(argv: list[str] | None = None, description: str = "") -> Window:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="inclus (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="exclu (YYYY-MM-DD)")
    args = parser.parse_args(argv)
    if args.start >= args.end:
        parser.error("--start doit être strictement antérieur à --end")
    return Window(args.start, args.end)


def setup_logging() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    return logging.getLogger("taxi_pipeline")
