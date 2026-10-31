"""Script de téléchargement, utilisable hors Airflow pour rejouer une ingestion.

Exemple (depuis un conteneur Airflow) :
    python -m taxi_pipeline.ingestion --start 2023-01-01 --end 2023-04-01
"""

import argparse
import logging
from datetime import date
from pathlib import Path

from taxi_pipeline.ingestion.bronze import ingest_reference, ingest_trips


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion SODA -> bronze")
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="inclus (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="exclu (YYYY-MM-DD)")
    parser.add_argument(
        "--reference",
        type=Path,
        default=Path("/opt/airflow/data/reference/community_areas.csv"),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    ingest_reference(args.reference)
    print(ingest_trips(args.start, args.end))


if __name__ == "__main__":
    main()
