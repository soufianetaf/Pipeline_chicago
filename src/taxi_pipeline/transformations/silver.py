"""Bronze → silver : typage, normalisation, validation, dédoublonnage.

Les fonctions sont pures (DataFrame en entrée, DataFrame en sortie) : aucune
lecture ni écriture ici. Elles se testent donc avec de petits DataFrames en
mémoire, sans stockage objet (voir tests/).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
MONEY = DecimalType(10, 2)  # jamais de flottant pour des montants

# Seuils métier au-delà desquels une valeur est jugée aberrante.
MAX_TRIP_SECONDS = 24 * 3600
MAX_TRIP_MILES = 500
MAX_TRIP_TOTAL = 1000

UNKNOWN = "Unknown"

SILVER_COLUMNS = [
    "trip_id",
    "taxi_id",
    "trip_start_ts",
    "trip_end_ts",
    "trip_date",
    "pickup_hour",
    "day_of_week",
    "trip_seconds",
    "trip_minutes",
    "trip_miles",
    "pickup_community_area",
    "dropoff_community_area",
    "pickup_census_tract",
    "dropoff_census_tract",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "payment_type",
    "company",
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_latitude",
    "dropoff_longitude",
    "_source_file",
]


def _clean_label(column: str) -> Column:
    value = F.trim(F.col(column))
    return F.when(value.isNull() | (value == ""), F.lit(UNKNOWN)).otherwise(value)


def parse_types(raw: DataFrame) -> DataFrame:
    """Convertit les colonnes texte du bronze en types métier.

    Une valeur non convertible devient NULL ; les règles de validation
    ci-dessous la rattrapent pour les colonnes obligatoires.
    Les colonnes *_location (WKT redondant avec lat/lon) sont abandonnées.
    """
    return raw.select(
        F.trim("trip_id").alias("trip_id"),
        F.trim("taxi_id").alias("taxi_id"),
        F.to_timestamp("trip_start_timestamp", TS_FORMAT).alias("trip_start_ts"),
        F.to_timestamp("trip_end_timestamp", TS_FORMAT).alias("trip_end_ts"),
        F.col("trip_seconds").cast("int").alias("trip_seconds"),
        F.col("trip_miles").cast("double").alias("trip_miles"),
        F.col("pickup_census_tract"),
        F.col("dropoff_census_tract"),
        F.col("pickup_community_area").cast("int").alias("pickup_community_area"),
        F.col("dropoff_community_area").cast("int").alias("dropoff_community_area"),
        *[F.col(c).cast(MONEY).alias(c) for c in ("fare", "tips", "tolls", "extras", "trip_total")],
        _clean_label("payment_type").alias("payment_type"),
        _clean_label("company").alias("company"),
        F.col("pickup_centroid_latitude").cast("double").alias("pickup_latitude"),
        F.col("pickup_centroid_longitude").cast("double").alias("pickup_longitude"),
        F.col("dropoff_centroid_latitude").cast("double").alias("dropoff_latitude"),
        F.col("dropoff_centroid_longitude").cast("double").alias("dropoff_longitude"),
        F.col("trip_start_date").cast("date").alias("trip_start_date"),
        F.col("_source_file"),
    )


def rejection_rules() -> list[tuple[str, Column]]:
    """Règles évaluées dans l'ordre : la première qui échoue donne le motif de rejet.

    Fonction (et non constante) : une Column ne peut être créée qu'avec une
    SparkSession active, pas à l'import du module.
    """
    return [
        ("missing_trip_id", F.col("trip_id").isNull() | (F.col("trip_id") == "")),
        ("invalid_start_timestamp", F.col("trip_start_ts").isNull()),
        ("start_outside_partition", F.to_date("trip_start_ts") != F.col("trip_start_date")),
        (
            "invalid_end_timestamp",
            F.col("trip_end_ts").isNull() | (F.col("trip_end_ts") < F.col("trip_start_ts")),
        ),
        (
            "invalid_duration",
            F.col("trip_seconds").isNull()
            | (F.col("trip_seconds") < 0)
            | (F.col("trip_seconds") > MAX_TRIP_SECONDS),
        ),
        (
            "invalid_distance",
            F.col("trip_miles").isNull() | (F.col("trip_miles") < 0) | (F.col("trip_miles") > MAX_TRIP_MILES),
        ),
        (
            "invalid_amount",
            F.col("fare").isNull()
            | (F.col("fare") < 0)
            | F.col("trip_total").isNull()
            | (F.col("trip_total") < 0)
            | (F.col("trip_total") > MAX_TRIP_TOTAL),
        ),
    ]


def flag_rejections(df: DataFrame) -> DataFrame:
    """Ajoute `rejection_reason` (NULL si la ligne est valide).

    Les doublons de trip_id sont aussi marqués : on garde une ligne de façon
    déterministe (fichier source le plus récent) et on trace les autres.
    """
    reason = F.lit(None).cast("string")
    for name, condition in reversed(rejection_rules()):
        reason = F.when(condition, F.lit(name)).otherwise(reason)
    flagged = df.withColumn("rejection_reason", reason)

    rank = Window.partitionBy("trip_id", F.col("rejection_reason").isNull()).orderBy(
        F.col("_source_file").desc()
    )
    return (
        flagged.withColumn("_rank", F.row_number().over(rank))
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("rejection_reason").isNull() & (F.col("_rank") > 1), F.lit("duplicate_trip_id")
            ).otherwise(F.col("rejection_reason")),
        )
        .drop("_rank")
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("trip_date", F.to_date("trip_start_ts"))
        .withColumn("pickup_hour", F.hour("trip_start_ts"))
        # ISO 8601 : 1 = lundi … 7 = dimanche (Spark : 1 = dimanche)
        .withColumn("day_of_week", ((F.dayofweek("trip_start_ts") + 5) % 7) + 1)
        .withColumn("trip_minutes", F.round(F.col("trip_seconds") / 60.0, 2))
    )


def bronze_to_silver(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Renvoie (lignes valides au format silver, lignes rejetées avec motif)."""
    flagged = flag_rejections(parse_types(raw))

    valid = (
        flagged.filter(F.col("rejection_reason").isNull())
        .transform(add_derived_columns)
        .select(*SILVER_COLUMNS)
    )
    rejected = flagged.filter(F.col("rejection_reason").isNotNull())
    return valid, rejected
