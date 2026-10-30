"""Contrats de données : colonnes attendues de la source et schémas Spark."""

from pyspark.sql.types import StringType, StructField, StructType

# Colonnes exactes renvoyées par l'export CSV de l'API SODA (dataset wrvz-psew).
# L'ingestion vérifie l'en-tête contre cette liste pour détecter une dérive de schéma
# côté source avant d'écrire quoi que ce soit dans le lac.
SOURCE_COLUMNS: tuple[str, ...] = (
    "trip_id",
    "taxi_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "trip_miles",
    "pickup_census_tract",
    "dropoff_census_tract",
    "pickup_community_area",
    "dropoff_community_area",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "payment_type",
    "company",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "pickup_centroid_location",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
    "dropoff_centroid_location",
)

# Le bronze est relu en chaînes de caractères : le typage est une responsabilité
# du silver, où chaque échec de conversion est tracé au lieu d'être silencieux.
BRONZE_TRIPS_SCHEMA = StructType([StructField(c, StringType(), True) for c in SOURCE_COLUMNS])

COMMUNITY_AREAS_SCHEMA = StructType(
    [
        StructField("community_area", StringType(), False),
        StructField("community_name", StringType(), False),
    ]
)
