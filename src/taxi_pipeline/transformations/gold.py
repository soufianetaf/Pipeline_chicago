"""Silver → gold : tables analytiques prêtes pour la BI (fonctions pures)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

OUTSIDE_CHICAGO = "Hors Chicago / non renseigné"
DAY_NAMES = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]


def _share_of_total(column: str) -> Column:
    # Fenêtre sans partition : acceptable ici car la table agrégée est minuscule.
    return F.round(F.col(column) / F.sum(column).over(Window.partitionBy()), 4)


def daily_kpis(trips: DataFrame) -> DataFrame:
    """Revenus, volumes et durées par jour."""
    return (
        trips.groupBy("trip_date")
        .agg(
            F.count("*").alias("nb_trips"),
            F.countDistinct("taxi_id").alias("active_taxis"),
            F.sum("trip_total").alias("total_revenue"),
            F.sum("fare").alias("total_fare"),
            F.sum("tips").alias("total_tips"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
            F.percentile_approx("trip_minutes", 0.5).alias("median_trip_minutes"),
            F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"),
        )
        .withColumn("tip_rate", F.round((F.col("total_tips") / F.col("total_fare")).cast("double"), 4))
    )


def pickup_zones(trips: DataFrame, community_areas: DataFrame) -> DataFrame:
    """Activité par zone de prise en charge, avec le nom de la zone et son rang."""
    by_zone = trips.groupBy("pickup_community_area").agg(
        F.count("*").alias("nb_trips"),
        F.sum("trip_total").alias("total_revenue"),
        F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
        F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
    )
    areas = community_areas.select(
        F.col("community_area").cast("int").alias("area_id"), F.col("community_name")
    )
    return (
        # Référentiel de 77 lignes : broadcast pour éviter un shuffle de jointure.
        by_zone.join(F.broadcast(areas), by_zone.pickup_community_area == areas.area_id, "left")
        .select(
            F.col("pickup_community_area").alias("community_area"),
            F.coalesce("community_name", F.lit(OUTSIDE_CHICAGO)).alias("community_name"),
            "nb_trips",
            _share_of_total("nb_trips").alias("share_of_trips"),
            "total_revenue",
            "avg_trip_total",
            "avg_trip_minutes",
        )
        .withColumn("rank", F.dense_rank().over(Window.orderBy(F.col("nb_trips").desc())))
    )


def hourly_demand(trips: DataFrame) -> DataFrame:
    """Demande moyenne par jour de semaine et heure (base d'une heatmap)."""
    return (
        trips.groupBy("day_of_week", "pickup_hour")
        .agg(
            F.count("*").alias("nb_trips"),
            F.countDistinct("trip_date").alias("nb_days"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
        )
        .withColumn("avg_trips_per_day", F.round(F.col("nb_trips") / F.col("nb_days"), 1))
        .withColumn("day_name", F.element_at(F.array(*map(F.lit, DAY_NAMES)), F.col("day_of_week")))
    )


def payment_breakdown(trips: DataFrame) -> DataFrame:
    """Répartition des trajets et des pourboires par moyen de paiement."""
    return (
        trips.groupBy("payment_type")
        .agg(
            F.count("*").alias("nb_trips"),
            F.sum("trip_total").alias("total_revenue"),
            F.sum("tips").alias("total_tips"),
            F.sum("fare").alias("total_fare"),
        )
        .withColumn("share_of_trips", _share_of_total("nb_trips"))
        .withColumn("tip_rate", F.round((F.col("total_tips") / F.col("total_fare")).cast("double"), 4))
    )
