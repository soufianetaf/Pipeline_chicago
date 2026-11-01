-- Vues de la couche d'exposition, (re)créées à chaque run par le DAG.
-- Elles donnent à la BI des requêtes prêtes à l'emploi au-dessus des tables gold.
-- DROP puis CREATE (et non CREATE OR REPLACE seul) : Postgres refuse de changer
-- le type d'une colonne de vue existante. Le tout s'exécute dans une transaction.

DROP VIEW IF EXISTS analytics.v_top10_pickup_zones;
DROP VIEW IF EXISTS analytics.v_monthly_kpis;
DROP VIEW IF EXISTS analytics.v_latest_quality_checks;

CREATE OR REPLACE VIEW analytics.v_top10_pickup_zones AS
-- Rang recalculé sans la zone "hors Chicago" pour un classement continu (1..10).
SELECT
    ROW_NUMBER() OVER (ORDER BY nb_trips DESC) AS rank,
    community_area, community_name, nb_trips, share_of_trips, total_revenue, avg_trip_total
FROM analytics.pickup_zones
WHERE community_area IS NOT NULL
ORDER BY nb_trips DESC
LIMIT 10;

CREATE OR REPLACE VIEW analytics.v_monthly_kpis AS
SELECT
    date_trunc('month', trip_date)::date                      AS month,
    SUM(nb_trips)                                             AS nb_trips,
    SUM(total_revenue)                                        AS total_revenue,
    ROUND(SUM(total_revenue) / SUM(nb_trips), 2)              AS avg_revenue_per_trip,
    ROUND((SUM(avg_trip_minutes * nb_trips) / SUM(nb_trips))::numeric, 2) AS avg_trip_minutes
FROM analytics.daily_kpis
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW analytics.v_latest_quality_checks AS
SELECT DISTINCT ON (window_start, "check")
    window_start, window_end, "check", severity, passed, observed, expected, checked_at
FROM analytics.data_quality_checks
ORDER BY window_start, "check", checked_at DESC;
