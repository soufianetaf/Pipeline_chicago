#!/bin/bash
# Exécuté une seule fois, à la création du volume Postgres.
# Une seule instance, trois bases isolées avec chacune son propre utilisateur :
#   airflow   : métadonnées Airflow (créée par l'image via POSTGRES_DB)
#   warehouse : couche d'exposition SQL (schéma analytics)
#   metabase  : base applicative de Metabase
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ${WAREHOUSE_USER} WITH PASSWORD '${WAREHOUSE_PASSWORD}';
    CREATE DATABASE warehouse OWNER ${WAREHOUSE_USER};

    CREATE USER ${METABASE_DB_USER} WITH PASSWORD '${METABASE_DB_PASSWORD}';
    CREATE DATABASE metabase OWNER ${METABASE_DB_USER};
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname warehouse <<-EOSQL
    CREATE SCHEMA analytics AUTHORIZATION ${WAREHOUSE_USER};
EOSQL
