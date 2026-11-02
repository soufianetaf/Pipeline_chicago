# Chicago Taxi Trips : pipeline médaillon (Airflow + PySpark + Delta Lake)

Pipeline de données de bout en bout qui :

1. **ingère** les trajets de taxi de Chicago depuis l'API SODA vers un datalake (MinIO) ;
2. les **nettoie et transforme** avec PySpark en tables Delta (bronze → silver → gold) ;
3. **expose** des KPIs prêts pour la BI dans Postgres, consultables dans Metabase.

Le tout est orchestré par Airflow et tourne en local avec une seule commande Docker Compose.

> L'énoncé original est dans [docs/ENONCE.md](docs/ENONCE.md).

---

## Sommaire

1. [Démarrage rapide](#1-démarrage-rapide)
2. [Architecture](#2-architecture)
3. [Données sources et reproductibilité](#3-données-sources-et-reproductibilité)
4. [Les couches du datalake en détail](#4-les-couches-du-datalake-en-détail)
5. [Orchestration](#5-orchestration)
6. [Qualité des données](#6-qualité-des-données)
7. [Exposition et BI](#7-exposition-et-bi)
8. [Organisation du code](#8-organisation-du-code)
9. [Tests](#9-tests)
10. [Choix techniques et arbitrages](#10-choix-techniques-et-arbitrages)
11. [Résultats obtenus](#11-résultats-obtenus)
12. [Avec plus de temps](#12-avec-plus-de-temps)

---

## 1. Démarrage rapide

### Prérequis

- **Docker Desktop** (ou Docker Engine) avec **Docker Compose v2**
- **8 Go de RAM** alloués à Docker (Spark en a besoin pour le driver), 4 CPU conseillés
- Ports libres : `8080` (Airflow), `9000`/`9001` (MinIO), `3000` (Metabase), `5432` (Postgres)
- Un accès Internet (API SODA + téléchargement des images)

### Lancement

```bash
# 1. (optionnel) personnaliser la configuration ; tout a une valeur par défaut
cp .env.example .env

# 2. construire l'image et démarrer la stack
docker compose up -d --build
```

C'est tout. Le DAG `chicago_taxi_medallion` **démarre automatiquement** : il est actif dès sa création
et `catchup=True` déclenche un run par mois de la période (janvier, février et mars 2023).

### Suivre l'exécution

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Console MinIO (datalake) | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Metabase (BI) | http://localhost:3000 | compte créé au premier accès |
| Postgres (entrepôt) | `localhost:5432`, base `warehouse` | `analytics` / `analytics` |

Comptez environ **5 minutes par mois**, soit 15 à 20 minutes pour les 3 mois. L'essentiel est le
téléchargement depuis l'API publique, et le premier `--build` ajoute quelques minutes.

### Commandes utiles

```bash
# tests unitaires (dans le conteneur, qui a Java + Spark)
docker compose exec airflow-scheduler python -m pytest

# relancer un mois précis (idempotent)
docker compose exec airflow-scheduler airflow tasks clear chicago_taxi_medallion -s 2023-02-01 -e 2023-02-01 -y

# ingestion seule, hors Airflow (script de téléchargement)
docker compose exec airflow-scheduler python -m taxi_pipeline.ingestion --start 2023-01-01 --end 2023-04-01

# interroger les KPIs
docker compose exec postgres psql -U analytics -d warehouse -c "select * from analytics.v_monthly_kpis"

# tout arrêter et supprimer les données
docker compose down -v
```

---

## 2. Architecture

```
                         ┌────────────────────────── Airflow (LocalExecutor) ──────────────────────────┐
                         │  DAG chicago_taxi_medallion : 1 run / mois, catchup, idempotent             │
                         └──────┬──────────────┬──────────────┬──────────────┬──────────────┬──────────┘
                                │              │              │              │              │
 API SODA   ── requests ──►  ingest_bronze   silver      quality_silver     gold     export_warehouse
 (CSV, paginé par jour)         │         (spark-submit)  (spark-submit) (spark-submit) (spark-submit)
                                ▼              ▼              ▼              ▼              ▼
          ┌─────────────────────────── MinIO : s3://datalake ──────────────────────┐   ┌───────────────┐
          │ bronze/taxi_trips/trip_start_date=YYYY-MM-DD/part-*.csv + _manifest     │   │ Postgres      │
          │ bronze/reference/community_areas/                                       │   │  warehouse    │
          │ silver/taxi_trips           (Delta, partition trip_date)                │──►│  analytics.*  │──► Metabase
          │ silver/taxi_trips_rejected  (Delta, lignes rejetées + motif)            │   │  + vues SQL   │
          │ quality/check_results       (Delta, journal des contrôles)              │   └───────────────┘
          │ gold/daily_kpis, pickup_zones, hourly_demand, payment_breakdown (Delta) │
          └─────────────────────────────────────────────────────────────────────────┘
```

| Conteneur | Rôle |
|---|---|
| `airflow-init` | migre la base Airflow, crée l'admin et le pool `spark`, puis s'arrête |
| `airflow-webserver` | interface Airflow |
| `airflow-scheduler` | planifie et **exécute** les tâches (LocalExecutor), y compris `spark-submit` |
| `minio` | stockage objet compatible S3, socle du datalake |
| `postgres` | 3 bases isolées : `airflow` (métadonnées), `warehouse` (exposition), `metabase` |
| `metabase` | dashboards BI branchés sur `warehouse` |

---

## 3. Données sources et reproductibilité

- **Dataset** : [Taxi Trips (2013-2023)](https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew), id `wrvz-psew`
- **Période choisie** : **T1 2023**, du 1er janvier au 31 mars 2023, soit **1 446 439 trajets** (≈ 600 Mo de CSV).
  Le volume est réaliste, tout en tournant sur un portable.
- **Endpoint** : `https://data.cityofchicago.org/resource/wrvz-psew.csv`

### Stratégie de téléchargement

Une requête **par jour**, paginée par blocs de 50 000 lignes :

```
GET /resource/wrvz-psew.csv
    ?$where=trip_start_timestamp >= '2023-01-15T00:00:00' AND trip_start_timestamp < '2023-01-16T00:00:00'
    &$order=trip_start_timestamp,trip_id
    &$limit=50000
    &$offset=0
```

Pourquoi ce découpage :

- l'API devient très lente avec de gros `$offset` et certains tris (`$order=:id` dépassait 2 minutes
  lors des essais) ; un jour représente environ 10 à 20 000 lignes, donc presque toujours une seule page ;
- le tri sur `(trip_start_timestamp, trip_id)` est **déterministe**, donc la pagination est stable ;
- le jour est l'unité d'**idempotence** : chaque jour est réécrit entièrement à chaque exécution ;
- les jours sont téléchargés **en parallèle** (4 threads par défaut, `SODA_MAX_WORKERS`) avec retries
  et backoff exponentiel sur les erreurs 429 et 5xx.

### Reproduire le téléchargement

```bash
docker compose exec airflow-scheduler python -m taxi_pipeline.ingestion --start 2023-01-01 --end 2023-04-01
```

Pour changer de période, modifiez `PIPELINE_START_DATE` / `PIPELINE_END_DATE` dans `.env`.
Un token applicatif gratuit (`SODA_APP_TOKEN`) relève les quotas de l'API, mais il est facultatif.

---

## 4. Les couches du datalake en détail

### Bronze : données brutes

`s3://datalake/bronze/taxi_trips/trip_start_date=YYYY-MM-DD/part-00000.csv`

- Le CSV est stocké **tel que renvoyé par l'API**, octet pour octet : aucune transformation.
  On peut toujours tout recalculer à partir du bronze, même si l'API change ou disparaît.
- **Contrat de schéma** : l'en-tête est comparé à la liste des colonnes attendues
  ([schemas.py](src/taxi_pipeline/schemas.py)). En cas de **dérive de schéma** côté source, l'ingestion
  échoue avant d'écrire quoi que ce soit.
- Un `_manifest.json` par partition indique la source, le nombre de lignes, de pages et la date d'ingestion.
  C'est le **lineage** au niveau fichier. Il est écrit en dernier et sert donc de marqueur de complétude.
- Le référentiel des **77 community areas** (numéro → nom), versionné dans
  [data/reference](data/reference/community_areas.csv), est déposé dans `bronze/reference/`.

### Silver : données nettoyées

`s3://datalake/silver/taxi_trips` (Delta, partitionné par `trip_date`)

Ce que fait [transformations/silver.py](src/taxi_pipeline/transformations/silver.py) :

| Étape | Détail |
|---|---|
| **Typage** | timestamps, entiers, `double` pour les distances et coordonnées, **`decimal(10,2)` pour les montants** (jamais de flottant pour de l'argent) |
| **Normalisation** | `trim` sur les libellés ; une valeur vide ou nulle de `payment_type` ou `company` devient `Unknown` ; les colonnes WKT `*_location`, redondantes avec lat/lon, sont supprimées |
| **Validation** | 7 règles métier (voir ci-dessous) ; une ligne invalide n'est **pas supprimée en silence** mais part dans `taxi_trips_rejected` avec son **motif** |
| **Dédoublonnage** | sur `trip_id`, de façon **déterministe** (on garde le fichier source le plus récent) ; les doublons sont tracés comme rejets `duplicate_trip_id` |
| **Enrichissement** | `trip_date`, `pickup_hour`, `day_of_week` (ISO, 1 = lundi), `trip_minutes` |
| **Lineage** | colonne `_source_file` : le fichier bronze d'origine de chaque ligne |

Règles de rejet, évaluées dans l'ordre (la première en échec donne le motif) :

| Motif | Condition |
|---|---|
| `missing_trip_id` | identifiant absent |
| `invalid_start_timestamp` | date de début absente ou illisible |
| `start_outside_partition` | la date de début ne correspond pas à la partition bronze |
| `invalid_end_timestamp` | date de fin absente ou antérieure au début |
| `invalid_duration` | durée absente, négative ou > 24 h |
| `invalid_distance` | distance absente, négative ou > 500 miles |
| `invalid_amount` | tarif ou total absent, négatif, ou total > 1 000 $ |

**Écriture idempotente** : `mode("overwrite")` + `replaceWhere` sur la fenêtre du run. Delta remplace
**atomiquement** les seules partitions du mois traité, sans toucher aux autres mois.
On garantit `lignes bronze = lignes silver + lignes rejetées` : aucune ligne n'est perdue.

### Gold : tables analytiques

`s3://datalake/gold/<table>` (Delta), recalculées à partir de **tout** le silver
([transformations/gold.py](src/taxi_pipeline/transformations/gold.py)) :

| Table | Grain | Contenu |
|---|---|---|
| `daily_kpis` | jour | nb trajets, taxis actifs, **revenus** (total, tarifs, pourboires), panier moyen, **durée moyenne et médiane**, distance moyenne, taux de pourboire |
| `pickup_zones` | zone de prise en charge | nb trajets, part, revenus, panier et durée moyens, **rang** ; jointure avec le référentiel des noms de zones (broadcast join) |
| `hourly_demand` | jour de semaine × heure | volume total et **moyen par jour**, panier et durée moyens (base d'un tableau croisé jour × heure) |
| `payment_breakdown` | moyen de paiement | nb trajets, part, revenus, taux de pourboire |

Le gold est entièrement recalculé à chaque run : les parts et les rangs dépendent de l'ensemble des données,
et le volume agrégé reste faible. C'est plus simple et plus sûr qu'une mise à jour incrémentale.

---

## 5. Orchestration

DAG [`chicago_taxi_medallion`](dags/chicago_taxi_pipeline.py) :

```
ingest_reference ┐
ingest_bronze ───┴─► silver ─► quality_silver ─► gold ─► export_warehouse ─► create_views
```

Bonnes pratiques Airflow appliquées :

- **Un run = une fenêtre de données** (`@monthly`, `data_interval_start` → `data_interval_end`) :
  chaque tâche reçoit `--start` / `--end` et ne traite que sa fenêtre. **Backfill** et relance d'un mois
  passé se font avec les commandes standard d'Airflow.
- **Idempotence** : relancer un mois redonne les mêmes données en bronze, silver, gold et Postgres (vérifié
  en relançant février). Seul le journal qualité grandit, car c'est un historique ; la vue
  `v_latest_quality_checks` n'en garde que le dernier état.
- **Retries** avec backoff exponentiel, `execution_timeout` sur l'ingestion.
- `max_active_runs=1` et un **pool `spark` à 1 slot** : une seule session Spark à la fois, pour maîtriser la RAM.
- **Airflow orchestre, il ne calcule pas** : les traitements lourds sont des scripts `spark-submit`
  autonomes (`SparkSubmitOperator`), exécutables et testables hors d'Airflow.
- Le fichier du DAG n'importe rien de lourd au niveau module : le parsing par le scheduler reste rapide.
- Les **connexions** (`spark_local`, `warehouse`) sont déclarées par variables d'environnement
  (`AIRFLOW_CONN_*`) : aucune configuration manuelle dans l'interface.
- `doc_md` : la documentation du DAG est visible directement dans l'interface Airflow.

---

## 6. Qualité des données

Après le silver, la tâche `quality_silver` ([quality/checks.py](src/taxi_pipeline/quality/checks.py))
calcule toutes les métriques **en une seule passe** sur les données :

| Contrôle | Sévérité | Règle |
|---|---|---|
| `row_count_positive` | ERROR | au moins une ligne |
| `trip_id_unique` | ERROR | aucun doublon de `trip_id` |
| `key_columns_not_null` | ERROR | aucune valeur nulle dans les colonnes clés |
| `rejected_ratio` | ERROR | rejets ≤ 5 % (`MAX_REJECTED_RATIO`) |
| `trip_total_gte_fare` | WARNING | total ≥ tarif pour 99,9 % des trajets |
| `days_complete` | WARNING | un jour présent pour chaque jour de la fenêtre |

- Un contrôle **ERROR** en échec fait **échouer la tâche** : le gold n'est pas recalculé.
  C'est un **circuit breaker** : on ne publie jamais de KPI faux à la BI.
- Tous les résultats sont **historisés** dans `quality/check_results` (Delta, en ajout) puis exposés
  dans `analytics.data_quality_checks` et la vue `v_latest_quality_checks`.

---

## 7. Exposition et BI

La tâche `export_warehouse` copie les tables gold et le journal qualité dans Postgres
(`warehouse.analytics.*`) via JDBC, en mode `overwrite` + `truncate` : les tables et les vues qui en
dépendent sont conservées. Puis `create_views` (re)crée des vues prêtes à l'emploi ([sql/views.sql](sql/views.sql)) :

- `v_top10_pickup_zones` : top 10 des zones de prise en charge
- `v_monthly_kpis` : revenus, volumes et durée moyenne par mois
- `v_latest_quality_checks` : dernier état de chaque contrôle qualité

### Brancher Metabase (2 minutes, une seule fois)

1. Ouvrir http://localhost:3000 et créer le compte administrateur.
2. *Ajouter une base de données* → PostgreSQL : hôte `postgres`, port `5432`, base `warehouse`,
   utilisateur `analytics`, mot de passe `analytics`.
3. Les tables du schéma `analytics` sont alors explorables. Idées de graphiques : revenus par jour
   (`daily_kpis`), tableau croisé jour × heure (`hourly_demand`), top zones (`v_top10_pickup_zones`).

Postgres est aussi accessible depuis n'importe quel client SQL (DBeaver, Power BI, Excel…) sur `localhost:5432`.

---

## 8. Organisation du code

```
.
├── dags/
│   └── chicago_taxi_pipeline.py      # DAG : orchestration uniquement, aucune logique métier
├── src/taxi_pipeline/                 # package Python, indépendant d'Airflow
│   ├── config.py                     # configuration par variables d'environnement + chemins du lac
│   ├── schemas.py                    # contrat de la source et schémas Spark
│   ├── spark.py                      # SparkSession (Delta + S3A) partagée
│   ├── ingestion/                    # API SODA → bronze (requests + boto3, sans Spark)
│   │   ├── soda.py                   #   client HTTP paginé avec retries et contrôle de schéma
│   │   ├── storage.py                #   accès MinIO / S3
│   │   ├── bronze.py                 #   écriture idempotente par jour + manifestes
│   │   └── __main__.py               #   script de téléchargement autonome (CLI)
│   ├── transformations/              # logique métier PURE (DataFrame → DataFrame), testée
│   │   ├── silver.py
│   │   └── gold.py
│   ├── quality/checks.py             # contrôles qualité
│   └── jobs/                         # points d'entrée spark-submit : lecture → transformation → écriture
│       ├── silver_job.py
│       ├── quality_job.py
│       ├── gold_job.py
│       └── export_job.py
├── tests/                            # pytest : transformations, qualité, ingestion, intégrité du DAG
├── sql/views.sql                     # vues de la couche d'exposition
├── data/reference/                   # référentiel versionné (community areas)
├── docker/
│   ├── airflow/Dockerfile            # Airflow + Java + PySpark + JAR Delta/S3A/Postgres
│   └── postgres/init-databases.sh    # création des bases warehouse et metabase
├── docker-compose.yml
├── .env.example
└── pyproject.toml                    # configuration pytest et ruff
```

Le principe clé est la **séparation des responsabilités** :

- `transformations/` contient la logique métier, sous forme de **fonctions pures** sans I/O, donc testables en mémoire ;
- `jobs/` gère les I/O : où lire, où écrire, comment (Delta, `replaceWhere`…) ;
- `dags/` décide quand lancer quoi et dans quel ordre.

On peut ainsi changer d'orchestrateur ou de stockage sans toucher à la logique métier.

---

## 9. Tests

```bash
docker compose exec airflow-scheduler python -m pytest
```

Les tests tournent dans le conteneur, qui fournit déjà Java et Spark. Ils couvrent :

- **silver** : typage, enrichissement, chaque règle de rejet, dédoublonnage déterministe, aucune ligne perdue ;
- **gold** : calcul des KPIs, jointure et rang des zones, parts qui somment à 1 ;
- **qualité** : le circuit breaker bloque bien, un warning ne bloque pas ;
- **ingestion** : comptage CSV robuste aux retours à la ligne entre guillemets, fenêtres de dates ;
- **DAG** : chargement sans erreur d'import et structure des dépendances (test classique de CI).

---

## 10. Choix techniques et arbitrages

| Choix | Pourquoi | Alternative écartée |
|---|---|---|
| **Airflow 2.10**, LocalExecutor | standard du marché (Composer, MWAA), notion native de `data_interval` pour l'idempotence et le backfill | Dagster (assets très adaptés au médaillon, mais moins répandu chez les clients) ; CeleryExecutor (Redis et workers inutiles en local) |
| **Spark en mode `local[*]`** dans le conteneur Airflow | ≈ 1,5 M lignes : un cluster n'apporterait que de la complexité et de la RAM consommée | cluster Spark master/worker dans Compose ; le code serait identique, seul le `master` change |
| **`SparkSubmitOperator`** plutôt que `PythonOperator` | la JVM Spark tourne dans un processus séparé du worker Airflow ; les jobs restent exécutables hors Airflow | `PythonOperator` qui crée une SparkSession dans le worker |
| **MinIO** (image `bitnamilegacy/minio`, version épinglée) | API S3 : le même code tourne sur AWS S3, GCS (connecteur) ou ADLS. MinIO ne publie plus d'images officielles sur Docker Hub ni quay.io, d'où la construction Bitnami archivée | système de fichiers local (pas représentatif d'un datalake) |
| **Delta Lake** pour silver et gold | transactions ACID, `replaceWhere` atomique (idempotence), schéma imposé, time travel pour l'audit | Parquet seul (pas d'atomicité, réécritures risquées) ; Iceberg (excellent, mais demande un catalogue en plus) |
| **CSV brut** en bronze | « données brutes telles quelles » : fidélité totale à la source, rejouabilité | conversion en Parquet dès le bronze, qui ferait perdre la donnée d'origine |
| **Ingestion en Python** (requests), sans Spark | goulot = réseau, pas CPU ; Spark n'apporterait rien pour des appels HTTP | ingestion Spark |
| **JAR téléchargés au build** de l'image | aucun accès Maven au runtime, donc exécutions reproductibles ; versions alignées (Spark 3.5 / Hadoop 3.3.4 / Delta 3.2) | `spark.jars.packages` (téléchargement à chaque démarrage) |
| **Postgres + Metabase** pour l'exposition | interface SQL universelle pour la BI + dashboards sans code, deux conteneurs légers | Trino sur le lac (plus élégant, mais beaucoup plus lourd) ; API REST |
| **Contrôles qualité maison** | 6 règles claires en une passe Spark, aucune dépendance supplémentaire | Great Expectations / Soda (pertinents à plus grande échelle) |
| **Gold recalculé entièrement** | correct par construction (rangs, parts), volume faible | `MERGE` incrémental, plus complexe et inutile à ce volume |
| **Rejets conservés** dans une table dédiée | aucune perte silencieuse, données auditables et rejouables | `filter()` qui supprime les lignes invalides sans trace |

Identifiants : toutes les valeurs par défaut sont des identifiants de **développement local**,
surchargeables via `.env` (non versionné). En production, ils viendraient d'un gestionnaire de secrets.

---

## 11. Résultats obtenus

Exécution complète en local sur le T1 2023 (3 runs mensuels, environ 5 minutes chacun) :

| | Janvier | Février | Mars | Total |
|---|---|---|---|---|
| Trajets valides (silver) | 413 969 | 446 274 | 584 602 | **1 444 845** |
| Revenus | 10,40 M$ | 10,99 M$ | 15,41 M$ | **36,80 M$** |
| Panier moyen | 25,12 $ | 24,62 $ | 26,36 $ | |
| Durée moyenne | 18,3 min | 18,6 min | 19,8 min | |
| Contrôles qualité | 6/6 | 6/6 | 6/6 | |

- **Aucune ligne perdue** : 1 444 845 lignes valides et 1 594 rejetées (0,11 %) donnent exactement les
  1 446 439 trajets annoncés par l'API. Motifs de rejet : `invalid_amount` (1 281), `invalid_duration` (253),
  `invalid_end_timestamp` (36), `invalid_distance` (24).
- **Top zones de prise en charge** : Near North Side (21 %), O'Hare (17 %), Loop (16 %), Near West Side (10 %).
  Les trajets depuis O'Hare rapportent environ 3,5 fois plus par course (51 $ contre 15 $).
- **Paiement** : carte bancaire 35 %, espèces 30 %, mobile 16 %. Le taux de pourboire est d'environ 22 %
  par carte et quasi nul en espèces, car les pourboires en espèces ne sont pas déclarés.

## 12. Avec plus de temps

- **CI** (GitHub Actions) : `ruff`, `pytest` et tests d'intégrité du DAG à chaque PR, build de l'image.
- **Test d'intégration de bout en bout** sur un petit échantillon figé (fixture CSV) dans Compose.
- **Moteur SQL sur le lac** (Trino ou DuckDB + Delta) pour interroger le gold sans copie vers Postgres.
- **Silver incrémental** avec `MERGE` Delta et CDC sur la source, plutôt que la réécriture par mois.
- **Maintenance Delta** : `OPTIMIZE` / `Z-ORDER` et `VACUUM` planifiés.
- **Dashboard Metabase provisionné** automatiquement via son API, plutôt que configuré à la main.
- **Airflow 3** : migration vers le Task SDK et les *assets* (anciennement datasets).
- **Données enrichies** : météo, jours fériés, géométries des zones pour des cartes.
