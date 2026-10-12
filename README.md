# WABA Group — Plateforme Lakehouse Financière Multi-Pays

Challenge Data Engineer Artefact — niveaux 1 à 3 :
génération de données → MinIO → Spark/Iceberg (raw, bronze, silver, gold) → Trino,
orchestré par Airflow, avec un chemin temps réel NiFi → Kafka → Spark Structured Streaming (architecture Lambda).

```
 Streamlit ──► MinIO raw-landing ──┬──► Spark batch (Airflow) ──► Iceberg raw ► bronze ► silver ► gold ──┐
                                   │                                                                      ├──► Trino
                                   └──► NiFi ──► Kafka raw-* ──► Spark Streaming Job 1 ──► silver-* + Iceberg silver
                                                                  Spark Streaming Job 2 ──► gold-fraud/aml/liquidity ─┘
```

## Prérequis

| Outil | Version testée |
|---|---|
| Docker Engine | ≥ 24 (BuildKit activé) |
| Docker Compose | ≥ v2.20 |
| RAM allouée à Docker | 8 Go (niveau 1), 12–16 Go (stack complète) |
| Accès Internet au premier build | Maven Central, PyPI, Docker Hub |

## Structure

```
generator/          Application Streamlit + générateur (pandas/numpy) + Dockerfile
spark/Dockerfile    Image Spark 3.5.3 + jars Iceberg 1.6.1, S3A, Kafka
spark/jobs/         Jobs PySpark (batch + streaming) et package partagé waba/
airflow/            Image Airflow 2.10.5 + DAGs
trino/              Catalogues Iceberg et Kafka, descriptions des topics, requêtes SQL
kafka/              Création des topics
nifi/               Script de création du flux NiFi via l'API REST
tests/              Test local de la logique (sans Docker)
docs/WRITEUP.md     Choix d'architecture, compromis, limites
```

## Démarrage

```bash
git clone <repo> && cd waba-lakehouse
cp .env.example .env          # puis remplacer les valeurs change_me
docker compose up -d --build  # niveau 1 : MinIO, Iceberg REST, Spark, Trino, Streamlit
```

| Service | URL |
|---|---|
| Streamlit (générateur) | http://localhost:8501 |
| MinIO console | http://localhost:9001 |
| Spark master | http://localhost:8081 |
| Trino | http://localhost:8085 |
| Airflow (niveau 2) | http://localhost:8088 |
| NiFi (niveau 3) | http://localhost:8090/nifi |

### Niveau 1 — ingestion batch

1. Streamlit → « Référentiels » → *Générer et envoyer* (ou `make referentials`).
2. Streamlit → chaque type de transaction → *One-time* (ou `make transactions`).
3. Ingestion Spark dans `raw.*` puis archivage des fichiers :
   ```bash
   make ingest
   ```
4. Idempotence : ré-ingérer les mêmes fichiers depuis l'archive ne crée aucun doublon :
   ```bash
   make reingest-archive
   ```
5. Requêtes :
   ```bash
   docker compose exec trino trino --catalog iceberg -f /queries/level1_raw.sql
   ```

### Niveau 2 — Airflow + médaillon

```bash
docker compose --profile orchestration up -d --build
```

- `dag_ingest_raw` tourne toutes les 10 min, ne fait rien s'il n'y a pas de fichier, sinon raw → bronze puis déclenche `dag_bronze_to_silver` → `dag_silver_to_gold`.
- `dag_regulatory_report` tourne à 00h30 UTC et produit les agrégats BCEAO/CIMA de J-1.
- Backfill d'un pays : *Trigger DAG w/ config* → `{"countries": "CI,SN"}`.
- Sans Airflow : `make bronze silver gold report`.

```bash
docker compose exec trino trino --catalog iceberg -f /queries/level2_gold.sql
```

### Niveau 3 — streaming

Pré-requis : avoir lancé au moins une fois `bronze_to_silver` (les référentiels silver servent à l'enrichissement temps réel).

```bash
docker compose --profile orchestration --profile streaming up -d --build
```

- `kafka-init` crée les 11 topics, `nifi-init` crée et démarre le flux NiFi.
- Dans Streamlit : mode *Continue*, cocher « Injecter des scénarios suspects » et éventuellement un taux de lignes malformées.
- Résultats :
  ```bash
  docker compose exec trino trino -f /queries/level3_lambda.sql
  docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic gold-fraud-alerts
  ```

## Tests sans Docker

```bash
pip install pyspark==3.5.3 pandas==2.2.3 numpy boto3
python tests/test_local_pipeline.py
```

Le test génère des données, applique la validation, les transformations Silver, les 7 KPIs Gold et les règles de fraude sur un Spark local.

## Arrêt

```bash
docker compose --profile orchestration --profile streaming down      # -v pour effacer les données
```
