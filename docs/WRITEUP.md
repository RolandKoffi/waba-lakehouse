# Write-up technique — WABA Lakehouse

## 1. Architecture retenue

Architecture Lambda autour d'un stockage objet unique (MinIO) et d'un format de table ouvert (Apache Iceberg) :

- **Couche batch** : Airflow orchestre des jobs PySpark qui font passer les fichiers de `raw-landing` par les zones `raw → bronze → silver → gold`. C'est la source de vérité.
- **Couche vitesse** : NiFi détecte les mêmes fichiers, publie chaque ligne dans Kafka ; deux jobs Spark Structured Streaming nettoient (silver) puis détectent fraude, AML et tension de liquidité (gold).
- **Couche de service** : Trino lit Iceberg (catalogue REST) et Kafka dans la même requête SQL.

Iceberg apporte ce dont une banque a besoin : transactions ACID (MERGE atomique), time travel (audit), évolution de schéma, partitionnement caché (`days(timestamp)`) et un catalogue partagé par Spark et Trino.

## 2. Choix et compromis

| Sujet | Choix | Pourquoi / compromis |
|---|---|---|
| Catalogue | Iceberg REST (tabulario), SQLite sur volume | Plus léger qu'un Hive Metastore + Postgres. En production : catalogue REST managé (Polaris, Nessie, Glue). |
| Raw vs Bronze | `raw.*` partitionné par date métier, `bronze.*` par date d'ingestion | Le sujet demande les deux partitionnements. `raw` sert l'analyse par date d'opération, `bronze` le rechargement incrémental. Coût : stockage doublé. |
| Schéma explicite | CSV lu tout en STRING puis cast contrôlé | Permet de dire *pourquoi* une ligne est rejetée (type, champ manquant, devise, pays) au lieu de perdre l'info avec un cast silencieux. |
| Idempotence | `MERGE INTO … ON id AND country_code` + dédoublonnage source | Re-traiter un fichier ne crée pas de doublon ; la condition sur `country_code` permet l'élagage de partitions. |
| Archivage | Copie → vérification (HEAD) → suppression, après commit Iceberg | Aucune suppression avant que les données soient sûres dans le lakehouse. |
| Pseudonymisation | SHA-256 salé des numéros de compte en silver | Déterministe, donc les jointures restent possibles. Raw/bronze en clair = zone à accès restreint. |
| Silver partagé batch/stream | Mêmes fonctions de transformation (`waba/silver.py`) | Une seule logique métier : pas de divergence entre couches Lambda. |
| Conflit batch/stream | Stream : insert si absent ; batch : upsert | Le batch fait foi. Colonne `source` pour tracer l'origine. |
| Gold | Recalcul complet par pays + `overwritePartitions` | Simple et idempotent ; acceptable aux volumes du challenge. En production : recalcul incrémental par fenêtre de dates. |
| Spark | Cluster standalone pour le batch, `local[2]` pour le streaming | Les jobs streaming gardent leurs cœurs en permanence ; les isoler évite d'affamer le batch. |
| Airflow → Spark | `SparkSubmitOperator` en mode client, connexion `spark_waba` | Les logs du driver remontent dans Airflow. Le driver tourne dans le conteneur Airflow (`spark.driver.host=airflow`). |
| Déclenchement | Planification 10 min + `ShortCircuitOperator` qui liste MinIO | Plus robuste qu'un sensor qui monopolise un slot. Chaînage par `TriggerDagRunOperator` qui transmet `countries`. |
| Kafka | KRaft mono-broker, 8 partitions, clé = pays | Ordre garanti par pays ; 8 partitions = parallélisme naturel. |
| NiFi | Flux créé par script REST idempotent | Reproductible depuis Git sans export manuel de template. |
| Fraude | Fenêtres glissantes 5 min / 1 min, watermark 10 min, mode `update` | Alerte dès le seuil franchi ; `alert_id` déterministe → MERGE évite les doublons d'alerte. |

## 3. Données simulées

Les référentiels sont générés avec une graine fixe : les clés sont stables d'une génération à l'autre. Les transactions piochent uniquement dans les comptes/agences du pays concerné. Le générateur est calibré pour des KPIs réalistes : NPL 4–7 % par pays, loss ratio 56–76 %. Des scénarios suspects (rafales de virements, mobile money hors pays, sinistres anormaux) et des lignes malformées peuvent être injectés.

## 4. Limites connues

- Pas de test bout-en-bout automatisé de la stack Docker (test local de la logique fourni).
- Taux de change GHS/EUR fixe en configuration ; en production, table de taux quotidienne.
- Intérêts perçus estimés (montant payé × taux du produit), faute de tableau d'amortissement dans le schéma.
- Le streaming sur des fichiers historiques (mode one-time) voit beaucoup d'événements arriver « en retard » pour le watermark : la détection temps réel est pensée pour le mode continu.
- NiFi et Airflow lisent le même bucket ; NiFi interroge toutes les 5 s, Airflow archive au plus tôt 10 min après. En production : notifications d'événements MinIO vers Kafka.
- Mono-nœud partout (Kafka, MinIO, NiFi) : pas de haute disponibilité. Le niveau 4 (Kubernetes/Helm) n'est pas traité.
- Secrets dans `.env` ; en production, Vault ou un secret manager.
