-- Requête du sujet (exécutable telle quelle grâce aux champs event_time / streaming_amount_eur du topic)
SELECT
    COALESCE(b.country_code, s.country_code) AS country,
    COALESCE(b.txn_date, CAST(s.event_time AS DATE)) AS date,
    COALESCE(b.total_amount_eur, 0) + COALESCE(s.streaming_amount_eur, 0) AS total_eur
FROM iceberg.gold.daily_transaction_volume b
FULL OUTER JOIN kafka.default."silver-bank-transactions" s
    ON b.country_code = s.country_code
    AND b.txn_date = CAST(s.event_time AS DATE)
ORDER BY 1, 2;

-- Version corrigée : pré-agrégation des deux côtés + la couche batch fait foi quand elle existe
WITH batch AS (
    SELECT country_code, txn_date, SUM(total_amount_eur) AS amount_eur
    FROM iceberg.gold.daily_transaction_volume
    GROUP BY 1, 2
),
speed AS (
    SELECT country_code, CAST(event_time AS DATE) AS txn_date, SUM(amount_eur) AS amount_eur
    FROM kafka.default."silver-bank-transactions"
    WHERE transaction_status = 'SUCCESS'
    GROUP BY 1, 2
)
SELECT
    COALESCE(b.country_code, s.country_code) AS country,
    COALESCE(b.txn_date, s.txn_date) AS date,
    ROUND(COALESCE(b.amount_eur, s.amount_eur), 2) AS total_eur,
    CASE WHEN b.amount_eur IS NOT NULL THEN 'batch' ELSE 'speed' END AS served_by
FROM batch b
FULL OUTER JOIN speed s ON b.country_code = s.country_code AND b.txn_date = s.txn_date
ORDER BY 1, 2;

SELECT rule, country_code, COUNT(*) AS alerts FROM kafka.default."gold-fraud-alerts" GROUP BY 1, 2;
SELECT * FROM kafka.default."gold-aml-events" ORDER BY detected_at DESC LIMIT 20;
SELECT * FROM kafka.default."gold-liquidity-alerts" ORDER BY detected_at DESC LIMIT 20;
SELECT error, COUNT(*) FROM kafka.default."dlq-financial-events" GROUP BY 1;
SELECT rule, COUNT(*) FROM iceberg.gold.fraud_alerts GROUP BY 1;
