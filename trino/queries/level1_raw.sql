SHOW TABLES FROM iceberg.raw;

SELECT country_code, currency, COUNT(*) AS accounts, ROUND(SUM(balance), 2) AS total_balance
FROM iceberg.raw.accounts
GROUP BY 1, 2
ORDER BY 1;

SELECT country_code, CAST("timestamp" AS DATE) AS txn_date, COUNT(*) AS txn_count, ROUND(SUM(amount), 2) AS amount
FROM iceberg.raw.bank_transactions
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT entity_type, COUNT(*) AS customers
FROM iceberg.raw.customers
GROUP BY 1;

SELECT transaction_id, COUNT(*) AS n
FROM iceberg.raw.bank_transactions
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT COUNT(*) AS orphan_transactions
FROM iceberg.raw.bank_transactions t
LEFT JOIN iceberg.raw.accounts a ON t.account_id = a.account_id
WHERE a.account_id IS NULL;

SELECT dataset, error, COUNT(*) FROM iceberg.raw.rejected_records GROUP BY 1, 2;
