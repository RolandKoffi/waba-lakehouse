SELECT * FROM iceberg.gold.daily_transaction_volume WHERE country_code = 'CI' ORDER BY txn_date DESC LIMIT 20;

SELECT month, country_code, npl_ratio, above_bceao_threshold
FROM iceberg.gold.npl_ratio_by_country
WHERE loan_type = 'ALL'
ORDER BY month DESC, country_code;

SELECT * FROM iceberg.gold.customer_arpu_monthly WHERE country_code = 'SN' ORDER BY month, segment;

SELECT month, country_code, product_line, loss_ratio, above_cima_threshold
FROM iceberg.gold.loss_ratio_by_product
ORDER BY loss_ratio DESC;

SELECT * FROM iceberg.gold.claims_processing_time ORDER BY month, country_code, line_group;

SELECT * FROM iceberg.gold.mobile_money_daily_flow WHERE country_code = 'GH' ORDER BY txn_date DESC LIMIT 20;

SELECT * FROM iceberg.gold.cross_border_transfers WHERE corridor = 'CI-SN' ORDER BY week_start;

SELECT * FROM iceberg.gold.regulatory_bceao_daily ORDER BY report_date DESC, country_code;
SELECT * FROM iceberg.gold.regulatory_cima_daily ORDER BY report_date DESC, country_code, product_line;

SELECT 'bronze' AS layer, COUNT(*) FROM iceberg.bronze.bank_transactions
UNION ALL SELECT 'silver', COUNT(*) FROM iceberg.silver.bank_transactions;

SELECT * FROM iceberg.silver.quarantine LIMIT 20;
