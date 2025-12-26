-- ==============================
-- Query 1: Data Freshness Check
-- ==============================

SELECT
    'staging' AS layer,
    MAX(created_at) AS latest_timestamp
FROM staging.transactions
UNION ALL
SELECT
    'production' AS layer,
    MAX(created_at) AS latest_timestamp
FROM production.transactions;

-- ==============================
-- Query 2: Volume Trend (Last 30 Days)
-- ==============================

SELECT
    transaction_date,
    COUNT(*) AS daily_transactions
FROM production.transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY transaction_date
ORDER BY transaction_date;

-- ==============================
-- Query 3: Data Quality Checks
-- ==============================

SELECT
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_ids,
    COUNT(*) FILTER (WHERE total_amount <= 0) AS invalid_amounts
FROM production.transactions;

-- ==============================
-- Query 4: Database Size
-- ==============================

SELECT
    pg_database_size(current_database()) / 1024 / 1024 AS database_size_mb;
