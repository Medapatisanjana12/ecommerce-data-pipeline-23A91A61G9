-- Query 1: Top Customers by Revenue
SELECT
    customer_id,
    SUM(total_amount) AS total_revenue,
    COUNT(*) AS transaction_count
FROM production.transactions
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 2: Monthly Sales Trend
SELECT
    TO_CHAR(transaction_date, 'YYYY-MM') AS year_month,
    SUM(total_amount) AS total_revenue,
    COUNT(*) AS total_transactions,
    AVG(total_amount) AS avg_order_value
FROM production.transactions
GROUP BY year_month
ORDER BY year_month;

-- Query 3: Customer Segmentation
WITH customer_totals AS (
    SELECT
        customer_id,
        SUM(total_amount) AS total_spent
    FROM production.transactions
    GROUP BY customer_id
)
SELECT
    CASE
        WHEN total_spent < 1000 THEN '0-1k'
        WHEN total_spent BETWEEN 1000 AND 5000 THEN '1k-5k'
        WHEN total_spent BETWEEN 5000 AND 10000 THEN '5k-10k'
        ELSE '10k+'
    END AS spending_segment,
    COUNT(*) AS customer_count
FROM customer_totals
GROUP BY spending_segment
ORDER BY customer_count DESC;

-- Query 4: Payment Method Distribution
SELECT
    payment_method,
    COUNT(*) AS transaction_count,
    SUM(total_amount) AS total_revenue
FROM production.transactions
GROUP BY payment_method
ORDER BY total_revenue DESC;

-- Query 5: Day of Week Sales Pattern
SELECT
    TO_CHAR(transaction_date, 'Day') AS day_name,
    COUNT(*) AS transactions,
    SUM(total_amount) AS total_revenue
FROM production.transactions
GROUP BY day_name
ORDER BY total_revenue DESC;
