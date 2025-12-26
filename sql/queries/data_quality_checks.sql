-- ===============================
-- COMPLETENESS CHECKS
-- ===============================

-- NULL checks
SELECT 'customers.email' AS check_name, COUNT(*) AS violations
FROM staging.customers
WHERE email IS NULL OR email = '';

SELECT 'products.price' AS check_name, COUNT(*) AS violations
FROM staging.products
WHERE price IS NULL;

-- ===============================
-- DUPLICATE CHECKS
-- ===============================

-- Duplicate customer emails
SELECT 'duplicate_customer_email' AS check_name, COUNT(*) AS violations
FROM (
    SELECT email
    FROM staging.customers
    GROUP BY email
    HAVING COUNT(*) > 1
) t;

-- ===============================
-- REFERENTIAL INTEGRITY
-- ===============================

-- Orphan transactions
SELECT 'orphan_transactions' AS check_name, COUNT(*) AS violations
FROM staging.transactions t
LEFT JOIN staging.customers c
ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orphan transaction items (transaction)
SELECT 'orphan_transaction_items_txn' AS check_name, COUNT(*) AS violations
FROM staging.transaction_items ti
LEFT JOIN staging.transactions t
ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Orphan transaction items (product)
SELECT 'orphan_transaction_items_product' AS check_name, COUNT(*) AS violations
FROM staging.transaction_items ti
LEFT JOIN staging.products p
ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;

-- ===============================
-- RANGE / BUSINESS RULE CHECKS
-- ===============================

-- Invalid discount
SELECT 'invalid_discount' AS check_name, COUNT(*) AS violations
FROM staging.transaction_items
WHERE discount_percentage < 0 OR discount_percentage > 100;

-- Cost >= price
SELECT 'cost_price_violation' AS check_name, COUNT(*) AS violations
FROM staging.products
WHERE cost >= price;

-- ===============================
-- CONSISTENCY CHECKS
-- ===============================

-- Line total mismatch
SELECT 'line_total_mismatch' AS check_name, COUNT(*) AS violations
FROM staging.transaction_items
WHERE ROUND(quantity * unit_price * (1 - discount_percentage/100.0), 2)
      <> ROUND(line_total, 2);
