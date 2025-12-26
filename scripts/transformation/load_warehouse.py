import psycopg2
import datetime

conn = psycopg2.connect(
    host="localhost",
    dbname="ecommerce_db",
    user="admin",
    password="admin123",
    port=5432
)
cur = conn.cursor()

# ---------------------------
# Load dim_date (2024)
# ---------------------------
start = datetime.date(2024, 1, 1)
end = datetime.date(2024, 12, 31)

cur.execute("TRUNCATE warehouse.dim_date CASCADE")

current = start
while current <= end:
    cur.execute("""
        INSERT INTO warehouse.dim_date
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        int(current.strftime("%Y%m%d")),
        current,
        current.year,
        (current.month - 1)//3 + 1,
        current.month,
        current.day,
        current.strftime("%B"),
        current.strftime("%A"),
        current.isocalendar()[1],
        current.weekday() >= 5
    ))
    current += datetime.timedelta(days=1)

# ---------------------------
# Load dim_payment_method
# ---------------------------
cur.execute("TRUNCATE warehouse.dim_payment_method CASCADE")
cur.execute("""
INSERT INTO warehouse.dim_payment_method (payment_method_name, payment_type)
SELECT DISTINCT payment_method,
CASE
    WHEN payment_method IN ('Credit Card','Debit Card','UPI','Net Banking') THEN 'Online'
    ELSE 'Offline'
END
FROM production.transactions
""")

# ---------------------------
# Load dim_customers (SCD2 simplified)
# ---------------------------
cur.execute("TRUNCATE warehouse.dim_customers CASCADE")
cur.execute("""
INSERT INTO warehouse.dim_customers
(customer_id, full_name, email, city, state, country, age_group,
 customer_segment, registration_date, effective_date, end_date, is_current)
SELECT
customer_id,
first_name || ' ' || last_name,
email,
city, state, country, age_group,
'Regular',
registration_date,
CURRENT_DATE,
NULL,
TRUE
FROM production.customers
""")

# ---------------------------
# Load dim_products (SCD2 simplified)
# ---------------------------
cur.execute("TRUNCATE warehouse.dim_products CASCADE")
cur.execute("""
INSERT INTO warehouse.dim_products
(product_id, product_name, category, sub_category, brand, price_range,
 effective_date, end_date, is_current)
SELECT
product_id,
product_name,
category,
sub_category,
brand,
CASE
    WHEN price < 50 THEN 'Budget'
    WHEN price < 200 THEN 'Mid-range'
    ELSE 'Premium'
END,
CURRENT_DATE,
NULL,
TRUE
FROM production.products
""")

# ---------------------------
# Load fact_sales
# ---------------------------
cur.execute("TRUNCATE warehouse.fact_sales")

cur.execute("""
INSERT INTO warehouse.fact_sales
(date_key, customer_key, product_key, payment_method_key,
 transaction_id, quantity, unit_price, discount_amount, line_total, profit)
SELECT
d.date_key,
c.customer_key,
p.product_key,
pm.payment_method_key,
ti.transaction_id,
ti.quantity,
ti.unit_price,
ti.unit_price * ti.quantity * (ti.discount_percentage/100),
ti.line_total,
ti.line_total - (pr.cost * ti.quantity)
FROM production.transaction_items ti
JOIN production.transactions t ON ti.transaction_id = t.transaction_id
JOIN production.products pr ON ti.product_id = pr.product_id
JOIN warehouse.dim_date d ON d.full_date = t.transaction_date
JOIN warehouse.dim_customers c ON c.customer_id = t.customer_id AND c.is_current = TRUE
JOIN warehouse.dim_products p ON p.product_id = ti.product_id AND p.is_current = TRUE
JOIN warehouse.dim_payment_method pm ON pm.payment_method_name = t.payment_method
""")

# ---------------------------
# Aggregate table
# ---------------------------
cur.execute("TRUNCATE warehouse.agg_daily_sales")
cur.execute("""
INSERT INTO warehouse.agg_daily_sales
SELECT
date_key,
COUNT(DISTINCT transaction_id),
SUM(line_total),
SUM(profit),
COUNT(DISTINCT customer_key)
FROM warehouse.fact_sales
GROUP BY date_key
""")

conn.commit()
cur.close()
conn.close()

print("Warehouse load completed successfully")
