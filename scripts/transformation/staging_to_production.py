import psycopg2
import datetime
import pandas as pd
import os
import json

# -----------------------------
# Database connection
# -----------------------------
conn = psycopg2.connect(
    host="localhost",
    database="ecommerce_db",
    user="admin",
    password="admin",   # change if different
    port=5432
)

cur = conn.cursor()

print("Starting Staging → Production ETL...")

# -----------------------------
# Load PRODUCTS
# -----------------------------
cur.execute("""
INSERT INTO production.products (
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    price,
    cost,
    created_at
)
SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    price,
    cost,
    NOW()
FROM staging.products
WHERE price > 0
  AND cost < price;
""")

print("Products loaded")

# -----------------------------
# Load CUSTOMERS
# -----------------------------
cur.execute("""
INSERT INTO production.customers (
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    country,
    registration_date,
    created_at
)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    country,
    registration_date,
    NOW()
FROM staging.customers;
""")

print("✓ Customers loaded")

# -----------------------------
# Load TRANSACTIONS
# -----------------------------

cur.execute("""INSERT INTO production.transactions (
  transaction_id,
  customer_id,
  transaction_date,
  transaction_time,
  payment_method,
  shipping_address,
  total_amount,
  created_at,
  updated_at
)
SELECT
  transaction_id,
  customer_id,
  transaction_date,
  transaction_time,
  payment_method,
  shipping_address,
  total_amount,
  NOW(),
  NOW()
FROM staging.transactions;
""")
print("✓ Transactions loaded")

# Commit the loads before starting analytics
conn.commit()
print("✅ Staging → Production ETL completed successfully")

# ---------------------------------------------------------
# CONTINUATION: ANALYTICS GENERATION (Queries 1-10)
# ---------------------------------------------------------
print("\n📊 Generating 10 Analytical Reports...")

# Create the directory structure as per Image 2
output_dir = "data/processed/analytics/"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Define the queries based on Image 1 requirements
queries = {
     "Query 1":
"""SELECT
    p.product_name,
    SUM(ti.quantity * ti.unit_price) AS total_revenue,
    SUM(ti.quantity) AS total_units
FROM production.transaction_items ti
JOIN production.products p
    ON ti.product_id = p.product_id
JOIN production.transactions t
    ON ti.transaction_id = t.transaction_id
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 10;""",

    "query2_monthly_trend": """
        SELECT
    TO_CHAR(t.transaction_date, 'YYYY-MM') AS month,
    SUM(ti.quantity * ti.unit_price) AS total_revenue,
    COUNT(DISTINCT t.transaction_id) AS transaction_count
FROM production.transaction_items ti
JOIN production.transactions t
    ON ti.transaction_id = t.transaction_id
GROUP BY 1
ORDER BY 1;
    """,
    "query3_customer_segmentation": """
    WITH customer_totals AS (
    SELECT
        customer_id,
        SUM(total_amount) AS total_spent,
        COUNT(*) AS txn_count
    FROM production.transactions
    GROUP BY customer_id
)
SELECT
    CASE
        WHEN total_spent <= 1000 THEN '$0-$1,000'
        WHEN total_spent <= 5000 THEN '$1,000-$5,000'
        WHEN total_spent <= 10000 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    AVG(total_spent / txn_count) AS avg_transaction_value
FROM customer_totals
GROUP BY spending_segment
ORDER BY spending_segment;
   
   """,
    "query4_category_performance": """
        SELECT p.category, SUM(t.quantity * t.unit_price) as total_revenue, SUM((t.unit_price - p.cost) * t.quantity) as total_profit,
               (SUM((t.unit_price - p.cost) * t.quantity) / NULLIF(SUM(t.quantity * t.unit_price), 0)) * 100 as profit_margin_pct,
               SUM(t.quantity) as units_sold
        FROM production.transactions t JOIN production.products p ON t.product_id = p.product_id GROUP BY 1;
    """,
    "query5_payment_distribution": """
        SELECT payment_method, COUNT(*) as transaction_count, SUM(quantity * unit_price) as total_revenue,
               (COUNT(*)::float / SUM(COUNT(*)) OVER()) * 100 as pct_of_transactions,
               (SUM(quantity * unit_price) / SUM(SUM(quantity * unit_price)) OVER()) * 100 as pct_of_revenue
        FROM production.transactions GROUP BY 1;
    """,
    "query6_geographic_analysis": """
        SELECT c.state, SUM(t.quantity * t.unit_price) as total_revenue, COUNT(DISTINCT c.customer_id) as total_customers,
               SUM(t.quantity * t.unit_price) / NULLIF(COUNT(DISTINCT c.customer_id), 0) as avg_revenue_per_customer
        FROM production.transactions t JOIN production.customers c ON t.customer_id = c.customer_id GROUP BY 1;
    """,
    "query7_customer_lifetime_value": """
     SELECT
  c.customer_id,
  SUM(t.total_amount) AS total_spent,
  COUNT(DISTINCT t.transaction_id) AS transaction_count
FROM production.customers c
JOIN production.transactions t
  ON c.customer_id = t.customer_id
GROUP BY c.customer_id
ORDER BY total_spent DESC;

     """,
    "query8_product_profitability": """
      SELECT
  p.product_name,
  SUM(ti.quantity * ti.unit_price) AS total_revenue,
  SUM(ti.quantity) AS units_sold
FROM production.transaction_items ti
JOIN production.products p
  ON ti.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 10;

     """,
    "query9_day_of_week_pattern": """
        SELECT TO_CHAR(transaction_date, 'Day') as day_name, AVG(daily_rev) as avg_daily_revenue, 
               AVG(daily_cnt) as avg_daily_transactions, SUM(daily_rev) as total_revenue
        FROM (
            SELECT transaction_date, SUM(quantity * unit_price) as daily_rev, COUNT(*) as daily_cnt
            FROM production.transactions GROUP BY 1
        ) sub GROUP BY 1;
    """,
    "query10_discount_impact": """
        SELECT 
            CASE 
                WHEN discount_percentage = 0 THEN '0%'
                WHEN discount_percentage <= 10 THEN '1-10%'
                WHEN discount_percentage <= 25 THEN '11-25%'
                WHEN discount_percentage <= 50 THEN '26-50%'
                ELSE '50%+' 
            END as discount_range,
            AVG(discount_percentage) as avg_discount_pct, SUM(quantity) as total_quantity_sold,
            SUM(quantity * unit_price) as total_revenue, AVG(quantity * unit_price) as avg_line_total
        FROM production.transactions GROUP BY 1;
    """
}

# Run each query and save to CSV as shown in Image 2
for filename, sql in queries.items():
    df = pd.read_sql(sql, conn)
    df.to_csv(f"{output_dir}{filename}.csv", index=False)
    print(f"  ✓ {filename}.csv generated")

# Generate the JSON summary
summary_data = {
    "last_run": str(datetime.datetime.now()),
    "total_queries": len(queries),
    "status": "completed"
}
with open(f"analytics_summary.json", 'w') as f:
    json.dump(summary_data, f, indent=4)

print("  analytics_summary.json generated")

# -----------------------------
# Close Connection
# -----------------------------
cur.close()
conn.close()

print("\nAll 10 outputs created successfully in " + output_dir)
