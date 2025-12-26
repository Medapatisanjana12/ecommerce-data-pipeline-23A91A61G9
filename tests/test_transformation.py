import psycopg2

def test_production_tables_populated():
    conn = psycopg2.connect(
        host="localhost",
        database="ecommerce_db",
        user="admin",
        password="admin"
    )
    cur = conn.cursor()

    tables = [
        "production.customers",
        "production.products",
        "production.transactions"
    ]

    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        assert count > 0

    conn.close()
