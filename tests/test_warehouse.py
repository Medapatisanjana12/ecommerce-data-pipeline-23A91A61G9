import psycopg2

def test_fact_sales_exists():
    conn = psycopg2.connect(
        host="localhost",
        database="ecommerce_db",
        user="admin",
        password="admin"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='warehouse'
            AND table_name='fact_sales'
        );
    """)
    assert cur.fetchone()[0] is True
    conn.close()
