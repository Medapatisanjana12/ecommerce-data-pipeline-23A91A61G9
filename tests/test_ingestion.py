import psycopg2

def test_database_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="ecommerce_db",
        user="admin",
        password="admin"
    )
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    assert cur.fetchone()[0] == 1
    conn.close()
