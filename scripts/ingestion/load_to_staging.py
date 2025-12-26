import os
import psycopg2
import pandas as pd

RAW_DIR = "data/raw"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin"),
}

TABLES = {
    "customers.csv": "staging.customers",
    "products.csv": "staging.products",
    "transactions.csv": "staging.transactions",
    "transaction_items.csv": "staging.transaction_items",
}

def load_csv(cursor, conn, csv_file, table):
    path = os.path.join(RAW_DIR, csv_file)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")

    df = pd.read_csv(path)

    cursor.execute(f"TRUNCATE TABLE {table}")

    cols = ",".join(df.columns)
    values = ",".join(["%s"] * len(df.columns))

    insert_sql = f"INSERT INTO {table} ({cols}) VALUES ({values})"

    for row in df.itertuples(index=False):
        cursor.execute(insert_sql, tuple(row))

    conn.commit()

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for csv_file, table in TABLES.items():
        print(f"Loading {csv_file} → {table}")
        load_csv(cur, conn, csv_file, table)

    cur.close()
    conn.close()
    print("Staging load completed successfully")

if __name__ == "__main__":
    main()
