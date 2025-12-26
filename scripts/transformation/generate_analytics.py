import psycopg2
import pandas as pd
import json
import time
import os
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "dbname": "ecommerce_db",
    "user": "admin",
    "password": "admin",
    "port": 5432
}

SQL_FILE = "sql/queries/analytical_queries.sql"
OUTPUT_DIR = "data/processed/analytics"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def execute_query(conn, query):
    return pd.read_sql(query, conn)

def main():
    start_time = time.time()
    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": 0,
        "query_results": {}
    }

    with open(SQL_FILE, "r") as f:
        queries = f.read().split(";")

    conn = psycopg2.connect(**DB_CONFIG)

    for i, query in enumerate(queries):
        query = query.strip()
        if not query:
            continue

        query_name = f"query{i+1}"
        t0 = time.time()
        df = execute_query(conn, query)
        exec_time = round((time.time() - t0) * 1000, 2)

        file_name = f"{OUTPUT_DIR}/{query_name}.csv"
        df.to_csv(file_name, index=False)

        summary["queries_executed"] += 1
        summary["query_results"][query_name] = {
            "rows": len(df),
            "columns": len(df.columns),
            "execution_time_ms": exec_time
        }

    summary["total_execution_time_seconds"] = round(time.time() - start_time, 2)

    with open(f"{OUTPUT_DIR}/analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    conn.close()
    print("Analytics queries executed successfully")

if __name__ == "__main__":
    main()
