import os
import json
import time
import logging
import psycopg2
import yaml
from datetime import datetime

# ----------------------------------------
# Paths
# ----------------------------------------
RAW_DIR = "data/raw"
STAGING_DIR = "data/staging"
LOG_DIR = "logs"

os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ----------------------------------------
# Logging setup
# ----------------------------------------
log_file = f"{LOG_DIR}/ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------------------
# Load config
# ----------------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

db = config["database"]

# ----------------------------------------
# Database connection
# ----------------------------------------
def get_connection():
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"]
    )

# ----------------------------------------
# Bulk COPY loader
# ----------------------------------------
def copy_csv(cursor, table, csv_file):
    with open(csv_file, "r", encoding="utf-8") as f:
        next(f)  # skip header
        cursor.copy_expert(
            f"COPY {table} FROM STDIN WITH CSV",
            f
        )

# ----------------------------------------
# Validation
# ----------------------------------------
def validate_staging_load(cursor, table, csv_file):
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    db_count = cursor.fetchone()[0]

    with open(csv_file, "r", encoding="utf-8") as f:
        csv_count = sum(1 for _ in f) - 1

    return db_count == csv_count, db_count, csv_count

# ----------------------------------------
# Main ingestion
# ----------------------------------------
def main():
    start_time = time.time()
    summary = {
        "ingestion_timestamp": datetime.now().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0
    }

    tables = {
        "staging.customers": f"{RAW_DIR}/customers.csv",
        "staging.products": f"{RAW_DIR}/products.csv",
        "staging.transactions": f"{RAW_DIR}/transactions.csv",
        "staging.transaction_items": f"{RAW_DIR}/transaction_items.csv"
    }

    conn = None

    try:
        conn = get_connection()
        conn.autocommit = False
        cursor = conn.cursor()

        logging.info("Starting ingestion transaction")

        for table, csv_file in tables.items():
            try:
                logging.info(f"Loading {table} from {csv_file}")

                cursor.execute(f"TRUNCATE TABLE {table}")
                copy_csv(cursor, table, csv_file)

                valid, db_rows, csv_rows = validate_staging_load(cursor, table, csv_file)

                if not valid:
                    raise Exception(
                        f"Row count mismatch for {table} (db={db_rows}, csv={csv_rows})"
                    )

                summary["tables_loaded"][table] = {
                    "rows_loaded": db_rows,
                    "status": "success"
                }

                logging.info(f"{table} loaded successfully")

            except Exception as e:
                summary["tables_loaded"][table] = {
                    "rows_loaded": 0,
                    "status": "failed",
                    "error_message": str(e)
                }
                logging.error(str(e))
                raise

        conn.commit()
        logging.info("Transaction committed")

    except Exception as e:
        if conn:
            conn.rollback()
            logging.error("Transaction rolled back")
        logging.error(str(e))

    finally:
        if conn:
            conn.close()

    summary["total_execution_time_seconds"] = round(
        time.time() - start_time, 2
    )

    with open(f"{STAGING_DIR}/ingestion_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

    print("CSV ingestion to staging completed")

# ----------------------------------------
if __name__ == "__main__":
    main()
