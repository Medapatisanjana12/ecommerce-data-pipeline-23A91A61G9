import os
import yaml
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# --------------------------------------------------
# Load config
# --------------------------------------------------
with open("config/config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

db = cfg["database"]

# --------------------------------------------------
# Database connection
# --------------------------------------------------
engine = create_engine(
    f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}",
    echo=False
)

# --------------------------------------------------
# Paths
# --------------------------------------------------
RAW_DIR = "data/raw"

TABLE_FILE_MAP = {
    "customers": "customers.csv",
    "products": "products.csv",
    "transactions": "transactions.csv",
    "transaction_items": "transaction_items.csv",
}

# --------------------------------------------------
# Load function
# --------------------------------------------------
def load_csv_to_staging(table_name, file_name):
    file_path = os.path.join(RAW_DIR, file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing file: {file_path}")

    df = pd.read_csv(file_path)

    with engine.begin() as conn:
        # Idempotency: TRUNCATE before load
        conn.execute(text(f"TRUNCATE TABLE staging.{table_name}"))

        df.to_sql(
            table_name,
            conn,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi"
        )

    return len(df)


# --------------------------------------------------
# Main execution
# --------------------------------------------------
if __name__ == "__main__":
    start = datetime.now()
    print("Starting Data Ingestion → Staging")

    total_rows = 0

    for table, file in TABLE_FILE_MAP.items():
        rows = load_csv_to_staging(table, file)
        total_rows += rows
        print(f"Loaded {rows} rows into staging.{table}")

    print(f"Data ingestion completed. Total rows loaded: {total_rows}")
    print(f"Duration: {(datetime.now() - start).total_seconds():.2f}s")
