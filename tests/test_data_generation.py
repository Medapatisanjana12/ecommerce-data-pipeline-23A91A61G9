import os
import pandas as pd

RAW_DIR = "data/raw"

def test_raw_files_exist():
    files = [
        "customers.csv",
        "products.csv",
        "transactions.csv",
        "transaction_items.csv"
    ]
    for f in files:
        assert os.path.exists(os.path.join(RAW_DIR, f))

def test_raw_files_not_empty():
    for f in os.listdir(RAW_DIR):
        if f.endswith(".csv"):
            df = pd.read_csv(os.path.join(RAW_DIR, f))
            assert len(df) > 0
