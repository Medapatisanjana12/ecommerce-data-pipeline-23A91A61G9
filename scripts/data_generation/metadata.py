import pandas as pd

BASE_DIR = "data/raw"

def generate_metadata():
    metadata = {}
    for file in ["customers.csv", "products.csv", "transactions.csv", "transaction_items.csv"]:
        df = pd.read_csv(f"{BASE_DIR}/{file}")
        metadata[file] = {
            "record_count": len(df),
            "columns": list(df.columns)
        }
    return metadata

if __name__ == "__main__":
    print(generate_metadata())
