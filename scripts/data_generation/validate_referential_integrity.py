import pandas as pd

BASE_DIR = "data/raw"

customers = pd.read_csv(f"{BASE_DIR}/customers.csv")
products = pd.read_csv(f"{BASE_DIR}/products.csv")
transactions = pd.read_csv(f"{BASE_DIR}/transactions.csv")
items = pd.read_csv(f"{BASE_DIR}/transaction_items.csv")

def validate_referential_integrity():
    errors = {}

    errors["invalid_customers"] = transactions[
        ~transactions["customer_id"].isin(customers["customer_id"])
    ].shape[0]

    errors["invalid_products"] = items[
        ~items["product_id"].isin(products["product_id"])
    ].shape[0]

    errors["invalid_transactions"] = items[
        ~items["transaction_id"].isin(transactions["transaction_id"])
    ].shape[0]

    return errors

if __name__ == "__main__":
    result = validate_referential_integrity()
    print("Validation Results:", result)
