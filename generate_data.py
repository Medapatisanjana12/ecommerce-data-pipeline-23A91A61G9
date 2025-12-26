import os
import json
import random
import yaml
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd

fake = Faker()

RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

# -----------------------------
# Load config
# -----------------------------
with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

gen_cfg = config["data_generation"]

CUSTOMERS_COUNT = gen_cfg["customers_count"]
PRODUCTS_COUNT = gen_cfg["products_count"]
ORDERS_COUNT = gen_cfg["orders_count"]

start_date = datetime.fromisoformat(gen_cfg["date_range"]["start_date"])
end_date = datetime.fromisoformat(gen_cfg["date_range"]["end_date"])

# -----------------------------
# Customers
# -----------------------------
customers = []
for i in range(1, CUSTOMERS_COUNT + 1):
    customers.append({
        "customer_id": f"CUST{i:04d}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": f"user{i}@example.com",
        "phone": fake.phone_number(),
        "registration_date": fake.date_between(start_date="-2y", end_date="today"),
        "city": fake.city(),
        "state": fake.state(),
        "country": fake.country(),
        "age_group": random.choice(["18-25", "26-35", "36-50", "50+"])
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv(f"{RAW_DIR}/customers.csv", index=False)

# -----------------------------
# Products
# -----------------------------
categories = {
    "Electronics": (500, 50000),
    "Clothing": (300, 5000),
    "Home & Kitchen": (400, 10000),
    "Books": (200, 2000),
    "Sports": (500, 8000),
    "Beauty": (300, 6000)
}

products = []
for i in range(1, PRODUCTS_COUNT + 1):
    category = random.choice(list(categories.keys()))
    price = round(random.uniform(*categories[category]), 2)
    cost = round(price * random.uniform(0.6, 0.9), 2)

    products.append({
        "product_id": f"PROD{i:04d}",
        "product_name": fake.word().title(),
        "category": category,
        "sub_category": fake.word(),
        "price": price,
        "cost": cost,
        "brand": fake.company(),
        "stock_quantity": random.randint(10, 500),
        "supplier_id": f"SUP{random.randint(1,50):03d}"
    })

products_df = pd.DataFrame(products)
products_df.to_csv(f"{RAW_DIR}/products.csv", index=False)

# -----------------------------
# Transactions & Items
# -----------------------------
transactions = []
items = []

item_counter = 1

for i in range(1, ORDERS_COUNT + 1):
    txn_id = f"TXN{i:05d}"
    cust_id = random.choice(customers_df["customer_id"].tolist())
    txn_date = fake.date_between(start_date=start_date, end_date=end_date)
    txn_time = fake.time()
    payment = random.choice(["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"])

    num_items = random.randint(1, 5)
    chosen_products = products_df.sample(num_items)

    total_amount = 0

    for _, prod in chosen_products.iterrows():
        qty = random.randint(1, 3)
        discount = random.choice([0, 5, 10, 15])
        line_total = round(qty * prod["price"] * (1 - discount / 100), 2)

        items.append({
            "item_id": f"ITEM{item_counter:05d}",
            "transaction_id": txn_id,
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": prod["price"],
            "discount_percentage": discount,
            "line_total": line_total
        })

        total_amount += line_total
        item_counter += 1

    transactions.append({
        "transaction_id": txn_id,
        "customer_id": cust_id,
        "transaction_date": txn_date,
        "transaction_time": txn_time,
        "payment_method": payment,
        "shipping_address": fake.address().replace("\n", ", "),
        "total_amount": round(total_amount, 2)
    })

transactions_df = pd.DataFrame(transactions)
items_df = pd.DataFrame(items)

transactions_df.to_csv(f"{RAW_DIR}/transactions.csv", index=False)
items_df.to_csv(f"{RAW_DIR}/transaction_items.csv", index=False)

# -----------------------------
# Metadata
# -----------------------------
metadata = {
    "generated_at": datetime.now().isoformat(),
    "record_counts": {
        "customers": len(customers_df),
        "products": len(products_df),
        "transactions": len(transactions_df),
        "transaction_items": len(items_df)
    },
    "date_range": {
        "start": gen_cfg["date_range"]["start_date"],
        "end": gen_cfg["date_range"]["end_date"]
    }
}

with open(f"{RAW_DIR}/generation_metadata.json", "w") as f:
    json.dump(metadata, f, indent=4)

print("Data generation completed successfully.")

