import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
import json
import random
import yaml
from faker import Faker
from datetime import datetime
import pandas as pd

# -----------------------------------
# Setup
# -----------------------------------
fake = Faker()
RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

# -----------------------------------
# Load configuration
# -----------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

gen_cfg = config["data_generation"]

CUSTOMERS_COUNT = gen_cfg["customers_count"]
PRODUCTS_COUNT = gen_cfg["products_count"]
ORDERS_COUNT = gen_cfg["orders_count"]

START_DATE = datetime.fromisoformat(gen_cfg["date_range"]["start_date"])
END_DATE = datetime.fromisoformat(gen_cfg["date_range"]["end_date"])

# -----------------------------------
# Generate Customers
# -----------------------------------
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

# -----------------------------------
# Generate Products
# -----------------------------------
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
        "supplier_id": f"SUP{random.randint(1, 50):03d}"
    })

products_df = pd.DataFrame(products)
products_df.to_csv(f"{RAW_DIR}/products.csv", index=False)

# -----------------------------------
# Generate Transactions & Items
# -----------------------------------
transactions = []
transaction_items = []
item_counter = 1

customer_ids = customers_df["customer_id"].tolist()
product_records = products_df.to_dict("records")

for i in range(1, ORDERS_COUNT + 1):
    transaction_id = f"TXN{i:05d}"
    customer_id = random.choice(customer_ids)
    transaction_date = fake.date_between(start_date=START_DATE, end_date=END_DATE)
    transaction_time = fake.time()
    payment_method = random.choice(
        ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
    )

    chosen_products = random.sample(product_records, random.randint(1, 5))
    total_amount = 0.0

    for prod in chosen_products:
        quantity = random.randint(1, 3)
        discount = random.choice([0, 5, 10, 15])
        line_total = round(quantity * prod["price"] * (1 - discount / 100), 2)

        transaction_items.append({
            "item_id": f"ITEM{item_counter:05d}",
            "transaction_id": transaction_id,
            "product_id": prod["product_id"],
            "quantity": quantity,
            "unit_price": prod["price"],
            "discount_percentage": discount,
            "line_total": line_total
        })

        total_amount += line_total
        item_counter += 1

    transactions.append({
        "transaction_id": transaction_id,
        "customer_id": customer_id,
        "transaction_date": transaction_date,
        "transaction_time": transaction_time,
        "payment_method": payment_method,
        "shipping_address": fake.address().replace("\n", ", "),
        "total_amount": round(total_amount, 2)
    })

pd.DataFrame(transactions).to_csv(f"{RAW_DIR}/transactions.csv", index=False)
pd.DataFrame(transaction_items).to_csv(f"{RAW_DIR}/transaction_items.csv", index=False)

# -----------------------------------
# Metadata
# -----------------------------------
metadata = {
    "generated_at": datetime.now().isoformat(),
    "record_counts": {
        "customers": len(customers),
        "products": len(products),
        "transactions": len(transactions),
        "transaction_items": len(transaction_items)
    }
}

with open(f"{RAW_DIR}/generation_metadata.json", "w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=4)

print("Data generation completed successfully")
