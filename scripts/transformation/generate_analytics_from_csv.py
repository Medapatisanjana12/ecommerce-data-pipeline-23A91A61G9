import pandas as pd
import os

RAW = "data/raw"
OUT = "data/processed/analytics"
os.makedirs(OUT, exist_ok=True)

customers = pd.read_csv(f"{RAW}/customers.csv")
products = pd.read_csv(f"{RAW}/products.csv")
transactions = pd.read_csv(f"{RAW}/transactions.csv")
items = pd.read_csv(f"{RAW}/transaction_items.csv")

# Merge base fact
fact = items.merge(transactions, on="transaction_id") \
            .merge(products, on="product_id") \
            .merge(customers, on="customer_id")

fact["transaction_date"] = pd.to_datetime(fact["transaction_date"])
fact["revenue"] = fact["quantity"] * fact["unit_price"]
fact["profit"] = fact["revenue"] - (fact["cost"] * fact["quantity"])

# 1️⃣ Top Products
q1 = fact.groupby(["product_name", "category"]) \
         .agg(total_revenue=("revenue", "sum"),
              units_sold=("quantity", "sum"),
              avg_price=("unit_price", "mean")) \
         .sort_values("total_revenue", ascending=False) \
         .head(10)
q1.to_csv(f"{OUT}/query1_top_products.csv")

# 2️⃣ Monthly Trend
fact["year_month"] = fact["transaction_date"].dt.to_period("M").astype(str)
q2 = fact.groupby("year_month") \
         .agg(total_revenue=("revenue", "sum"),
              total_transactions=("transaction_id", "nunique"),
              average_order_value=("revenue", "mean"),
              unique_customers=("customer_id", "nunique"))
q2.to_csv(f"{OUT}/query2_monthly_trend.csv")

# 3️⃣ Customer Segmentation
cust_rev = fact.groupby("customer_id")["revenue"].sum().reset_index()

cust_rev["segment"] = pd.cut(
    cust_rev["revenue"],
    bins=[0,1000,5000,10000,9999999],
    labels=["$0-$1k","$1k-$5k","$5k-$10k","$10k+"]
)

q3 = cust_rev.groupby("segment") \
             .agg(customer_count=("customer_id","count"),
                  total_revenue=("revenue","sum"),
                  avg_transaction_value=("revenue","mean"))
q3.to_csv(f"{OUT}/query3_customer_segments.csv")

# 4️⃣ Category Performance
q4 = fact.groupby("category") \
         .agg(total_revenue=("revenue","sum"),
              total_profit=("profit","sum"),
              units_sold=("quantity","sum"))
q4["profit_margin_pct"] = (q4["total_profit"]/q4["total_revenue"])*100
q4.to_csv(f"{OUT}/query4_category_performance.csv")

# 5️⃣ Payment Distribution
q5 = fact.groupby("payment_method") \
         .agg(transaction_count=("transaction_id","nunique"),
              total_revenue=("revenue","sum"))
q5["pct_of_revenue"] = q5["total_revenue"] / q5["total_revenue"].sum() * 100
q5.to_csv(f"{OUT}/query5_payment_distribution.csv")

# 6️⃣ Geographic Analysis
q6 = fact.groupby("state") \
         .agg(total_revenue=("revenue","sum"),
              total_customers=("customer_id","nunique"),
              avg_revenue_per_customer=("revenue","mean"))
q6.to_csv(f"{OUT}/query6_geographic_analysis.csv")

# 7️⃣ Customer Lifetime Value
# 7️⃣ Customer Lifetime Value (CLV)
q7 = fact.groupby("customer_id") \
         .agg(
             total_spent=("revenue", "sum"),
             transaction_count=("transaction_id", "nunique")
         )

q7["avg_order_value"] = q7["total_spent"] / q7["transaction_count"]

q7.to_csv(f"{OUT}/query7_customer_lifetime_value.csv")

# 8️⃣ Product Profitability
q8 = fact.groupby(["product_name","category"]) \
         .agg(total_profit=("profit","sum"),
              revenue=("revenue","sum"),
              units_sold=("quantity","sum"))
q8["profit_margin"] = q8["total_profit"]/q8["revenue"]*100
q8.to_csv(f"{OUT}/query8_product_profitability.csv")

# 9️⃣ Day of Week Pattern
fact["day_name"] = fact["transaction_date"].dt.day_name()
q9 = fact.groupby("day_name") \
         .agg(avg_daily_revenue=("revenue","mean"),
              avg_daily_transactions=("transaction_id","nunique"),
              total_revenue=("revenue","sum"))
q9.to_csv(f"{OUT}/query9_day_of_week_pattern.csv")

# 🔟 Discount Impact
fact["discount_range"] = pd.cut(
    fact["discount_percentage"],
    bins=[0,10,25,50,100],
    labels=["0-10%","11-25%","26-50%","50%+"]
)

q10 = fact.groupby("discount_range") \
          .agg(avg_discount_pct=("discount_percentage","mean"),
               total_quantity_sold=("quantity","sum"),
               total_revenue=("revenue","sum"),
               avg_line_total=("revenue","mean"))
q10.to_csv(f"{OUT}/query10_discount_impact.csv")

print("All 10 analytics generated correctly")
