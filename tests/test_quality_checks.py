import pandas as pd

def test_no_null_customer_ids():
    df = pd.read_csv("data/raw/customers.csv")
    assert df["customer_id"].isnull().sum() == 0
