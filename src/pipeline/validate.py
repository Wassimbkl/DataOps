def validate_data(df):
    assert df["customer_id"].notna().all()
    assert df["email"].str.contains("@").all()
    assert (df["last_purchase_amount"] >= 0).all()
