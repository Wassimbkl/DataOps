import pandas as pd
import os
import re

DATA_DIR = "data/processed"
EXPECTED_COLUMNS = [
    "customer_id",
    "full_name",
    "email",
    "signup_date",
    "country",
    "age",
    "last_purchase_amount",
    "loyalty_tier",
]

EMAIL_REGEX = re.compile(r".+@.+\..+")
DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def load_all_clean_files():
    dfs = []
    for file in os.listdir(DATA_DIR):
        if file.endswith(".csv"):
            dfs.append(pd.read_csv(os.path.join(DATA_DIR, file)))
    return dfs


def test_files_exist():
    assert os.path.exists(DATA_DIR)
    assert len(os.listdir(DATA_DIR)) > 0


def test_columns_present():
    for df in load_all_clean_files():
        assert list(df.columns) == EXPECTED_COLUMNS


def test_email_valid():
    for df in load_all_clean_files():
        assert df["email"].str.match(EMAIL_REGEX).all()


def test_signup_date_format():
    for df in load_all_clean_files():
        assert df["signup_date"].str.match(DATE_REGEX).all()


def test_age_valid():
    for df in load_all_clean_files():
        assert df["age"].notna().all()
        assert (df["age"] >= 1).all()
        assert (df["age"] <= 100).all()


def test_amount_valid():
    for df in load_all_clean_files():
        assert df["last_purchase_amount"].notna().all()
        assert (df["last_purchase_amount"] >= 0).all()


def test_no_empty_rows():
    for df in load_all_clean_files():
        assert not df.isnull().all(axis=1).any()
