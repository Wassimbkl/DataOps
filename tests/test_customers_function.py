import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import pytest


from src.pipeline.clean import clean_data



# FIXTURE : ligne valide de référence

@pytest.fixture
def valid_row():
    return {
        "customer_id": 1,
        "full_name": "John Doe",
        "email": "john@example.com",
        "signup_date": "2023-01-01",
        "country": "FR",
        "age": 30,
        "last_purchase_amount": 100.0,
        "loyalty_tier": "GOLD",
    }

#  EMAIL

def test_remove_invalid_email(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "email": "bad-email"}
    ])

    result = clean_data(df)

    assert len(result) == 1
    assert result.iloc[0]["email"] == "john@example.com"


# 2️⃣ FULL NAME

def test_remove_empty_full_name(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "full_name": ""}
    ])

    result = clean_data(df)

    assert len(result) == 1
    assert result.iloc[0]["full_name"] == "John Doe"

# DATE

def test_remove_invalid_date(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "signup_date": "2024-02-30"}
    ])

    result = clean_data(df)

    assert len(result) == 1
    assert result.iloc[0]["signup_date"] == "2023-01-01"


def test_remove_future_date(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "signup_date": "2099-01-01"}
    ])

    result = clean_data(df)

    assert len(result) == 1



# COUNTRY

def test_country_mapping(valid_row):
    df = pd.DataFrame([
        {**valid_row, "country": "FRANCE"}
    ])

    result = clean_data(df)

    assert result.iloc[0]["country"] == "FR"


def test_reject_invalid_country_code(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "country": "AA"}
    ])

    result = clean_data(df)

    assert len(result) == 1



#  AGE

def test_reject_age_out_of_range(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "age": 12}
    ])

    result = clean_data(df)

    assert len(result) == 1
    assert result.iloc[0]["age"] == 30


def test_reject_non_numeric_age(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "age": "abc"}
    ])

    result = clean_data(df)

    assert len(result) == 1



# AMOUNT

def test_reject_negative_amount(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "last_purchase_amount": -10}
    ])

    result = clean_data(df)

    assert len(result) == 1


def test_reject_too_large_amount(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "last_purchase_amount": 2_000_000}
    ])

    result = clean_data(df)

    assert len(result) == 1


# LOYALTY TIER

def test_reject_invalid_loyalty_tier(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row, "customer_id": 2, "loyalty_tier": "PLATINUM"}
    ])

    result = clean_data(df)

    assert len(result) == 1
    assert result.iloc[0]["loyalty_tier"] == "GOLD"

# 8️DUPLICATES

def test_duplicate_customer_id_removed(valid_row):
    df = pd.DataFrame([
        valid_row,
        {**valid_row}  # même customer_id
    ])

    result = clean_data(df)

    assert len(result) == 1



# 9️VALID ROW IS KEPT

def test_valid_row_is_kept(valid_row):
    df = pd.DataFrame([valid_row])

    result = clean_data(df)

    assert len(result) == 1
    assert result.iloc[0]["customer_id"] == 1
