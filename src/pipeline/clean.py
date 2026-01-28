import pandas as pd
import re

ALLOWED_TIERS = {"BRONZE", "SILVER", "GOLD"}

COUNTRY_MAPPING = {
    "FRANCE": "FR",
    "FRA": "FR",
    "FR": "FR",
    "US": "US",
    "USA": "US",
    "UK": "UK",
    "GB": "UK",
}

DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")
EMAIL_REGEX = re.compile(r".+@.+\..+")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset="customer_id")

    df["full_name"] = df["full_name"].astype(str).str.strip()
    df = df[df["full_name"] != ""]

    df["email"] = df["email"].astype(str).str.strip()
    df = df[df["email"].str.fullmatch(EMAIL_REGEX, na=False)]

    df["signup_date"] = df["signup_date"].astype(str)
    df = df[df["signup_date"].str.match(DATE_REGEX)]

    df["country"] = df["country"].astype(str).str.upper().str.strip()
    df["country"] = df["country"].replace(COUNTRY_MAPPING)
    df = df[df["country"].str.len() == 2]

    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df = df[(df["age"] >= 16) & (df["age"] <= 95)]

    df["last_purchase_amount"] = pd.to_numeric(df["last_purchase_amount"], errors="coerce")
    df = df[df["last_purchase_amount"] >= 0]

    df["loyalty_tier"] = df["loyalty_tier"].astype(str).str.upper().str.strip()
    df = df[df["loyalty_tier"].isin(ALLOWED_TIERS)]

    return df.reset_index(drop=True)
