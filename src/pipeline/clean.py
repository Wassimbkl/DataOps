import pandas as pd
import re
from datetime import datetime

# =========================
# CONSTANTES MÉTIER
# =========================

ALLOWED_TIERS = {"BRONZE", "SILVER", "GOLD"}

COUNTRY_MAPPING = {
    "FRANCE": "FR",
    "FRA": "FR",
    "FR": "FR",
    "US": "US",
    "USA": "US",
    "UK": "UK",
    "GB": "UK",
    "GERMANY": "DE",
    "DEU": "DE",
    "DE": "DE",
    "SPAIN": "ES",
    "ESP": "ES",
    "ES": "ES",
    "ITALY": "IT",
    "ITA": "IT",
    "IT": "IT",
}

VALID_COUNTRY_CODES = {
    "FR", "US", "UK", "DE", "ES", "IT", "BE", "NL", "PT", "CH",
    "AT", "SE", "NO", "DK", "FI", "PL", "CZ", "IE", "GR", "CA",
    "AU", "NZ", "JP", "CN", "IN", "BR", "MX", "AR", "CL", "CO"
}

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
PHONE_REGEX = re.compile(r"^\+?[\d\s\-\(\)]{8,20}$")


# =========================
# FONCTIONS UTILITAIRES
# =========================

def is_valid_date(date_str: str) -> bool:
    """
    Vérifie si une date est valide (YYYY-MM-DD, date réelle, pas future, >= 1900)
    """
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return False

    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        if date_obj > datetime.now():
            return False

        if date_obj.year < 1900:
            return False

        return True

    except ValueError:
        return False


# =========================
# FONCTION PRINCIPALE
# =========================

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage et validation stricte des données clients
    """
    initial_count = len(df)
    print(f"Nombre de lignes initiales: {initial_count}")

    # 1. Suppression des doublons
    df = df.drop_duplicates(subset="customer_id", keep="first")
    print(f"Doublons supprimés: {initial_count - len(df)}")

    # 2. Validation du nom complet
    df["full_name"] = df["full_name"].astype(str).str.strip()
    before = len(df)
    df = df[
        (df["full_name"] != "") &
        (df["full_name"].str.len() >= 2) &
        (~df["full_name"].str.lower().isin(["nan", "null", "none", "n/a"]))
    ]
    print(f" Noms invalides supprimés: {before - len(df)}")

    # 3. Validation email
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    before = len(df)
    df = df[df["email"].str.match(EMAIL_REGEX, na=False)]
    print(f"Emails invalides supprimés: {before - len(df)}")

    # 4. Validation date d'inscription
    df["signup_date"] = df["signup_date"].astype(str).str.strip()
    before = len(df)
    df = df[df["signup_date"].apply(is_valid_date)]
    print(f" Dates invalides supprimées: {before - len(df)}")

    # 5. Validation pays
    df["country"] = df["country"].astype(str).str.upper().str.strip()
    df["country"] = df["country"].replace(COUNTRY_MAPPING)
    before = len(df)

    # Rejet des codes du type AA, BB, FF...
    df = df[~(
        (df["country"].str.len() == 2) &
        (df["country"].str[0] == df["country"].str[1])
    )]

    df = df[df["country"].isin(VALID_COUNTRY_CODES)]
    print(f"Pays invalides supprimés: {before - len(df)}")

    # 6. Validation âge
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    before = len(df)
    df = df[df["age"].between(16, 95)]
    df["age"] = df["age"].astype(int)
    print(f"Ages invalides supprimés: {before - len(df)}")

    # 7. Validation montant dernier achat
    df["last_purchase_amount"] = pd.to_numeric(df["last_purchase_amount"], errors="coerce")
    before = len(df)
    df = df[
        (df["last_purchase_amount"].notna()) &
        (df["last_purchase_amount"] >= 0) &
        (df["last_purchase_amount"] <= 1_000_000)
    ]
    print(f"Montants invalides supprimés: {before - len(df)}")

    # 8. Validation loyalty tier
    df["loyalty_tier"] = df["loyalty_tier"].astype(str).str.upper().str.strip()
    before = len(df)
    df = df[df["loyalty_tier"].isin(ALLOWED_TIERS)]
    print(f"Tiers invalides supprimés: {before - len(df)}")

    # 9. Validation téléphone (optionnel)
    if "phone" in df.columns:
        df["phone"] = df["phone"].astype(str).str.strip()
        df["phone"] = df["phone"].replace({"nan": None})
        before = len(df)
        df = df[
            df["phone"].isna() |
            df["phone"].str.match(PHONE_REGEX, na=False)
        ]
        print(f"Téléphones invalides supprimés: {before - len(df)}")

    print(f"Nombre de lignes finales: {len(df)}")
    print(f"Taux de rejet: {((initial_count - len(df)) / initial_count * 100):.2f}%")

    return df.reset_index(drop=True)
