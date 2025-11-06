import pandas as pd
import re

def find_self_acquiring_clients(df: pd.DataFrame):
    """
    Визначає корпоративних клієнтів банку, які користуються еквайрингом інших банків,
    і отримують кошти на власний рахунок (self-transfer).
    """

    # --- 1. Ключові патерни еквайрингу ---
    patterns = [
        "еквайринг", "екваїринг", "інтернет-еквайринг", "платіжний термінал",
        "виторг за картками", "надходження від покупців", "кошти від покупців",
        "продаж через pos", "оплата карткою",
        "acquiring", "merchant", "pos terminal", "internet acquiring",
        "card revenue", "card sales", "customer payments",
        "terminal payment", "sales via pos", "card income", "payment card"
    ]
    regex = "|".join([re.escape(p) for p in patterns])

    # --- 2. Фільтруємо наших клієнтів (з ненульовим CONTRAGENTAID) ---
    df = df[df["CONTRAGENTAID"].notna()].copy()

    # --- 3. Ознаки еквайрингу в призначенні ---
    df["is_acquiring_related"] = (
        df["PLATPURPOSE"].fillna("").str.lower().str.contains(regex)
    )

    # --- 4. Клієнт перекидає сам собі ---
    df["is_self_transfer"] = (
        df["CONTRAGENTAIDENTIFYCODE"].astype(str) == df["CONTRAGENTBIDENTIFYCODE"].astype(str)
    )

    # --- 5. Залишаємо тільки потрібні кейси ---
    result = df[
        df["is_acquiring_related"] & df["is_self_transfer"]
    ][[
        "CONTRAGENTAIDENTIFYCODE", "CONTRAGENTA",
        "BANKAID", "BANKBID",
        "SUMMAEQ", "PLATPURPOSE"
    ]].copy()

    # --- 6. Прибираємо дублі та очищаємо ---
    result["PLATPURPOSE"] = result["PLATPURPOSE"].str.strip()
    result = result.drop_duplicates()

    return result


# === 🔧 Приклад використання ===
# df = pd.read_parquet(r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_10.parquet")
# self_acquiring_clients = find_self_acquiring_clients(df)
# print(self_acquiring_clients.head(10))
