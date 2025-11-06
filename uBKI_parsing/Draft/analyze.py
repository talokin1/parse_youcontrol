import pandas as pd
import re

def find_clients_using_foreign_acquiring(df: pd.DataFrame):
    """
    Визначає корпоративних клієнтів банку, які користуються еквайрингом інших банків.
    """

    # 1️⃣ Патерни еквайрингу
    patterns = [
        "еквайринг", "екваїринг", "інтернет-еквайринг", "платіжний термінал",
        "виторг за картками", "надходження від покупців", "кошти від покупців",
        "продаж через pos", "оплата карткою",
        "acquiring", "merchant", "pos terminal", "internet acquiring",
        "card revenue", "card sales", "customer payments",
        "terminal payment", "sales via pos", "card income", "payment card"
    ]
    regex = "|".join([re.escape(p) for p in patterns])

    # 2️⃣ Фільтр: наш клієнт і призначення з ознаками еквайрингу
    mask = (
        df["CONTRAGENTAID"].notna() &  # наш клієнт
        df["PLATPURPOSE"].fillna("").str.lower().str.contains(regex)
    )

    acquiring_clients = df.loc[mask, [
        "CONTRAGENTAIDENTIFYCODE", "CONTRAGENTA", "BANKAID", "BANKBID",
        "SUMMAEQ", "PLATPURPOSE"
    ]].copy()

    # 3️⃣ Нормалізація
    acquiring_clients["PLATPURPOSE"] = acquiring_clients["PLATPURPOSE"].str.strip()
    acquiring_clients = acquiring_clients.drop_duplicates()

    return acquiring_clients


# === 🔧 Приклад використання ===
# df = pd.read_parquet(r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_10.parquet")
# clients_using_other_acquiring = find_clients_using_foreign_acquiring(df)

# === Топ-20 за сумою ===
# clients_using_other_acquiring.groupby("CONTRAGENTA").agg({"SUMMAEQ": "sum"}).sort_values("SUMMAEQ", ascending=False).head(20)
