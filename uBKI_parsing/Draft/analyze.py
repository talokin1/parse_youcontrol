import pandas as pd
import re

def detect_acquiring_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Визначає еквайрингові транзакції за текстом у полі PLATPURPOSE
    і агрегує по отримувачу (CONTRAGENTB).
    """

    # --- 1. Ключові патерни (UA + EN) ---
    patterns = [
        # 🇺🇦 українські
        "еквайринг", "екваїринг", "pos", "інтернет-еквайринг",
        "платіжний термінал", "виторг за картками", "кошти від покупців",
        "надходження від покупців", "продаж через pos", "оплата карткою",
        # 🇬🇧 англійські
        "acquiring", "merchant", "pos terminal", "internet acquiring",
        "card revenue", "card sales", "customer payments",
        "transaction fee", "terminal payment", "purchase via pos",
        "sales via pos", "payment card", "card income"
    ]
    regex = "|".join([re.escape(p) for p in patterns])

    # --- 2. Детекція ---
    df["is_acquiring_related"] = (
        df["PLATPURPOSE"]
        .fillna("")
        .str.lower()
        .str.contains(regex)
    )

    acquiring_df = df[df["is_acquiring_related"]].copy()

    # --- 3. Агрегація по отримувачу ---
    agg = (
        acquiring_df.groupby("CONTRAGENTB")
        .agg(
            n_txn=("PLATPURPOSE", "count"),
            total_sum=("AMOUNT", "sum"),
            example_purpose=("PLATPURPOSE", lambda x: x.iloc[0][:120] + "..." if len(x.iloc[0]) > 120 else x.iloc[0])
        )
        .reset_index()
        .sort_values("total_sum", ascending=False)
    )

    return agg, acquiring_df




agg, acquiring_df = detect_acquiring_transactions(df)

print("🔹 Топ-10 потенційних клієнтів для еквайрингу:")
print(agg.head(10))
