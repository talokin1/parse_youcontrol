import pandas as pd
import re

def detect_acquiring_transactions(df: pd.DataFrame):
    """
    Визначає еквайрингові транзакції за текстом PLATPURPOSE
    та агрегує їх по отримувачу (CONTRAGENTB, CONTRAGENTBIDENTIFIER).
    """

    # --- 1. Ключові фрази (UA + EN) ---
    patterns = [
        # 🇺🇦 українські
        "еквайринг", "екваїринг", "інтернет-еквайринг", "виторг за картками",
        "платіжний термінал", "кошти від покупців", "продаж через pos",
        "надходження від покупців", "оплата карткою", "переказ за pos",
        # 🇬🇧 англійські
        "acquiring", "merchant", "pos terminal", "internet acquiring",
        "card revenue", "card sales", "customer payments", "terminal payment",
        "purchase via pos", "sales via pos", "card income", "card payment"
    ]
    regex = "|".join([re.escape(p) for p in patterns])

    # --- 2. Створюємо прапорець ---
    df["is_acquiring_related"] = (
        df["PLATPURPOSE"]
        .fillna("")
        .str.lower()
        .str.contains(regex)
    )

    acquiring_df = df[df["is_acquiring_related"]].copy()

    # --- 3. Агрегуємо по отримувачу ---
    agg = (
        acquiring_df.groupby(
            ["CONTRAGENTBIDENTIFIER", "CONTRAGENTB"], dropna=False
        )
        .agg(
            n_txn=("PLATPURPOSE", "count"),
            total_sum=("SUMMAEQ", "sum"),
            example_purpose=("PLATPURPOSE", lambda x: x.iloc[0][:100] + "..." if len(x.iloc[0]) > 100 else x.iloc[0]),
        )
        .reset_index()
        .sort_values("total_sum", ascending=False)
    )

    return acquiring_df, agg


# === Приклад використання ===
# df = pd.read_csv("transactions.csv")
# acquiring_df, agg = detect_acquiring_transactions(df)

# print("🔹 Знайдено еквайрингових транзакцій:", len(acquiring_df))
# print("🔹 Топ-10 потенційних клієнтів для еквайрингу:")
# print(agg.head(10))
