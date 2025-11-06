import pandas as pd
import re

# === 1. Шляхи до parquet-файлів ===
files = [
    r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_02.parquet",
    r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_03.parquet",
    r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_04.parquet",
    r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_05.parquet",
    r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_06.parquet",
    r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_08.parquet",
    r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_09.parquet",
    r"M:\Controlling\Data_Science_Projects\Corp_Churn\Data\Raw\data_trxs_2025_10.parquet",
]

# === 2. Функція для пошуку еквайрингових self-переказів ===
def find_self_acquiring_clients(df: pd.DataFrame, period_label: str):
    patterns = [
        "еквайринг", "екваїринг", "інтернет-еквайринг", "платіжний термінал",
        "виторг за картками", "надходження від покупців", "кошти від покупців",
        "продаж через pos", "оплата карткою",
        "acquiring", "merchant", "pos terminal", "internet acquiring",
        "card revenue", "card sales", "customer payments",
        "terminal payment", "sales via pos", "card income", "payment card"
    ]
    regex = "|".join([re.escape(p) for p in patterns])

    df = df[df["CONTRAGENTAID"].notna()].copy()  # наші клієнти

    df["is_acquiring_related"] = (
        df["PLATPURPOSE"].fillna("").str.lower().str.contains(regex)
    )

    df["is_self_transfer"] = (
        df["CONTRAGENTAIDENTIFYCODE"].astype(str) == df["CONTRAGENTBIDENTIFYCODE"].astype(str)
    )

    result = df[
        df["is_acquiring_related"] & df["is_self_transfer"]
    ][[
        "CONTRAGENTAIDENTIFYCODE", "CONTRAGENTA",
        "BANKAID", "BANKBID", "SUMMAEQ", "PLATPURPOSE"
    ]].copy()

    result["period"] = period_label
    result["PLATPURPOSE"] = result["PLATPURPOSE"].str.strip()
    result = result.drop_duplicates()

    return result


# === 3. Зчитування та аналіз усіх місяців ===
all_results = []

for path in files:
    match = re.search(r"data_trxs_(\d{4})_(\d{2})", path)
    if match:
        year, month = match.groups()
        period = f"{year}-{month}"
        print(f"📂 Обробляю {period} ...")

        df = pd.read_parquet(path)
        month_df = find_self_acquiring_clients(df, period)
        all_results.append(month_df)

print("✅ Усі файли оброблені")

# === 4. Об'єднання результатів по всіх місяцях ===
merged = pd.concat(all_results, ignore_index=True)

# === 5. Зведена таблиця по кожному клієнту ===
summary = (
    merged.groupby(["CONTRAGENTAIDENTIFYCODE", "CONTRAGENTA"])
    .agg(
        n_txn=("SUMMAEQ", "count"),
        total_sum=("SUMMAEQ", "sum"),
        months_active=("period", "nunique"),
        last_month=("period", "max")
    )
    .reset_index()
    .sort_values("total_sum", ascending=False)
)

# === 6. Збереження результатів ===
merged.to_csv(r"M:\Controlling\Data_Science_Projects\Corp_Churn\Results\self_acquiring_clients_monthly.csv", index=False)
summary.to_csv(r"M:\Controlling\Data_Science_Projects\Corp_Churn\Results\self_acquiring_clients_summary.csv", index=False)

print("📊 Збережено результати у Results/self_acquiring_clients_summary.csv")

# === 7. Перевірка топів ===
print(summary.head(10))
