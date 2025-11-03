import pandas as pd

# 1️⃣ Визначаємо топ-5 мерчантів у кожному кластері
top_merchants = (
    contragents_stats
    .groupby("CLUSTERS", group_keys=False)
    .apply(lambda g: g.nlargest(5, "MERCHANT_COUNT"))
    .copy()
)

# 2️⃣ Розраховуємо середній чек
top_merchants["AVG_CHECK"] = top_merchants["SUMMAEQ"] / top_merchants["MERCHANT_COUNT"]

# 3️⃣ Формуємо красивий текстовий звіт
for cluster, group in top_merchants.groupby("CLUSTERS"):
    print(f"\n🟩=== Кластер {cluster} ===")
    for i, row in enumerate(group.itertuples(index=False), start=1):
        print(f"{i}) {row.MERCHANT_NAME_NORM:<15} | {int(row.MERCHANT_COUNT):>5} візитів | "
              f"Середній чек: {row.AVG_CHECK:,.2f} грн | Категорія: {row.MCC_GROUPS}")
