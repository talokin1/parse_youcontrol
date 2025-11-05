# --- Вихідні дані ---
foods = mcc_lst
temp1 = df_mcc[df_mcc["MCC"].isin(foods)]

# --- Підрахунок кількості клієнтів, які користувалися food shops у кожному кластері ---
food_cluster_sizes = (
    temp1.groupby("CLUSTERS")["CONTRAGENTID"].nunique().to_dict()
)

# --- Основна агрегація ---
contragents_stats = (
    temp1.groupby(["CLUSTERS", "MERCHANT_NAME_NORM", "MCC_GROUPS"])
    .agg(
        MERCHANT_COUNT=("MERCHANT_NAME_NORM", "count"),       # усі транзакції
        CLIENTS=("CONTRAGENTID", "nunique"),                   # унікальні клієнти у фудшопі
        SUMMAEQ=("SUMMAEQ", "mean"),                           # середній чек
    )
    .reset_index()
)

# --- Додаємо середню кількість візитів на клієнта для кожного фудшопу ---
contragents_stats["VISITS_PER_CLIENT"] = (
    contragents_stats["MERCHANT_COUNT"] / contragents_stats["CLIENTS"]
)

# --- Додаємо відсоток клієнтів серед тих, хто користувався food shops ---
contragents_stats["CLIENTS_PERCENT"] = contragents_stats.apply(
    lambda row: row["CLIENTS"] / food_cluster_sizes[row["CLUSTERS"]] * 100,
    axis=1
)

# --- Сортуємо за популярністю (часткою клієнтів серед food) ---
contragents_stats = contragents_stats.sort_values(
    ["CLUSTERS", "CLIENTS_PERCENT"], ascending=[True, False]
)

# --- Топ-5 фудшопів у кожному кластері ---
top_merchants = (
    contragents_stats.groupby("CLUSTERS", group_keys=False)
    .apply(lambda g: g.nlargest(5, "CLIENTS_PERCENT"))
    .copy()
)

# --- Вивід ---
for cluster, group in top_merchants.groupby("CLUSTERS"):
    total_food_users = food_cluster_sizes[cluster]
    print(f"\n=== Кластер {cluster} ===")
    print(f"Клієнтів, що користувалися food shops: {total_food_users}")

    for i, row in enumerate(group.itertuples(index=False), start=1):
        print(
            f"{i}. {row.MERCHANT_NAME_NORM:<25} | "
            f"Клієнтів: {row.CLIENTS_PERCENT:>5.1f}% | "
            f"Візитів/клієнта: {row.VISITS_PER_CLIENT:>5.2f} | "
            f"Середній чек: {humanize_number(row.SUMMAEQ)} грн"
        )
