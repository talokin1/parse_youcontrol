foods = mcc_lst
temp1 = df_mcc[df_mcc["MCC"].isin(foods)]

# --- Основна агрегація по мерчантам ---
contragents_stats = (
    temp1.groupby(["CLUSTERS", "MERCHANT_NAME_NORM", "MCC_GROUPS"])
    .agg(
        MERCHANT_COUNT=("MERCHANT_NAME_NORM", "count"),
        SUMMAEQ=("SUMMAEQ", "mean"),
    )
    .reset_index()
    .sort_values("MERCHANT_COUNT", ascending=False)
)

# --- Топ 5 мерчантів у кожному кластері ---
top_merchants = (
    contragents_stats.groupby("CLUSTERS", group_keys=False)
    .apply(lambda g: g.nlargest(5, "MERCHANT_COUNT"))
    .copy()
)

# --- Розрахунок кількості візитів на одного клієнта у кожному кластері ---
visits_per_cluster = (
    temp1.groupby("CLUSTERS")
    .agg(
        TOTAL_VISITS=("MERCHANT_NAME_NORM", "count"),
        UNIQUE_CLIENTS=("CLIENT_ID", "nunique")
    )
    .reset_index()
)

visits_per_cluster["VISITS_PER_CLIENT"] = (
    visits_per_cluster["TOTAL_VISITS"] / visits_per_cluster["UNIQUE_CLIENTS"]
)

# --- Вивід результатів ---
for cluster, group in top_merchants.groupby("CLUSTERS"):
    print(f"\n=== Кластер {cluster} ===")

    # Топ мерчанти
    for i, row in enumerate(group.itertuples(index=False), start=1):
        print(
            f"{i}. {row.MERCHANT_NAME_NORM:<30} "
            f"{humanize_number(row.MERCHANT_COUNT):>6} візитів | "
            f"Середній чек: {humanize_number(row.SUMMAEQ)} грн"
        )

    # Візити на клієнта
    visits_row = visits_per_cluster.loc[
        visits_per_cluster["CLUSTERS"] == cluster
    ].iloc[0]

    print(
        f"\n➡️ Всього візитів: {humanize_number(visits_row.TOTAL_VISITS)} | "
        f"Клієнтів: {humanize_number(visits_row.UNIQUE_CLIENTS)} | "
        f"Візитів на 1 клієнта: {visits_row.VISITS_PER_CLIENT:.2f}\n"
    )
