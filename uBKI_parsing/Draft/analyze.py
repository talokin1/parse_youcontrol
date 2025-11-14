client_stats = (
    merged.groupby("CONTRAGENTAIDENTIFYCODE")
    .agg(
        n_txn=("SUMMAEQ", "count"),
        total_sum=("SUMMAEQ", "sum"),
        months_active=("PERIOD", "nunique"),
        last_month=("PERIOD", "max"),
        CONTRAGENTASNAME=("CONTRAGENTASNAME", "first"),
        CONTRAGENTAID=("CONTRAGENTAID", "first")
    )
    .reset_index()
)

banks_per_client = (
    merged.groupby("CONTRAGENTAIDENTIFYCODE")["BANKBID"]
    .unique()
    .reset_index()
)

bank_names = mfos.set_index("BANKBID")["NAME"]

banks_per_client["BANKS_USED"] = banks_per_client["BANKBID"].apply(
    lambda lst: ", ".join(bank_names.get(x, str(x)) for x in lst)
)

summary_v2 = client_stats.merge(
    banks_per_client[["CONTRAGENTAIDENTIFYCODE", "BANKS_USED"]],
    on="CONTRAGENTAIDENTIFYCODE",
    how="left"
)

summary_v2 = summary_v2.sort_values("total_sum", ascending=False)
