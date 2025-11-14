# 1. Назви банків
bank_names = mfos.set_index("BANKBID")["NAME"]

# 2. Групуємо клієнта → всі банки
banks_per_client = (
    merged.groupby("CONTRAGENTAIDENTIFYCODE")["BANKBID"]
          .unique()
          .reset_index()
)

banks_per_client["BANKS_USED"] = banks_per_client["BANKBID"].apply(
    lambda lst: ", ".join(bank_names.get(x, str(x)) for x in lst)
)

# 3. Агрегація по клієнту
summary = (
    merged.groupby("CONTRAGENTAIDENTIFYCODE")
    .agg({
        "CONTRAGENTASNAME": "first",
        "CONTRAGENTAID": "first",
        "total_sum": "sum",
        "n_txn": "sum",
        "months_active": "max",
        "last_month": "max"
    })
    .reset_index()
)

# 4. Додаємо банки
summary = summary.merge(banks_per_client[["CONTRAGENTAIDENTIFYCODE", "BANKS_USED"]],
                        on="CONTRAGENTAIDENTIFYCODE", how="left")

summary
