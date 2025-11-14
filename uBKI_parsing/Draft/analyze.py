# 1. Рахуємо кількість банків для кожного клієнта
bank_counts = merged.groupby("CONTRAGENTAIDENTIFYCODE")["BANKBID"].nunique()

# 2. Вибираємо тільки тих, у кого більше одного банку
multi_bank_clients = bank_counts[bank_counts > 1].index

# 3. Фільтруємо merged
multi_bank_df = merged[merged["CONTRAGENTAIDENTIFYCODE"].isin(multi_bank_clients)]

# mfo_map = словник BANKBID → NAME
mfo_map = mfos.set_index("BANKBID")["NAME"]

multi_bank_summary = (
    multi_bank_df.groupby("CONTRAGENTAIDENTIFYCODE")["BANKBID"]
    .unique()
    .reset_index()
)

multi_bank_summary["BANKS_USED"] = multi_bank_summary["BANKBID"].apply(
    lambda lst: ", ".join(mfo_map.get(b, str(b)) for b in lst)
)
