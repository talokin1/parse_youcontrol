client_bank_map = (
    merged[["CONTRAGENTAIDENTIFYCODE", "BANKBID"]]
    .drop_duplicates()
    .groupby("CONTRAGENTAIDENTIFYCODE")["BANKBID"]
    .unique()
)

client_bank_map
