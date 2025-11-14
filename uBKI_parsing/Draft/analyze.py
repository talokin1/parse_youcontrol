# 1. Знаходимо клієнтів, які мають більше 1 банку
bank_counts = summary.groupby("CONTRAGENTAIDENTIFYCODE")["BANKBID"].nunique()
multi_bank_clients = bank_counts[bank_counts > 1].index

# 2. Фільтруємо summary тільки на них
multi_bank_full = summary[summary["CONTRAGENTAIDENTIFYCODE"].isin(multi_bank_clients)]

multi_bank_full
