import pandas as pd

# 1️⃣ Словник відповідності ID → Назва області
region_dict = {
    1: "АР Крим",
    2: "Вінницька",
    3: "Волинська",
    4: "Дніпропетровська",
    5: "Донецька",
    6: "Житомирська",
    # ... допиши решту при потребі
}

# 2️⃣ Додаємо назву регіону у DataFrame
df["region"] = df["ADDRESS_ID"].map(region_dict)

# 3️⃣ Агрегуємо кількість клієнтів по кожному кластеру та регіону
region_info = (
    df.groupby(["region", "CLUSTER"])
    .agg(CLIENTS=("CONTRAGENTID", "count"))
    .reset_index()
)

# 4️⃣ Робимо зведену таблицю (кластер — рядки, регіони — стовпці)
region_pivot = (
    pd.pivot_table(
        region_info,
        index="CLUSTER",
        columns="region",
        values="CLIENTS",
        fill_value=0
    )
    .reset_index()
)

# 5️⃣ Зберігаємо результат
region_pivot.to_excel("regions.xlsx", index=False)

print("✅ Таблицю regions.xlsx успішно створено!")
