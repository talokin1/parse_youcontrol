import pandas as pd

regions = pd.read_excel("regions.xlsx")

# Загальна кількість клієнтів у кожному кластері
totals = {0: 311, 1: 431, 2: 23, 3: 49}

# Копія таблиці для обчислення відсотків
percentages = regions.copy()

# Обчислюємо відсотки для кожного регіону у кожному кластері
for idx, row in regions.iterrows():
    cluster = row["# CLUSTERS"]
    total = totals.get(cluster, 1)  # захист від ділення на нуль
    for col in regions.columns[1:]:
        percentages.loc[idx, col] = (row[col] / total) * 100

# Знаходимо топ-регіони (в межах 90% від максимуму)
leaders = {}

for idx, row in percentages.iterrows():
    cluster = row["# CLUSTERS"]
    region_percentages = row[1:]
    max_percent = region_percentages.max()
    
    # усі регіони, які >= 90% від максимуму
    top_regions = region_percentages[region_percentages >= 0.9 * max_percent]
    top_regions = {col.replace("# ", ""): round(val, 2) for col, val in top_regions.items()}
    
    leaders[cluster] = top_regions

# Вивід результатів
for cluster, tops in leaders.items():
    print(f"=== Кластер {cluster} ===")
    for region, pct in tops.items():
        print(f" {region}: {pct:.2f}%")
    print()
