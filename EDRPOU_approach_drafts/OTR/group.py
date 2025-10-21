continents = ['UA', 'EU', 'NA', 'AS', 'AF', 'OC', 'SA']

# 1️⃣ Агрегація середніх по кожному континенту
country_agg_df = (
    df.groupby(["GROUP", "CLUSTER", "KMEANS_CLUSTERS"])
    .agg(
        CLIENTS=('CONTRAGENTID', 'count'),
        **{f'AV_in_{c}': (c, 'mean') for c in continents}  # автоматично
    )
    .reset_index()
)

# 2️⃣ Підрахунок кількості тих, хто має значення > 0 по кожному континенту
counts = []
for c in continents:
    temp = (
        df[df[c] > 0]
        .groupby(["GROUP", "CLUSTER", "KMEANS_CLUSTERS"])
        .agg(**{f'{c}_COUNT': ('CONTRAGENTID', 'count')})
        .reset_index()
    )
    counts.append(temp)

# 3️⃣ Об’єднання всіх підрахунків в один датафрейм
from functools import reduce

country_who_made = reduce(
    lambda left, right: pd.merge(left, right, on=["GROUP", "CLUSTER", "KMEANS_CLUSTERS"], how="outer"),
    counts
)

# 4️⃣ Об’єднання всього разом
final_df = pd.merge(country_agg_df, country_who_made, on=["GROUP", "CLUSTER", "KMEANS_CLUSTERS"], how="outer")

# 5️⃣ Переглянути результат
print(final_df.head())
