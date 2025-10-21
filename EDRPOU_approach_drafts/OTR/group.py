from functools import reduce

continents = ['UA', 'EU', 'NA', 'AS', 'AF', 'OC', 'SA']
groups = []

for c in continents:
    grp = (
        df[df[c] > 0]
        .groupby(["GROUP", "CLUSTER", "KMEANS_CLUSTERS"], as_index=False)
        .agg(
            **{
                f"AV_in_{c}_who_made": (c, "mean"),
                f"{c}_COUNT": ("CONTRAGENTID", "count")
            }
        )
    )
    groups.append(grp)

# Об’єднуємо всі датафрейми
country_who_made = reduce(
    lambda left, right: pd.merge(left, right, on=["GROUP", "CLUSTER", "KMEANS_CLUSTERS"], how="outer"),
    groups
)

country_who_made.head()
