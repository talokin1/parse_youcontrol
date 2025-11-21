def expand_founders_column(df, source_col="Founders", max_items=10):
    """
    Розплющує колонку Founders у окремі колонки Founder_i_*
    max_items — максимальна кількість засновників, яку очікуємо.
    """

    result = {}

    for idx, val in df[source_col].items():
        entry = {}

        # Якщо це не список — пропускаємо
        if not isinstance(val, list):
            result[idx] = entry
            continue

        # Перебираємо всіх засновників (до max_items)
        for i, founder in enumerate(val[:max_items], start=1):
            if not isinstance(founder, dict):
                continue

            entry[f"Founder_{i}_Name"] = founder.get("ПІБ / Назва")
            entry[f"Founder_{i}_Country"] = founder.get("Країна")
            entry[f"Founder_{i}_Contribution"] = founder.get("Розмір внеску")

        result[idx] = entry

    # Перетворюємо словник на DataFrame
    return pd.DataFrame.from_dict(result, orient="index")

df_founders_expanded = expand_founders_column(df, "Founders", max_items=10)

# Об'єднуємо з оригінальним df
df = pd.concat([df, df_founders_expanded], axis=1)
