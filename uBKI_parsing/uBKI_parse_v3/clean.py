import ast
import pandas as pd

def safe_parse(val):
    """Парсить значення в Python-об'єкт якщо можливо."""
    if isinstance(val, (list, dict)):
        return val
    if not isinstance(val, str):
        return None
    try:
        return ast.literal_eval(val)
    except:
        return None


def find_founder_key(df):
    """
    Шукає ключ 'ПІБ / Назва' у будь-якому стовпчику.
    Повертає словник: {назва_стовпця: [індекси_рядків_де_знайдено]}.
    """
    result = {}

    for col in df.columns:
        matches = []

        for idx, val in df[col].items():
            parsed = safe_parse(val)

            # якщо це список елементів
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict) and "ПІБ / Назва" in item:
                        matches.append(idx)
                        break  # один збіг достатній

            # якщо це один dict
            elif isinstance(parsed, dict):
                if "ПІБ / Назва" in parsed:
                    matches.append(idx)

        if matches:
            result[col] = matches

    return result


# ▶ Запуск
founder_locations = find_founder_key(df)

# Виводимо результат
for col, rows in founder_locations.items():
    print(f"🔎 Знайдено ПІБ / Назва у колонці: {col}")
    print(f"   ▸ Кількість рядків: {len(rows)}")
    print(f"   ▸ Перші 10 індексів: {rows[:10]}")
    print()
