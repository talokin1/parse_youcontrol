import ast
import pandas as pd

# -------------------------
# 1. Безпечний парсер
# -------------------------

def safe_parse(val):
    """Парсить значення у Python структуру або повертає None."""
    if isinstance(val, (list, dict)):
        return val
    if not isinstance(val, str):
        return None

    val = val.strip()
    if val in ["", "[]", "nan", "None", "Null"]:
        return None
    
    try:
        return ast.literal_eval(val)
    except:
        return None


# -------------------------
# 2. Знаходимо всі колонки, де є ПІБ / Назва
# -------------------------

def find_columns_with_founders(df):
    founder_columns = []

    for col in df.columns:
        for val in df[col].head(5000):  # прискорюємо, але можна прибрати обмеження
            parsed = safe_parse(val)
            if isinstance(parsed, list):
                if any(isinstance(item, dict) and "ПІБ / Назва" in item for item in parsed):
                    founder_columns.append(col)
                    break
            elif isinstance(parsed, dict):
                if "ПІБ / Назва" in parsed:
                    founder_columns.append(col)
                    break

    return founder_columns


# -------------------------
# 3. Створюємо колонку Founders
# -------------------------

def extract_all_founders(df, founder_cols):
    founders_list = []

    for idx, row in df.iterrows():
        combined = []

        for col in founder_cols:
            parsed = safe_parse(row[col])

            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict) and "ПІБ / Назва" in item:
                        combined.append(item)

            elif isinstance(parsed, dict):
                if "ПІБ / Назва" in parsed:
                    combined.append(parsed)

        founders_list.append(combined)

    df["Founders"] = founders_list
    return df


# ------------------------------------------------------
# ▶︎ Повний запуск
# ------------------------------------------------------

# 1) знаходимо колонки з даними про засновників
founder_cols = find_columns_with_founders(df)
print("🔎 Колонки, де знайдено ПІБ / Назва:")
print(founder_cols)

# 2) створюємо колонку Founders з усіх цих колонок
df = extract_all_founders(df, founder_cols)

# Перевірка
df["Founders"].head(10)
