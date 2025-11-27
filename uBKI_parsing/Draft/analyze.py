import ast
import pandas as pd

def safe_parse(x):
    """Надійно парсить рядки типу '[{...}]' або '{...}'. 
    Ніколи не падає і не викликає ambiguous truth value."""
    
    # 1. Якщо це NaN
    try:
        if pd.isna(x):
            return None
    except:
        pass  # x не скаляр — просто ігноруємо

    # 2. Перетворюємо у строку
    try:
        s = str(x).strip()
    except:
        return None

    # 3. Перевірка що схоже на JSON-like структуру
    if not ((s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}"))):
        return None

    # 4. Пробуємо парсити
    try:
        return ast.literal_eval(s)
    except:
        return None



def find_columns_with_founders(df):
    founder_cols = []

    for col in df.columns:
        for val in df[col]:
            parsed = safe_parse(val)

            # Format: {'ПІБ / Назва': '...'}
            if isinstance(parsed, dict) and "ПІБ / Назва" in parsed:
                founder_cols.append(col)
                break

            # Format: [{'ПІБ / Назва': '...'}, ...]
            if isinstance(parsed, list):
                if any(isinstance(item, dict) and "ПІБ / Назва" in item for item in parsed):
                    founder_cols.append(col)
                    break

    return founder_cols


def extract_all_founders(df, founder_cols):
    """Створює df['Founders'] як список словників засновників."""

    result = []

    for idx, row in df.iterrows():
        combined = []

        for col in founder_cols:
            parsed = safe_parse(row[col])

            # dict
            if isinstance(parsed, dict) and "ПІБ / Назва" in parsed:
                combined.append(parsed)

            # list of dicts
            elif isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict) and "ПІБ / Назва" in item:
                        combined.append(item)

        result.append(combined)

    df["Founders"] = result
    return df

def expand_founders_column(df, source_col="Founders", max_items=10):
    result = {}

    for idx, founders in df[source_col].items():
        entry = {}

        if isinstance(founders, list):
            for i, founder in enumerate(founders[:max_items], start=1):
                entry[f"Founder_{i}"] = founder.get("ПІБ / Назва")

        result[idx] = entry

    return pd.DataFrame.from_dict(result, orient="index")

def parse_founders(df, max_founders=10):
    """Повний ETL-процес по витягненню засновників у правильні колонки."""

    # 1. Знайти колонки з засновниками
    founder_cols = find_columns_with_founders(df)
    print("🔍 Знайдені колонки з засновниками:", founder_cols)

    # 2. Створити колонку Founders = список dictів
    df = extract_all_founders(df, founder_cols)

    # 3. Розкласти у Founder_1, Founder_2, …
    df_expanded = expand_founders_column(df, "Founders", max_items=max_founders)

    # 4. Додати їх назад у датафрейм
    df = pd.concat([df, df_expanded], axis=1)

    return df

df = parse_founders(df, max_founders=10)
