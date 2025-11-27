import ast
import pandas as pd

def try_parse(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    if x.startswith('[') or x.startswith('{'):
        try:
            return ast.literal_eval(x)
        except:
            return None
    return None

def extract_founders(obj):
    """Повертає список записів, де є ключ 'ПІБ / Назва'."""
    founders = []

    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict) and 'ПІБ / Назва' in item:
                founders.append(item)

    elif isinstance(obj, dict) and 'ПІБ / Назва' in obj:
        founders.append(obj)

    return founders if founders else None

cols_to_scan = df.columns.tolist()

df['Founders_List'] = None

for col in cols_to_scan:
    df[col] = df[col].apply(try_parse)

    df['Founders_List'] = df.apply(
        lambda row: (row['Founders_List'] or []) + (extract_founders(row[col]) or []),
        axis=1
    )

df['Founders_Names'] = df['Founders_List'].apply(
    lambda lst: [d['ПІБ / Назва'] for d in lst] if lst else None
)

max_len = df['Founders_Names'].apply(lambda x: len(x) if x else 0).max()

for i in range(max_len):
    df[f'Founder_{i+1}'] = df['Founders_Names'].apply(
        lambda lst: lst[i] if lst and len(lst) > i else None
    )
