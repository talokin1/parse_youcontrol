import ast

def parse_list_column(obj):
    """Гарантовано повертає список словників або порожній список."""
    if obj is None:
        return []

    # Якщо вже список — повертаємо
    if isinstance(obj, list):
        return obj

    # Якщо число — одразу повертаємо порожній
    if isinstance(obj, (int, float)):
        return []

    # Якщо NaN
    if obj != obj:  # NaN != NaN
        return []

    # Якщо текст
    if isinstance(obj, str):
        obj = obj.strip()
        if obj == "" or obj.lower() in ["nan", "none", "null", "[]", "{}"]:
            return []
        try:
            parsed = ast.literal_eval(obj)
            if isinstance(parsed, list):
                return parsed
            else:
                return []
        except:
            return []

    return []

def expand_founders(df):
    max_items = 5
    out = {}

    for idx, row in df["Засновники"].items():
        founders = parse_list_column(row)
        record = {}

        for i, f in enumerate(founders[:max_items], start=1):
            if not isinstance(f, dict):
                continue
            record[f"Founder_{i}_Name"] = f.get("ПІБ / Назва")
            record[f"Founder_{i}_Country"] = f.get("Країна")
            record[f"Founder_{i}_Contribution"] = f.get("Розмір внеску")

        out[idx] = record

    return pd.DataFrame.from_dict(out, orient="index")

def expand_authorized(df):
    max_items = 5
    out = {}

    for idx, row in df["Уповноважені особи"].items():
        people = parse_list_column(row)
        record = {}

        for i, p in enumerate(people[:max_items], start=1):
            if not isinstance(p, dict):
                continue
            record[f"Authorized_{i}_Name"] = p.get("ПІБ")
            record[f"Authorized_{i}_Role"] = p.get("Роль")

        out[idx] = record

    return pd.DataFrame.from_dict(out, orient="index")

def expand_beneficiaries(df):
    max_items = 5
    out = {}

    for idx, row in df["Бенефіціари"].items():
        bens = parse_list_column(row)
        record = {}

        for i, b in enumerate(bens[:max_items], start=1):
            if not isinstance(b, dict):
                continue
            record[f"Benef_{i}_Name"] = b.get("ПІБ")
            record[f"Benef_{i}_Country"] = b.get("Країна")
            record[f"Benef_{i}_Share"] = b.get("Частка")

        out[idx] = record

    return pd.DataFrame.from_dict(out, orient="index")


