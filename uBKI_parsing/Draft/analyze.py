import pandas as pd
import re

def parse_kveds(text):
    # Якщо NaN або порожній рядок
    if pd.isna(text) or not isinstance(text, str):
        return [None]*6

    # Розділяємо по крапці з комою
    parts = [p.strip() for p in text.split(';') if p.strip()]

    kved_codes = []
    kved_names = []

    for part in parts[:3]:   # беремо максимум 3 кведи
        # Шукаємо шаблон: 73.11 або 46.39 або 88.99 і т.д.
        match = re.search(r'(\d{2}\.\d{2})', part)
        if match:
            code = match.group(1)
            name = part.split(code, 1)[1].strip()  # назва після коду
        else:
            code = None
            name = part.strip()

        kved_codes.append(code)
        kved_names.append(name)

    # Якщо менше 3 — доповнюємо None
    while len(kved_codes) < 3:
        kved_codes.append(None)
        kved_names.append(None)

    return [
        kved_codes[0], kved_names[0],
        kved_codes[1], kved_names[1],
        kved_codes[2], kved_names[2],
    ]

# застосовуємо
parsed = data["Види діяльності"].apply(parse_kveds)
parsed = pd.DataFrame(parsed.tolist(), columns=[
    "KVED_1", "KVED_NAME_1",
    "KVED_2", "KVED_NAME_2",
    "KVED_3", "KVED_NAME_3"
])

df = pd.concat([data, parsed], axis=1)
