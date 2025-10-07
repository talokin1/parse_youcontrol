import re

def parse_authorised_persons(text: str):
    """
    Парсить рядок з уповноваженими особами.
    Витягує ПІБ, дату народження та роль.
    """
    persons = []
    if not isinstance(text, str) or text.strip() == "":
        return persons
    
    # Нормалізація пробілів
    t = re.sub(r"\s+", " ", text).strip()
    
    # Патерн: ІМ'Я [дата], роль
    pattern = re.compile(
        r"([А-ЯІЇЄҐA-Z'][А-ЯІЇЄҐA-Z' \-]+?)"            # ПІБ (кілька великих слів)
        r"(?:\s+(\d{2}\.\d{2}\.\d{4}))?"                # необов'язкова дата
        r",?\s*(керівник|підписант|ліквідатор|"
        r"голова комісії з припинення|виконуючий обов'язки|бухгалтер)?", 
        re.IGNORECASE
    )
    
    for m in pattern.finditer(t):
        name = m.group(1).strip(" ,;")
        birthdate = m.group(2)
        role = m.group(3).lower() if m.group(3) else None
        persons.append({"name": name, "birthdate": birthdate, "role": role})
    
    # fallback — якщо парсер нічого не знайшов
    if not persons:
        persons.append({"name": t, "birthdate": None, "role": None})
    
    return persons


# Парсимо і додаємо нові колонки
for i, row in df.iterrows():
    persons = parse_authorised_persons(row["Уповноважені особи"])
    for idx, p in enumerate(persons, start=1):
        df.loc[i, f"AUTH_PERSON_{idx}_NAME"] = p["name"]
        df.loc[i, f"AUTH_PERSON_{idx}_BIRTHDATE"] = p["birthdate"]
        df.loc[i, f"AUTH_PERSON_{idx}_ROLE"] = p["role"]


df[
    ["Уповноважені особи", 
     "AUTH_PERSON_1_NAME", "AUTH_PERSON_1_BIRTHDATE", "AUTH_PERSON_1_ROLE",
     "AUTH_PERSON_2_NAME", "AUTH_PERSON_2_BIRTHDATE", "AUTH_PERSON_2_ROLE"]
].head(10)
