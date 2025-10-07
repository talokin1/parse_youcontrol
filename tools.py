def parse_contact_info(text: str):
    """
    Витягує адресу, телефони, email-и та сайти з CONTACT_INFO.
    """
    res = {"address": np.nan, "phones": [], "emails": [], "websites": []}
    if not isinstance(text, str):
        return res
    t = re.sub(r"\s+", " ", text)
    
    # Адреса
    m = re.search(r"(Місцезнаходження юридичної особи|Місцезнаходження):\s*([^E]+?)(?=(?:E-mail:|Телефон:|Сайт:|$))", t, flags=re.I)
    if m:
        res["address"] = m.group(2).strip(" ;,")
    
    # Email
    res["emails"] = list(dict.fromkeys(re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", t)))
    
    # Телефони
    phones = re.findall(r"(?:\+?\d[\d\-\s]{7,}\d)", t)
    res["phones"] = [re.sub(r"\s+", "", p) for p in phones]
    
    # Сайти
    res["websites"] = re.findall(r"(https?://[^\s,;]+|www\.[^\s,;]+)", t)
    
    return res

def parse_authorised_persons(text: str):
    """
    Витягує уповноважених осіб (ПІБ, роль, дата народження).
    """
    persons = []
    if not isinstance(text, str):
        return persons
    
    t = re.sub(r"\s+", " ", text).strip()
    role_keywords = r"(керівник|підписант|ліквідатор|голова комісії з припинення|виконуючий обов'язки|бухгалтер)"
    pattern = re.compile(r"([А-ЯІЇЄҐA-Z][А-ЯІЇЄҐA-Za-z'\-\.\s]+?)\s*(\d{2}\.\d{2}\.\d{4})?,?\s*" + role_keywords, re.I)
    
    for m in pattern.finditer(t):
        persons.append({
            "name": m.group(1).strip(" ,;"),
            "birthdate": m.group(2),
            "role": m.group(3).lower() if m.group(3) else None
        })
    
    if not persons:
        persons.append({"name": t, "role": None, "birthdate": None})
    return persons


def parse_founders(text: str):
    """
    Витягує засновників (ПІБ, вклад, частка).
    """
    res = []
    if not isinstance(text, str):
        return res
    
    t = re.sub(r"\s+", " ", text)
    blocks = re.split(r"Частка\s*\(%\):", t, flags=re.I)
    
    for b in blocks:
        b = b.strip(" ;,")
        if not b:
            continue
        m_name = re.search(r"^(.+?)(?=\s+Адреса засновника:|\s+Розмір внеску|\s+Частка|\s*$)", b, flags=re.I)
        m_amt = re.search(r"Розмір внеску.*?:\s*([\d\s\.,]+)\s*грн", b, flags=re.I)
        m_pct = re.search(r"([\d,\.]+)\s*%$", b)
        res.append({
            "name": m_name.group(1).strip() if m_name else None,
            "amount_uah": re.sub(r"[^\d,\.]", "", m_amt.group(1)).replace(",", ".") if m_amt else None,
            "share_pct": m_pct.group(1).replace(",", ".") if m_pct else None
        })
    return res


def parse_beneficiaries(text: str):
    """
    Витягує кінцевих бенефіціарів (ПІБ, частку).
    """
    res = []
    if not isinstance(text, str):
        return res
    t = re.sub(r"\s+", " ", text)
    names = re.findall(r"([А-ЯІЇЄҐ'\- ]{3,})\s+(?=Адреса|$)", t)
    shares = re.findall(r"ЧАСТКА\s*[-:]?\s*([\d,\.]+)\s*%", t, flags=re.I)
    for i, name in enumerate(names):
        res.append({"name": name.strip(), "share_pct": shares[i] if i < len(shares) else None})
    if not res:
        m = re.search(r"^(.+?)\s+Адреса засновника", t, flags=re.I)
        if m:
            res.append({"name": m.group(1).strip(), "share_pct": None})
    return res


df = pd.read_csv(INPUT_FILE)
companies = []

for _, row in df.iterrows():
    base = row.to_dict()
    
    # ACTIVITES
    p_code, p_name, secs = parse_activities(base.get("ACTIVITES"))
    base["KVED_PRIMARY_CODE"], base["KVED_PRIMARY_NAME"] = p_code, p_name
    for i, (sc, sn) in enumerate(secs, 1):
        base[f"KVED_SEC_CODE_{i}"] = sc
        base[f"KVED_SEC_NAME_{i}"] = sn
    
    # CONTACT_INFO
    ci = parse_contact_info(base.get("CONTACT_INFO"))
    base["CONTACT_ADDRESS"] = ci["address"]
    for i, em in enumerate(ci["emails"], 1): base[f"CONTACT_EMAIL_{i}"] = em
    for i, ph in enumerate(ci["phones"], 1): base[f"CONTACT_PHONE_{i}"] = ph
    for i, w in enumerate(ci["websites"], 1): base[f"CONTACT_WEBSITE_{i}"] = w
    
    # AUTHORISED_PERSON
    for i, p in enumerate(parse_authorised_persons(base.get("AUTHORISED_PERSON")), 1):
        base[f"AUTH_PERSON_{i}_NAME"] = p["name"]
        base[f"AUTH_PERSON_{i}_ROLE"] = p["role"]
        base[f"AUTH_PERSON_{i}_BIRTHDATE"] = p["birthdate"]
    
    # FOUNDERS
    for i, f in enumerate(parse_founders(base.get("Перелік засновників/учасників юридичної особи")), 1):
        base[f"FOUNDER_{i}_NAME"] = f["name"]
        base[f"FOUNDER_{i}_AMOUNT_UAH"] = f["amount_uah"]
        base[f"FOUNDER_{i}_SHARE_PCT"] = f["share_pct"]
    
    # BENEFICIARIES
    for i, b in enumerate(parse_beneficiaries(base.get("Кінцевий бенефіціарний власник (контролер)")), 1):
        base[f"BENEFICIARY_{i}_NAME"] = b["name"]
        base[f"BENEFICIARY_{i}_SHARE_PCT"] = b["share_pct"]
    
    companies.append(base)

wide = pd.DataFrame(companies)









# Гарантія: одна компанія = один запис
wide_grouped = wide.groupby("EDRPOU_CODE", as_index=False).agg(first_nonnull)

# Збереження
wide_grouped.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Normalized data saved to {OUTPUT_FILE}")
print(f"Створено {wide_grouped.shape[0]} записів і {wide_grouped.shape[1]} колонок")
