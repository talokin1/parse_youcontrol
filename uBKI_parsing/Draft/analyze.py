def normalize_bank_name(name: str) -> str:
    if not isinstance(name, str):
        return "Невідомо"
    n = name.lower()
    n = n.replace("'", "").replace("«", "").replace("»", "")
    n = n.replace("ат кб", "").replace("ат", "").replace("пуат", "").strip()
    n = n.replace("банк", "").replace(" ", "")
    # ключові слова для групування
    if "приват" in n:
        return "ПриватБанк"
    if "ощад" in n:
        return "Ощадбанк"
    if "райф" in n or "aval" in n:
        return "Райффайзен Банк"
    if "укргаз" in n:
        return "Укргазбанк"
    if "пумб" in n:
        return "ПУМБ"
    if "акорд" in n:
        return "Акордбанк"
    if "отп" in n:
        return "OTP Банк"
    if "сiч" in n or "рад" in n:
        return "Радабанк"
    return name.strip()
