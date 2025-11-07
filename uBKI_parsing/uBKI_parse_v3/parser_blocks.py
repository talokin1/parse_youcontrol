from bs4 import BeautifulSoup
import re
import cloudscraper

def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def parse_ubki_universal(soup, edrpou):
    """Парсить дані про компанію UBKI за ЄДРПОУ"""
    url = f"https://edrpou.ubki.ua/ua/{edrpou}"
    scraper = cloudscraper.create_scraper()
    html = scraper.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    card = soup.find("div", id="anchor_ident")
    if not card:
        print(f"[!] Дані не знайдено для {edrpou}")
        return None

    data = {"ЄДРПОУ": edrpou}

    # --- Основна інформація ---
    header_block = card.find("div", class_="dr_row_spacebetween")
    if header_block:
        title = header_block.find("h2", class_="dr_value_title")
        if title:
            data["Повна назва"] = " ".join(title.get_text().split())

        status = header_block.find("div", class_="dr_value_state")
        if status:
            data["Статус"] = " ".join(status.get_text().split())

        for div in header_block.select(".dr_margin_16"):
            txt = " ".join(div.get_text().split())
            if "Актуально" in txt:
                data["Актуально на"] = txt.replace("Актуально на", "").strip(". ")
            elif "Останні зміни" in txt:
                data["Останні зміни"] = txt.replace("Останні зміни", "").strip(". ")

    # --- Простi поля ---
    for block in card.select(".dr_column.dr_padding_1"):
        subtitle = block.find("div", class_="dr_value_subtitle")
        value = block.find("div", class_="dr_value")
        if subtitle and value:
            key = subtitle.get_text(strip=True)
            val = " ".join(value.get_text().split())
            data[key] = val

    # --- Статутний капітал ---
    statcap_text = card.find("text", string=lambda s: s and "грн" in s)
    if statcap_text:
        parent = statcap_text.find_parent("text")
        if parent:
            full_text = parent.get_text(" ", strip=True)
            for part in full_text.split():
                if "," in part and any(ch.isdigit() for ch in part):
                    data["Статутний капітал"] = part + " грн"
                    break

    # --- Уповноважені особи ---
    authorized_block = card.find("div", class_="dr_value_subtitle", string=lambda s: s and "Уповноважені особи" in s)
    if authorized_block:
        authorized_list = []
        for li in authorized_block.find_next("ul").select("li"):
            person = {}
            name_span = li.find("span")
            role_span = li.find("span", class_="dr_signer_role")
            if name_span:
                person["ПІБ"] = name_span.get_text(strip=True)
            if role_span:
                person["Роль"] = role_span.get_text(strip=True)
            if person:
                authorized_list.append(person)
        if authorized_list:
            data["Уповноважені особи"] = authorized_list

    # --- Засновники ---
    founders_block = card.find("div", id="anchor_zasovniki")
    if founders_block:
        founders = []
        for f in founders_block.find_all_next("div", class_="dr_margin_12"):
            # Зупинка перед "Бенефіціари"
            if f.find_previous("div", class_="dr_value_subtitle", string=lambda s: s and "Бенефіціари" in s):
                break

            person = {}
            name_tag = f.find("div", class_="dr_value")
            if name_tag:
                person["ПІБ / Назва"] = name_tag.get_text(strip=True)

            country_tag = f.find("div", class_="dr_value_small", string=lambda s: s and "Країна" in s)
            if country_tag and country_tag.find("b"):
                person["Країна"] = country_tag.find("b").get_text(strip=True)

            contrib_tag = f.find("div", class_="dr_value_small", string=lambda s: s and "Розмір внеску" in s)
            if contrib_tag and contrib_tag.find("b"):
                person["Розмір внеску"] = contrib_tag.find("b").get_text(strip=True).replace(u'\xa0', ' ')

            if person:
                founders.append(person)
        if founders:
            data["Засновники"] = founders

    # --- Бенефіціари ---
    ben_block = card.find("div", class_="dr_value_subtitle", string=lambda s: s and "Бенефіціари" in s)
    if ben_block:
        beneficiaries = []
        for b in ben_block.find_all_next("div", class_="dr_margin_12"):
            person = {}
            name_tag = b.find("div", class_="dr_value")
            if name_tag:
                person["ПІБ"] = name_tag.get_text(strip=True)

            country_tag = b.find("div", class_="dr_value_small", string=lambda s: s and "громадянства" in s)
            if country_tag and country_tag.find("b"):
                person["Країна"] = country_tag.find("b").get_text(strip=True)

            type_tag = b.find("div", class_="dr_value_small", string=lambda s: s and "Тип бенефіціарного" in s)
            if type_tag and type_tag.find("b"):
                person["Тип володіння"] = type_tag.find("b").get_text(strip=True)

            share_tag = b.find("div", class_="dr_value_small", string=lambda s: s and "Відсоток" in s)
            if share_tag:
                person["Частка"] = share_tag.get_text(strip=True).split(":")[-1].strip()

            if person:
                beneficiaries.append(person)
        if beneficiaries:
            data["Бенефіціари"] = beneficiaries

    # --- Види діяльності ---
    kveds = [a.get_text(strip=True) for a in card.select("a.dr_kved_blk_lnk")]
    if kveds:
        data["Види діяльності"] = "; ".join(kveds)

    return data

def parse_ubki_violations(soup, edrpou):
    """Парсить блок 'Заборона брати участь у тендерах від АМКУ'"""
    url = f"https://edrpou.ubki.ua/ua/{edrpou}"
    scraper = cloudscraper.create_scraper()
    html = scraper.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    card = soup.find("div", id="anchor_violations")
    data = {"ЄДРПОУ": edrpou}

    if not card:
        data["АМКУ тендери"] = "Блок не знайдено"
        return data

    text_block = card.find_next("div", class_="dr_value_state")
    if text_block:
        data["АМКУ тендери"] = " ".join(text_block.get_text().split())
    else:
        data["АМКУ тендери"] = "Інформація відсутня"

    return data

def parse_msb_score(soup, edrpou):
    data = {"ЄДРПОУ": edrpou}

    card = soup.find("div", id="anchor_score")
    if not card:
        return {}

    score_value = soup.find("span", id="scoremsb")
    if score_value:
        data["МСБ скоринг (балів)"] = score_value.get_text(strip=True)

    score_level = soup.find("div", class_="vw-rating-value-text")
    if score_level:
        data["МСБ скоринг (рівень)"] = score_level.get_text(strip=True)

    score_date = soup.find("div", class_="vw-rating-datecnt")
    if score_date and "Дата розрахунку" in score_date.get_text():
        strong = score_date.find("strong")
        if strong:
            data["Дата скорингу"] = strong.get_text(strip=True)

    return data

def parse_bankruptcy(soup, edrpou):
    """Парсить блок 'Процедура банкрутства'"""
    data = {"ЄДРПОУ": edrpou}
    card = soup.find("div", id="anchor_bankruptcy")
    if not card:
        return {}

    info_block = card.find_next("div", class_="dr_value_state")
    if info_block:
        data["Банкрутство"] = clean_text(info_block.get_text())
    return data

def parse_finrep(soup, edrpou):
    """
    Парсить блок 'Фінансова звітність' (до поля 'Дохід' включно).
    """
    data = {"ЄДРПОУ": edrpou}

    # Знаходимо сам блок за id
    anchor = soup.find("div", id="anchor_finrep")
    if not anchor:
        return {}

    # Увесь контейнер із фінпоказниками — це батьківський div .dr_card
    card = anchor.find_parent("div", class_="dr_card")
    if not card:
        return {}

    # Збираємо всі фінансові колонки
    columns = card.select(".dr_column.dr_fin_column_1")

    for col in columns:
        subtitle = col.find("div", class_="dr_value_subtitle")
        value = col.find("div", class_="dr_value")
        if subtitle and value:
            key = clean_text(subtitle.get_text())
            val = clean_text(value.get_text())
            data[key] = val

            # Зупиняємось після поля "Дохід"
            if key.strip().lower().startswith("дохід"):
                break

    return data

def parse_tax_data(soup, edrpou):
    """
    Парсить блок 'Податкові дані' (anchor_podatki)
    — включно до кінця секції, з усіма фінансовими полями.
    """
    data = {"ЄДРПОУ": edrpou}

    # Знаходимо якір
    anchor = soup.find("div", id="anchor_podatki")
    if not anchor:
        return {}

    # Піднімаємося до контейнера .dr_card
    card = anchor.find_parent("div", class_="dr_card")
    if not card:
        return {}

    # --- Назва секції ---
    title = anchor.find("h2", class_="dr_value_title")
    if title:
        data["Податкові дані (назва)"] = clean_text(title.get_text())

    # --- Попереджувальна панель (якщо є) ---
    orange_panel = card.find("div", class_="dr_orange_panel")
    if orange_panel:
        data["Податкові дані (примітка)"] = clean_text(orange_panel.get_text())

    # --- Платник ПДВ / не є платником ---
    vat_state = card.find("b", class_="dr_value_state")
    if vat_state:
        data["Статус ПДВ"] = clean_text(vat_state.get_text())

    # --- Основні фінансові пункти ---
    for col in card.select(".dr_column.dr_padding_1"):
        subtitle = col.find("div", class_="dr_value_subtitle")
        value = col.find("div", class_="dr_value")
        if subtitle and value:
            key = clean_text(subtitle.get_text())
            val = clean_text(value.get_text())
            data[key] = val

    return data

def parse_courts(soup, edrpou):
    """
    Парсить блок 'Суди' (id='anchor_susd') — саме табличну частину.
    Повертає список справ із вкладеними документами.
    """
    block = soup.find("div", id="anchor_susd")
    if not block:
        return {}

    table_body = block.select_one("#tsusd_cases_table_body")
    if not table_body:
        return {"ЄДРПОУ": edrpou, "Суди (справи)": []}

    cases = []
    current_case = None

    for row in table_body.find_all("div", class_="dr_court-table-row", recursive=False):
        # верхній ряд — сама справа
        counter = row.find("div", class_="counter")
        cols = row.find_all("div", recursive=False)

        # якщо це основна справа (має лінк або роль)
        if len(cols) >= 5 and "Номер справи" not in row.get_text():
            n_case = clean_text(cols[1].get_text())
            role = clean_text(cols[2].get_text())
            instance = clean_text(cols[3].get_text())
            status = clean_text(cols[4].get_text())

            current_case = {
                "Номер справи": n_case,
                "Роль": role,
                "Інстанція": instance,
                "Стан розгляду": status,
                "Документи": []
            }
            cases.append(current_case)

        elif "dr_court-subtable-row" in row.get("class", []):
            cols = row.find_all("div", recursive=False)
            if len(cols) >= 5 and current_case:
                doc = {
                    "№": clean_text(cols[0].get_text()),
                    "Номер документа": clean_text(cols[1].get_text()),
                    "Дата": clean_text(cols[2].get_text()),
                    "Тип": clean_text(cols[3].get_text()),
                    "Суд": clean_text(cols[4].get_text())
                }
                current_case["Документи"].append(doc)

    return {"ЄДРПОУ": edrpou, "Суди (справи)": cases}

def parse_ubki_full(soup, edrpou):

    # окремі блоки
    base = parse_ubki_universal(soup, edrpou)
    score = parse_msb_score(soup, edrpou)
    bankrupt = parse_bankruptcy(soup, edrpou)
    finrep = parse_finrep(soup, edrpou)
    tax = parse_tax_data(soup, edrpou)
    courts = parse_courts(soup, edrpou)

    merged = {**(base or {}), **(score or {}), **(bankrupt or {}), **(finrep or {}), **(tax or {}), **(courts or {})}
    return merged

