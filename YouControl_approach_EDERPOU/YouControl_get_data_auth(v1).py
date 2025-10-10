from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
import pickle
from bs4 import BeautifulSoup
import re
import pandas as pd

# --- 1. Авторизація через Selenium ---
options = Options()
options.add_argument("--start-maximized")
# options.add_argument("--headless")

driver = webdriver.Chrome(options=options)
driver.get("https://youcontrol.com.ua/sign_in/")
time.sleep(5)

# Введення логіну та паролю
driver.find_element(By.NAME, "LoginForm[login]").send_keys("nelkovvasilij@gmail.com")
driver.find_element(By.NAME, "LoginForm[password]").send_keys("gEr32weIG&*")
driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
time.sleep(5)

# Збереження cookies
cookies = driver.get_cookies()
with open("cookies.pkl", "wb") as f:
    pickle.dump(cookies, f)
print("✅ Авторизація виконана і cookies збережено!")


# --- 2. Пошук компанії за ЄДРПОУ ---
search_box = driver.find_element(By.CSS_SELECTOR, "input[placeholder*='Введіть назву компанії, ЄДРПОУ']")
search_box.send_keys("22215409")
search_box.send_keys(Keys.ENTER)
time.sleep(5)

# --- 3. Отримання HTML ---
html = driver.page_source
soup = BeautifulSoup(html, "lxml")


# --- 4. Функції для парсингу блоків ---

def parse_activities_block(td_tag):
    """Парсить блок видів діяльності."""
    data = {}
    main_block = td_tag.select_one("div.localized-item.flex-activity")
    if main_block:
        div_copy = BeautifulSoup(str(main_block), "lxml")
        for t in div_copy.select("div.activity-tooltip-p, a"):
            t.decompose()
        text = div_copy.get_text(" ", strip=True)
        match = re.match(r"(\d{2}\.\d{2})\s+(.+)", text)
        if match:
            kved, desc = match.groups()
            data["КВЕД (Основний)"] = f"{kved} — {desc.strip()}"
    other_blocks = td_tag.select("li.localized-item.localized-other")[:3]
    for idx, li in enumerate(other_blocks, start=1):
        li_copy = BeautifulSoup(str(li), "lxml")
        for t in li_copy.select("div.activity-tooltip-p, a"):
            t.decompose()
        text = li_copy.get_text(" ", strip=True)
        match = re.match(r"(\d{2}\.\d{2})\s+(.+)", text)
        if match:
            kved, desc = match.groups()
            data[f"КВЕД_{idx} (Інші)"] = f"{kved} — {desc.strip()}"
    return data


def parse_founders_block(td_tag):
    """Збирає повні текстові блоки засновників."""
    data = {}
    founder_cards = td_tag.select("div.info-additional-file#founder-card-list-block")
    for idx, card in enumerate(founder_cards[:5], start=1):
        card_copy = BeautifulSoup(str(card), "lxml")
        for t in card_copy.select(".copy-done, .content-for-copy, .check-individuals-link-search"):
            t.decompose()
        text = card_copy.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()
        data[f"Cofounder_{idx}"] = text
    return data


def parse_company_youcontrol(html: str) -> dict:
    """Парсить сторінку компанії в один словник."""
    soup = BeautifulSoup(html, "lxml")
    result = {}

    for table in soup.select("table.detail-view"):
        for tr in table.select("tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(separator=" ", strip=True)
            key = re.sub(r"\s*\(Актуально.+?\)\s*", "", key).strip()
            td_copy = BeautifulSoup(str(td), "lxml")
            for bad in td_copy.select(".copy-done, .content-for-copy"):
                bad.decompose()

            if key.lower().startswith("види діяльності"):
                result.update(parse_activities_block(td_copy))
            elif key.lower().startswith("перелік засновників"):
                result.update(parse_founders_block(td_copy))
            else:
                value = td_copy.get_text(" ", strip=True)
                value = re.sub(r"\s+", " ", value).strip()
                result[key] = value

    edrpou_match = soup.find("h2", class_="seo-table-name case-icon short")
    if edrpou_match:
        edrpou = re.search(r"\d+", edrpou_match.get_text())
        if edrpou:
            result["EDRPOU_CODE"] = edrpou.group()

    return result


# --- 5. Використання ---
company_data = parse_company_youcontrol(html)
df = pd.DataFrame([company_data])
df.to_csv("company_22215409.csv", index=False, encoding="utf-8-sig")

print("✅ Компанію спарсено та збережено у CSV!")
driver.quit()
