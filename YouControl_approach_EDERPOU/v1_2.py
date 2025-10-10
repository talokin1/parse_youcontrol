from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
import pickle
from bs4 import BeautifulSoup
import re
import pandas as pd
from pathlib import Path
from datetime import datetime


# --- Конфігурація ---
INPUT_CSV = r"C:\OTP Draft\YouControl\test_dataset.csv"  # Файл з ЄДРПОУ (колонка "edrpou")
OUTPUT_DIR = "parsed_companies"  # Папка для результатів
COOKIES_FILE = "cookies.pkl"
DELAY_BETWEEN_REQUESTS = 3  # Затримка між запитами (секунди)


# --- 1. Функції парсингу (без змін) ---

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


# --- 2. Функція авторизації ---

def authorize_and_save_cookies(driver, login: str, password: str):
    """Авторизація та збереження cookies."""
    driver.get("https://youcontrol.com.ua/sign_in/")
    time.sleep(5)
    
    driver.find_element(By.NAME, "LoginForm[login]").send_keys(login)
    driver.find_element(By.NAME, "LoginForm[password]").send_keys(password)
    driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
    time.sleep(5)
    
    cookies = driver.get_cookies()
    with open(COOKIES_FILE, "wb") as f:
        pickle.dump(cookies, f)
    print("✅ Авторизація виконана і cookies збережено!")


def load_cookies(driver):
    """Завантаження збережених cookies."""
    if Path(COOKIES_FILE).exists():
        with open(COOKIES_FILE, "rb") as f:
            cookies = pickle.load(f)
        driver.get("https://youcontrol.com.ua/")
        for cookie in cookies:
            driver.add_cookie(cookie)
        driver.refresh()
        time.sleep(2)
        print("✅ Cookies завантажено!")
        return True
    return False


# --- 3. Функція пошуку та парсингу компанії ---

def search_and_parse_company(driver, edrpou: str) -> dict:
    """Шукає компанію за ЄДРПОУ та парсить дані."""
    try:
        # Перехід на головну для пошуку
        driver.get("https://youcontrol.com.ua/")
        time.sleep(2)
        
        # Пошук компанії
        search_box = driver.find_element(By.CSS_SELECTOR, "input[placeholder*='Введіть назву компанії, ЄДРПОУ']")
        search_box.clear()
        search_box.send_keys(str(edrpou))
        search_box.send_keys(Keys.ENTER)
        time.sleep(5)
        
        # Отримання HTML
        html = driver.page_source
        company_data = parse_company_youcontrol(html)
        company_data["EDRPOU_INPUT"] = edrpou
        company_data["parse_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return company_data
    
    except Exception as e:
        print(f"❌ Помилка при парсингу {edrpou}: {str(e)}")
        return {
            "EDRPOU_INPUT": edrpou,
            "ERROR": str(e),
            "parse_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


# --- 4. Основна функція ---

def main():
    """Головна функція для обробки списку ЄДРПОУ."""
    
    # Створення папки для результатів
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    
    # Читання списку ЄДРПОУ
    try:
        df_input = pd.read_csv(INPUT_CSV)
        if "EDRPOU" not in df_input.columns:
            print(f"❌ Колонка 'EDRPOU' не знайдена в {INPUT_CSV}")
            print(f"Доступні колонки: {', '.join(df_input.columns)}")
            return
        
        edrpou_list = df_input["EDRPOU"].dropna().astype(str).tolist()
        print(f"📋 Знайдено {len(edrpou_list)} ЄДРПОУ для обробки")
    
    except FileNotFoundError:
        print(f"❌ Файл {INPUT_CSV} не знайдено!")
        print("Створіть CSV файл з колонкою 'edrpou' та списком кодів ЄДРПОУ")
        return
    
    # Ініціалізація браузера
    options = Options()
    options.add_argument("--start-maximized")
    # options.add_argument("--headless")  # Розкоментуйте для фонового режиму
    
    driver = webdriver.Chrome(options=options)
    
    try:
        # Авторизація або завантаження cookies
        if not load_cookies(driver):
            print("⚠️ Cookies не знайдено, виконується авторизація...")
            authorize_and_save_cookies(
                driver,
                login="nelkovvasilij@gmail.com",
                password="gEr32weIG&*"
            )
        
        # Обробка кожного ЄДРПОУ
        all_results = []
        
        for idx, edrpou in enumerate(edrpou_list, start=1):
            print(f"\n[{idx}/{len(edrpou_list)}] Обробка ЄДРПОУ: {edrpou}")
            
            company_data = search_and_parse_company(driver, edrpou)
            all_results.append(company_data)
            
            # Збереження проміжного результату
            df_temp = pd.DataFrame(all_results)
            temp_file = Path(OUTPUT_DIR) / "temp_results.csv"
            df_temp.to_csv(temp_file, index=False, encoding="utf-8-sig")
            
            # Затримка між запитами
            if idx < len(edrpou_list):
                time.sleep(DELAY_BETWEEN_REQUESTS)
        
        # Фінальне збереження
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = Path(OUTPUT_DIR) / f"companies_parsed_{timestamp}.csv"
        df_final = pd.DataFrame(all_results)
        df_final.to_csv(output_file, index=False, encoding="utf-8-sig")
        
        print(f"\n✅ Парсинг завершено!")
        print(f"📁 Результати збережено в: {output_file}")
        print(f"📊 Оброблено компаній: {len(all_results)}")
        
    finally:
        driver.quit()


# --- 5. Запуск ---

if __name__ == "__main__":
    main()