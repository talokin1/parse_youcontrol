import bs4
import pycurl
import pandas as pd
import re
import time
import logging
import sys
import random
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youcontrol_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

URL_BASE = "https://youcontrol.com.ua/catalog/kved/"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36",

    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) "
    "Gecko/20100101 Firefox/123.0",

    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/16.3 Safari/605.1.15",

    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
]

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

def click_on_link(url):
    """Отримує HTML сторінки і повертає BeautifulSoup"""
    logger.info(f"Fetching: {url}")
    buffer = io.BytesIO()
    try:
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, buffer)
        headers_dict = get_headers()
        header_list = [f"{k}: {v}" for k, v in headers_dict.items()]
        c.setopt(c.HTTPHEADER, header_list)
        c.setopt(c.FOLLOWLOCATION, True)
        c.setopt(c.MAXREDIRS, 5)
        c.setopt(c.CONNECTTIMEOUT, 10)
        c.setopt(c.TIMEOUT, 30)
        c.perform()
        status_code = c.getinfo(c.RESPONSE_CODE)
        c.close()
        time.sleep(random.uniform(5, 15))
        if status_code == 200:
            return bs4.BeautifulSoup(buffer.getvalue().decode("utf-8", errors="ignore"), "lxml")
        else:
            logger.error(f"Status code {status_code} for {url}")
            return None
    except Exception as e:
        logger.error(f"Error fetching {url}: {str(e)}")
        return None

def clean_text(text):
    if not text:
        return ""
    text = " ".join(text.split())
    text = re.sub(r"(копіювати|скопійовано|Детальніше)", "", text, flags=re.IGNORECASE)
    return text.strip()

def save_to_csv(batch, class_code, file_path="youcontrol_full_dataset.csv"):
    if not batch:
        return
    df_batch = pd.DataFrame(batch)
    if os.path.exists(file_path):
        df_batch.to_csv(file_path, mode='a', header=False, index=False)
    else:
        df_batch.to_csv(file_path, mode='w', header=True, index=False)
    logger.info(f"Saved {len(batch)} rows for class {class_code} → {file_path}")


def parse_sections(html):
    return html.find_all("div", class_="kved-catalog-table")

def parse_chapters(section_block):
    return section_block.find_all("td", class_="green-col-num")

def parse_groups(table_rows):
    """Проходить по рядках глави і повертає (група, клас)"""
    current_group_code, current_group_name = None, None
    for row in table_rows:
        group_code_td = row.find("td", class_="green-col-word")
        if group_code_td:  # нова група
            current_group_code = group_code_td.text.strip()
            current_group_name = row.find("td", class_="caps-col").text.strip()
            continue
        class_code_td = row.find("td", class_="green-col-num")
        if class_code_td and current_group_code:
            class_code = class_code_td.text.strip()
            class_name = class_code_td.find_next_sibling("td").text.strip()
            yield current_group_code, current_group_name, class_code, class_name


def parse_company_profile(html):
    """Парсить блок профілю компанії"""
    block = html.find("div", class_="seo-table-contain", id="catalog-company-file")
    if not block:
        return {}
    rows = block.find_all("div", class_="seo-table-row")
    cols, vals = [], []
    names_for_df = {
        "Повне найменування юридичної особи": "JUR_PER_NAME",
        "Скорочена назва": "JUR_PER_NAME_TRUNC",
        "Статус юридичної особи": "JUR_PER_STATUS",
        "Статус з ЄДР": "EDR_STATUS",
        "Код ЄДРПОУ": "EDRPOU_CODE",
        "Дата реєстрації": "REGISTRATION_DATE",
        "Уповноважені особи": "AUTHORISED_PERSON",
        "Організаційно-правова форма": "ORG_FORM",
        "Розмір статутного капіталу": "CAPITAL_SIZE",
        "Види діяльності": "ACTIVITES",
        "Контактна інформація": "CONTACT_INFO"
    }
    for row in rows:
        col = row.find("div", class_="seo-table-col-1").text.strip()
        val = row.find("span", class_="copy-file-field") or row.find("div", class_="seo-table-col-2")
        val = clean_text(val.text if val else "")
        cols.append(names_for_df.get(col, col))
        vals.append(val)
    return dict(zip(cols, vals))

def parse_company_beneficiaries(html):
    """Парсить блок бенефіціарів"""
    block = html.find("div", class_="seo-table-contain", id="catalog-company-beneficiary")
    if not block:
        return {}
    rows = block.find_all("div", class_="seo-table-row")
    cols, vals = [], []
    for row in rows:
        cols.append(row.find("div", class_="seo-table-col-1").text.strip())
        val = row.find("span", class_="copy-file-field") or row.find("div", class_="seo-table-col-2")
        vals.append(clean_text(val.text if val else ""))
    raw_edrpou = html.find("h2", class_="seo-table-name case-icon short").text
    edrpou = re.search(r'\d+', raw_edrpou).group() if re.search(r'\d+', raw_edrpou) else None
    cols.append("EDRPOU_CODE")
    vals.append(edrpou)
    return dict(zip(cols, vals))

def parse_company(company_code, url_details, context):
    html = click_on_link(url_details)
    if not html:
        return None
    profile = parse_company_profile(html)
    beneficiaries = parse_company_beneficiaries(html)
    row = {**context, "EDRPOU_CODE": company_code}
    row.update(profile)
    row.update(beneficiaries)
    return row

def parse_class(url_chapter, class_code, class_name, context, max_workers=8):
    """Парсить всі компанії в класі паралельно"""
    url_class = url_chapter + f'/{class_code[-2:]}'
    html_class = click_on_link(url_class)
    if not html_class:
        return []

    # визначаємо кількість сторінок
    pagination = html_class.find("ul", class_="pagination")
    max_page = 1
    if pagination:
        pages = [int(a.text) for a in pagination.find_all("a") if a.text.isdigit()]
        if pages:
            max_page = max(pages)

    company_tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for page in range(1, max_page + 1):
            url_page = url_class + f'?page={page}'
            html_page = click_on_link(url_page)
            if not html_page:
                continue
            raw_edrpous = html_page.find_all("a", class_="link-details link-open")
            for raw in raw_edrpous:
                company_code = raw.text.split(",")[0].strip()
                url_details = f'https://youcontrol.com.ua{raw.get("href")}'
                company_tasks.append(executor.submit(
                    parse_company, company_code, url_details, {**context,
                        "CLASS_CODE": class_code,
                        "CLASS_NAME": class_name}
                ))

        results = []
        for future in as_completed(company_tasks):
            row = future.result()
            if row:
                results.append(row)

    logger.info(f"Parsed {len(results)} companies for class {class_code}")
    return results


def parse_all_kved(parallel_classes=True):
    data = []
    html_sections = click_on_link(URL_BASE)
    if not html_sections:
        return pd.DataFrame()

    futures = []
    with ProcessPoolExecutor(max_workers=4 if parallel_classes else 1) as pool:
        for section_block in parse_sections(html_sections):
            section_code = section_block.find("td", class_="green-col-word").text.strip()
            section_name = section_block.find("td", class_="caps-col").text.strip()
            for chapter_td in parse_chapters(section_block):
                chapter_code = chapter_td.text.strip()
                chapter_name = chapter_td.find_next_sibling("td").text.strip()
                url_chapter = URL_BASE + chapter_code
                html_chapter = click_on_link(url_chapter)
                if not html_chapter:
                    continue
                table = html_chapter.find("table")
                if not table:
                    continue
                for group_code, group_name, class_code, class_name in parse_groups(table.find_all("tr")):
                    context = {
                        "SECTION_CODE": section_code,
                        "SECTION_NAME": section_name,
                        "CHAPTER_CODE": chapter_code,
                        "CHAPTER_NAME": chapter_name,
                        "GROUP_CODE": group_code,
                        "GROUP_NAME": group_name,
                    }
                    futures.append(pool.submit(parse_class, url_chapter, class_code, class_name, context))

        for f in as_completed(futures):
            batch = f.result()
            if batch:
                save_to_csv(batch, batch[0].get("CLASS_CODE"))
                data.extend(batch)

    return pd.DataFrame(data)



if __name__ == "__main__":
    df = parse_all_kved()
    print(df.head())
