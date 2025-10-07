import bs4
import cloudscraper
import pandas as pd
import re
import time
import logging
import sys
from uuid import uuid4
import random
import io
import os

# === Налаштування ===
TARGET_CLASS_CODE = "01.11"
TARGET_PAGES = None

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

# створюємо глобальний сешн з обходом Cloudflare
scraper = cloudscraper.create_scraper(delay=10, browser={
    'browser': 'chrome',
    'platform': 'windows',
    'mobile': False
})

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

def smart_sleep(base_min=3, base_max=7):
    time.sleep(random.uniform(base_min, base_max))

def click_on_link(url, max_retries=5, long_wait=1800):
    """Cloudflare-safe request"""
    attempt = 0
    while True:
        try:
            logger.info(f"[{attempt+1}] Fetching {url}")
            resp = scraper.get(url, headers=get_headers(), timeout=30)
            
            if resp.status_code == 200:
                html = resp.text
                return bs4.BeautifulSoup(html, "lxml")

            elif resp.status_code in [429, 500, 502, 503, 504]:
                delay = 2 ** attempt + random.uniform(1, 3)
                logger.warning(f"Server error {resp.status_code}, retrying in {delay:.1f}s")
                time.sleep(delay)
                attempt += 1
                continue

            else:
                logger.error(f"Unexpected HTTP {resp.status_code}, waiting 5 min")
                time.sleep(300)
                attempt = 0

        except Exception as e:
            logger.warning(f"Error {e}, waiting 1 min...")
            time.sleep(60)
            attempt += 1
            if attempt >= max_retries:
                logger.error(f"Server unreachable. Waiting {long_wait/60:.0f} minutes.")
                time.sleep(long_wait)
                attempt = 0



def clean_text(text):
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# === Основна логіка ===
def parse_all_kved():
    logger.info("Starting KVED parsing")

    # Завантаження checkpoint
    last_completed = None
    if os.path.exists("checkpoint.txt"):
        with open("checkpoint.txt") as f:
            last_completed = f.read().strip()
            logger.info(f"Checkpoint found: {last_completed}")

    html_sections = click_on_link(URL_BASE)
    if not html_sections:
        logger.error("Cannot fetch main KVED catalog page.")
        return

    section_blocks = html_sections.find_all("div", class_="kved-catalog-table")
    logger.info(f"Found {len(section_blocks)} section blocks")

    for section_block in section_blocks:
        section_code = section_block.find("td", class_="green-col-word").text.strip()
        section_name = section_block.find("td", class_="caps-col").text.strip()

        chapter_tds = section_block.find_all("td", class_="green-col-num")
        for chapter_td in chapter_tds:
            chapter_code = chapter_td.text.strip()
            chapter_name = chapter_td.find_next_sibling("td").text.strip()

            url_chapter = URL_BASE + chapter_code
            html_chapter = click_on_link(url_chapter)
            if not html_chapter:
                continue

            table = html_chapter.find("table")
            if not table:
                continue

            rows = table.find_all("tr")
            current_group_code, current_group_name = None, None

            for row in rows:
                group_code_td = row.find("td", class_="green-col-word")
                if group_code_td:
                    current_group_code = group_code_td.text.strip()
                    current_group_name = row.find("td", class_="caps-col").text.strip()
                    continue

                class_code_td = row.find("td", class_="green-col-num")
                if class_code_td and current_group_code:
                    class_code = class_code_td.text.strip()
                    if last_completed:
                        if class_code == last_completed:
                            last_completed = None  # скидаємо прапорець
                            continue  # пропускаємо рівно останній завершений
                        elif last_completed is not None:
                            continue


                    class_name = class_code_td.find_next_sibling("td").text.strip()
                    url_class = url_chapter + f'/{class_code[-2:]}'
                    html_class = click_on_link(url_class)
                    if not html_class:
                        continue

                    pagination = html_class.find("ul", class_="pagination")
                    max_page = 1
                    if pagination:
                        for li in pagination.find_all("li"):
                            a = li.find("a")
                            if a and a.text.isdigit():
                                max_page = max(max_page, int(a.text))

                    pages_to_parse = TARGET_PAGES if TARGET_PAGES else range(1, max_page + 1)
                    batch_data = []

                    checkpoint_class, checkpoint_page = None, None
                    if os.path.exists("checkpoint.txt"):
                        with open("checkpoint.txt") as f:
                            line = f.read().strip()
                            if "|" in line:
                                checkpoint_class, checkpoint_page = line.split("|")
                                checkpoint_page = int(checkpoint_page)
                            else:
                                checkpoint_class = line
                                checkpoint_page = None

                    logger.info(f"Parsing {max_page} pages for class {class_code}")
                    for page in pages_to_parse:
                        # Пропуск сторінок, які вже оброблені
                        if checkpoint_class == class_code and checkpoint_page and page <= checkpoint_page:
                            logger.info(f"Skipping page {page} (already parsed before crash)")
                            continue

                        url_page = url_class + f'?page={page}'
                        html_page = click_on_link(url_page)
                        if not html_page:
                            continue

                        raw_edrpous = html_page.find_all("a", class_="link-details link-open")
                        if not raw_edrpous:
                            continue

                        batch_data = []
                        for raw_edrpou in raw_edrpous:
                            company_code = raw_edrpou.text.split(",")[0].strip()
                            url_details = f"https://youcontrol.com.ua{raw_edrpou.get('href')}"
                            html_details = click_on_link(url_details)
                            if not html_details:
                                continue

                            block_profile = html_details.find("div", class_="seo-table-contain", id="catalog-company-file")
                            profile_dict = {}
                            if block_profile:
                                profile_rows = block_profile.find_all("div", class_="seo-table-row")
                                profile_data_columns = [
                                    row.find("div", class_="seo-table-col-1").text.strip()
                                    for row in profile_rows
                                ]

                                raw_data = [
                                    (row.find("span", class_="copy-file-field") or
                                    row.find("div", class_="copy-file-field") or
                                    row.find("p", class_="ucfirst copy-file-field") or
                                    row.find("div", class_="seo-table-col-2")).text
                                    for row in profile_rows
                                    if (row.find("span", class_="copy-file-field") or
                                        row.find("div", class_="copy-file-field") or
                                        row.find("p", class_="ucfirst copy-file-field") or
                                        row.find("div", class_="seo-table-col-2"))
                                ]
                                profile_data_text = [clean_text(x) for x in raw_data if x.strip()]
                                profile_dict = dict(zip(profile_data_columns, profile_data_text))

                            block_beneficiary = html_details.find("div", class_="seo-table-contain", id="catalog-company-beneficiary")
                            data_beneficiary_dict = {}
                            if block_beneficiary:
                                beneficiary_rows = block_beneficiary.find_all("div", class_="seo-table-row")
                                beneficiary_data_columns = [r.find("div", class_="seo-table-col-1").text.strip() for r in beneficiary_rows]

                                raw_edrpou = html_details.find("h2", class_="seo-table-name case-icon short").text
                                edrpou = re.search(r'\d+', raw_edrpou).group() if re.search(r'\d+', raw_edrpou) else None
                                beneficiary_data_columns.append("EDRPOU_CODE")

                                text_beneficiary_spans = [r.find("span", class_="copy-file-field") for r in beneficiary_rows]
                                text_beneficiary_persons = [
                                    r.find("div", class_="seo-table-col-2")
                                    if r.find("div", class_="seo-table-col-2") and 'copy-hover' not in r.find("div", class_="seo-table-col-2").get("class", [])
                                    else None for r in beneficiary_rows
                                ]
                                text_beneficiary_spans = [x for x in text_beneficiary_spans if x]
                                text_beneficiary_persons = [x for x in text_beneficiary_persons if x]

                                data_beneficiary = text_beneficiary_spans + text_beneficiary_persons
                                data_beneficiary_text = [clean_text(x.text) for x in data_beneficiary] + [edrpou]

                                data_beneficiary_dict = dict(zip(beneficiary_data_columns, data_beneficiary_text))

                            row_data = {
                                "SECTION_CODE": section_code,
                                "SECTION_NAME": section_name,
                                "CHAPTER_CODE": chapter_code,
                                "CHAPTER_NAME": chapter_name,
                                "GROUP_CODE": current_group_code,
                                "GROUP_NAME": current_group_name,
                                "CLASS_CODE": class_code,
                                "CLASS_NAME": class_name,
                                "EDRPOU_CODE": company_code,
                            }
                            row_data.update(profile_dict)
                            row_data.update(data_beneficiary_dict)
                            batch_data.append(row_data)

                        # === ЗБЕРЕЖЕННЯ ПІСЛЯ КОЖНОЇ СТОРІНКИ ===
                        if batch_data:
                            df_batch = pd.DataFrame(batch_data)
                            file_path = f"kved_{class_code}_p{page}.csv"
                            df_batch.to_csv(file_path, mode='w', header=True, index=False)
                            logger.info(f"Saved class {class_code} page {page} to {file_path}")

                            with open("checkpoint.txt", "w") as f:
                                f.write(f"{class_code}|{page}")

                            smart_sleep(base_min=15, base_max=30)


                        for raw_edrpou in raw_edrpous:
                            company_code = raw_edrpou.text.split(",")[0].strip()
                            url_details = f"https://youcontrol.com.ua{raw_edrpou.get('href')}"
                            html_details = click_on_link(url_details)
                            if not html_details:
                                continue

                            block_profile = html_details.find("div", class_="seo-table-contain", id="catalog-company-file")
                            profile_dict = {}
                            if block_profile:
                                profile_rows = block_profile.find_all("div", class_="seo-table-row")
                                profile_data_columns = [
                                    row.find("div", class_="seo-table-col-1").text.strip()
                                    for row in profile_rows
                                ]

                            block_beneficiary = html_details.find("div", class_="seo-table-contain", id="catalog-company-beneficiary")
                            data_beneficiary_dict = {}
                            if block_beneficiary:
                                beneficiary_rows = block_beneficiary.find_all("div", class_="seo-table-row")

                                beneficiary_data_columns = []
                                for rows in beneficiary_rows:
                                    beneficiary_data_columns.append(rows.find("div", class_="seo-table-col-1").text.strip())

                                raw_edrpou = html_details.find("h2", class_="seo-table-name case-icon short").text
                                edrpou = re.search(r'\d+', raw_edrpou).group() if re.search(r'\d+', raw_edrpou) else None

                                beneficiary_data_columns.append("EDRPOU_CODE")

                                text_beneficiary_spans = [row.find("span", class_="copy-file-field") for row in beneficiary_rows]
                                text_beneficiary_persons = [
                                    row.find("div", class_="seo-table-col-2")
                                    if row.find("div", class_="seo-table-col-2")
                                    and 'copy-hover' not in row.find("div", class_="seo-table-col-2").get("class", [])
                                    else None
                                    for row in beneficiary_rows
                                ]

                                text_beneficiary_spans = [x for x in text_beneficiary_spans if x is not None]
                                text_beneficiary_persons = [x for x in text_beneficiary_persons if x is not None]

                                data_beneficiary = text_beneficiary_spans + text_beneficiary_persons
                                data_beneficiary_text = [data.text for data in data_beneficiary]
                                data_beneficiary_text = data_beneficiary_text + [edrpou]

                                data_beneficiary_dict = dict(zip(beneficiary_data_columns, data_beneficiary_text))
                                data_beneficiary_dict = {k: clean_text(v) for k, v in data_beneficiary_dict.items()}

                                raw_data = [
                                    (row.find("span", class_="copy-file-field") or
                                     row.find("div", class_="copy-file-field") or
                                     row.find("p", class_="ucfirst copy-file-field") or
                                     row.find("div", class_="seo-table-col-2")).text
                                    for row in profile_rows
                                    if (row.find("span", class_="copy-file-field") or
                                        row.find("div", class_="copy-file-field") or
                                        row.find("p", class_="ucfirst copy-file-field") or
                                        row.find("div", class_="seo-table-col-2"))
                                ]
                                profile_data_text = [clean_text(x) for x in raw_data if x.strip()]
                                profile_dict = dict(zip(profile_data_columns, profile_data_text))

                            row_data = {
                                "SECTION_CODE": section_code,
                                "SECTION_NAME": section_name,
                                "CHAPTER_CODE": chapter_code,
                                "CHAPTER_NAME": chapter_name,
                                "GROUP_CODE": current_group_code,
                                "GROUP_NAME": current_group_name,
                                "CLASS_CODE": class_code,
                                "CLASS_NAME": class_name,
                                "EDRPOU_CODE": company_code,
                            }
                            row_data.update(profile_dict)
                            row_data.update(data_beneficiary_dict) 
                            batch_data.append(row_data)

                    if batch_data:
                        df_batch = pd.DataFrame(batch_data)
                        file_path = f"kved_{class_code}.csv"
                        df_batch.to_csv(file_path, mode='w', header=True, index=False)
                        logger.info(f"Saved class {class_code} to {file_path}")

                        with open("checkpoint.txt", "w") as f:
                            f.write(class_code)

                        del df_batch, batch_data
                        smart_sleep(base_min=20, base_max=40, long_pause_chance=0.3)

    logger.info("Completed parsing all KVEDs.")

# === Глобальний контроль запуску ===
if __name__ == "__main__":
    while True:
        try:
            parse_all_kved()
            logger.info("Parsing complete. Restarting in 5 minutes.")
            time.sleep(300)
        except Exception as e:
            logger.error(f"Global crash: {e}. Restarting in 60s...")
            time.sleep(60)
