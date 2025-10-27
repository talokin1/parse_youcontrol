import sys
import os
import json
import time
import random
import logging
import re
import bs4
import pandas as pd
import cloudscraper
from uuid import uuid4

# === ЛОГІН ДЛЯ DATAIMPULSE (опціонально) ===
# DATAIMPULSE_LOGIN = "703846583e3f144c34e4"
# DATAIMPULSE_PASS = "aac7709c4b153b28"
# DATAIMPULSE_HOST = "gw.dataimpulse.com"
# DATAIMPULSE_PORT = 823

# === Глобальні налаштування ===
URL_BASE = "https://youcontrol.com.ua/catalog/kved/"
CHECKPOINT_FILE = "checkpoint.json"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youcontrol_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# === Cloudflare-safe scraper ===
scraper = cloudscraper.create_scraper(
    delay=10,
    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36",
]


def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://youcontrol.com.ua/catalog/kved/",
        "Upgrade-Insecure-Requests": "1",
    }


def smart_sleep(base_min=5, base_max=15):
    """Випадкова пауза між запитами."""
    delay = random.uniform(base_min, base_max)
    logger.info(f"Sleeping {delay:.1f}s...")
    time.sleep(delay)


def click_on_link(url, max_retries=5, long_wait=1800):
    """Cloudflare-safe request із повторними спробами."""
    attempt = 0
    while True:
        try:
            logger.info(f"[{attempt+1}] Fetching {url}")
            resp = scraper.get(url, headers=get_headers(), timeout=60)

            if resp.status_code == 200:
                return bs4.BeautifulSoup(resp.text, "lxml")

            elif resp.status_code in [429, 500, 502, 503, 504]:
                delay = min(2 ** attempt + random.uniform(1, 3), 120)
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


def load_checkpoint():
    """Читання JSON чекпоінта."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                logger.info(f"✅ Checkpoint loaded: {data}")
                return data
            except json.JSONDecodeError:
                logger.warning("Checkpoint corrupted, ignoring.")
    return {}


def save_checkpoint(data):
    """Запис JSON чекпоінта."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def clean_text(text):
    text = re.sub(r'\s+', ' ', text or "")
    return text.strip()


def fetch_company_details(company_code, url_details, meta):
    """Отримати дані компанії."""
    html = click_on_link(url_details)
    if not html:
        return None

    profile_dict = {}
    block_profile = html.find("div", id="catalog-company-file")
    if block_profile:
        for row in block_profile.find_all("div", class_="seo-table-row"):
            key = clean_text(row.find("div", class_="seo-table-col-1").text)
            val_elem = row.find("span", class_="copy-file-field") or \
                       row.find("div", class_="copy-file-field") or \
                       row.find("p", class_="ucfirst copy-file-field") or \
                       row.find("div", class_="seo-table-col-2")
            if val_elem:
                profile_dict[key] = clean_text(val_elem.text)

    beneficiary_dict = {}
    block_benef = html.find("div", id="catalog-company-beneficiary")
    if block_benef:
        for row in block_benef.find_all("div", class_="seo-table-row"):
            key = clean_text(row.find("div", class_="seo-table-col-1").text)
            val = row.find("span", class_="copy-file-field") or \
                  row.find("div", class_="seo-table-col-2")
            if val:
                beneficiary_dict[key] = clean_text(val.text)

    row = {
        **meta,
        "EDRPOU_CODE": company_code,
    }
    row.update(profile_dict)
    row.update(beneficiary_dict)
    return row


def parse_all_kved():
    """Основна функція парсингу."""
    checkpoint = load_checkpoint()
    last_url = checkpoint.get("last_url")

    html_sections = click_on_link(URL_BASE)
    if not html_sections:
        logger.error("Cannot fetch main catalog page.")
        return

    section_blocks = html_sections.find_all("div", class_="kved-catalog-table")
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
                if not (class_code_td and current_group_code):
                    continue

                class_code = class_code_td.text.strip()
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

                logger.info(f"Parsing class {class_code}, {max_page} pages total.")
                time.sleep(1.4)

                batch = []
                for page in range(1, max_page + 1):
                    url_page = f"{url_class}?page={page}"
                    html_page = click_on_link(url_page)
                    if not html_page:
                        continue

                    companies = html_page.find_all("a", class_="link-details link-open")
                    for idx, comp in enumerate(companies):
                        company_code = comp.text.split(",")[0].strip()
                        url_details = "https://youcontrol.com.ua" + comp.get("href")

                        # Якщо є чекпоінт - пропускаємо до нього
                        if last_url:
                            if url_details == last_url:
                                logger.info(f"Resuming from {url_details}")
                                last_url = None
                            else:
                                continue

                        meta = {
                            "SECTION_CODE": section_code,
                            "SECTION_NAME": section_name,
                            "CHAPTER_CODE": chapter_code,
                            "CHAPTER_NAME": chapter_name,
                            "GROUP_CODE": current_group_code,
                            "GROUP_NAME": current_group_name,
                            "CLASS_CODE": class_code,
                            "CLASS_NAME": class_name,
                            "PAGE": page
                        }

                        data = fetch_company_details(company_code, url_details, meta)
                        if data:
                            batch.append(data)
                            logger.info(f"Added company {company_code} ({len(batch)} in buffer)")

                        if len(batch) >= 5:
                            df = pd.DataFrame(batch)
                            file_name = f"kved_{class_code}_p{page}_{uuid4().hex[:6]}.csv"
                            df.to_csv(file_name, index=False)
                            logger.info(f"Saved {len(batch)} rows - {file_name}")
                            batch = []
                            save_checkpoint({"last_url": url_details})
                            smart_sleep(20, 40)

                        smart_sleep(3, 8)

                    # Після сторінки зберегти чекпоінт
                    save_checkpoint({"last_url": url_page})
                    if batch:
                        df = pd.DataFrame(batch)
                        file_name = f"kved_{class_code}_p{page}_{uuid4().hex[:6]}.csv"
                        df.to_csv(file_name, index=False)
                        logger.info(f"💾 Saved remaining {len(batch)} rows → {file_name}")
                        batch = []

                    smart_sleep(15, 30)

                save_checkpoint({"last_url": None})
                logger.info(f"✅ Completed class {class_code}")
                smart_sleep(30, 60)

    logger.info("🎉 Completed parsing all KVEDs.")


if __name__ == "__main__":
    while True:
        try:
            parse_all_kved()
            logger.info("Parsing complete. Restarting in 10 minutes...")
            time.sleep(600)
        except Exception as e:
            logger.error(f"Global crash: {e}. Restarting in 60s...")
            time.sleep(60)
