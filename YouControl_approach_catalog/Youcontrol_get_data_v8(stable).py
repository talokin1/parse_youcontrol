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


REQUEST_COUNT = 0
scraper = cloudscraper.create_scraper(
    delay=10,
    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
)

REFERERS = [
    "https://youcontrol.com.ua/catalog/",
    "https://youcontrol.com.ua/catalog/kved/",
    "https://google.com/",
    "https://www.bing.com/search?q=kved",
    "https://duckduckgo.com/?q=youcontrol",
]


def get_headers():
    """Динамічні Cloudflare-friendly заголовки."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": random.choice([
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "text/html,application/xhtml+xml;q=0.8,image/webp,*/*;q=0.5"
        ]),
        "Accept-Language": random.choice([
            "en-US,en;q=0.9",
            "uk-UA,uk;q=0.9,en;q=0.8",
            "ru-RU,ru;q=0.9,en;q=0.8",
        ]),
        "Referer": random.choice(REFERERS),
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": str(random.choice([0, 1])),
        "Cache-Control": random.choice(["max-age=0", "no-cache"]),
    }



def smart_sleep(base_min=1.5, base_max=3.5, jitter=0.8):
    """Коротка адаптивна пауза, з випадковістю та антидетектом."""

    delay = random.uniform(base_min, base_max) + random.uniform(0, jitter)
    logger.info(f"Sleeping {delay:.2f}s...")
    time.sleep(delay)


def human_delay():
    if random.random() < 0.2:
        read_time = random.uniform(2, 6)
        logger.info(f"Emulating user reading for {read_time:.1f}s...")
        time.sleep(read_time)




def click_on_link(url, max_retries=6, long_wait=1800):
    """
    Cloudflare-safe запит із антидетектом та сесійною ротацією.
    """
    global REQUEST_COUNT, scraper

    REQUEST_COUNT += 1
    REQUEST_COUNT += 1

    if REQUEST_COUNT % 100 == 0:
        logger.info("Restarting process to refresh global state.")
        save_checkpoint({"last_url": None})
        os.execv(sys.executable, ['python'] + sys.argv)


    if REQUEST_COUNT % 80 == 0:
        logger.info("Rotating scraper fingerprint (new Cloudflare session)")
        scraper = cloudscraper.create_scraper(
            delay=random.randint(8, 15),
            browser={'browser': random.choice(['chrome', 'firefox']),
                     'platform': random.choice(['windows', 'linux']),
                     'mobile': random.choice([False, True])}
        )
        smart_sleep(3, 6)

    attempt = 0
    while True:
        try:
            logger.info(f"[{attempt + 1}] Fetching {url}")
            headers = get_headers()

            resp = scraper.get(url, headers=headers, timeout=(10, 40))

            if resp.status_code == 200:
                smart_sleep(1.0, 2.0)
                return bs4.BeautifulSoup(resp.text, "lxml")

            elif resp.status_code in [429, 500, 502, 503, 504]:
                delay = min(2 ** attempt + random.uniform(1, 3), 120)
                logger.warning(f"Server error {resp.status_code}, retrying in {delay:.1f}s")
                time.sleep(delay)
                attempt += 1
                continue

            elif resp.status_code == 403:
                logger.warning("Cloudflare block detected Rotating fingerprint and restarting process")
                save_checkpoint({"last_url": None})
                time.sleep(random.uniform(10, 20))
                logger.info("Restarting process to refresh global state...")
                os.execv(sys.executable, ['python'] + sys.argv)

                time.sleep(random.uniform(15, 30))
                attempt += 1
                continue

            else:
                logger.error(f"Unexpected HTTP {resp.status_code}, waiting 2h")
                time.sleep(7200)
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
                logger.info(f"Checkpoint loaded: {data}")
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
                pagination = html_class.find("ul", class_="pagination")
                if pagination:
                    page_numbers = [
                        int(a.text) for a in pagination.find_all("a") if a.text.isdigit()
                    ]
                    if page_numbers:
                        max_page = max(page_numbers)
                    else:
                        max_page = 1


                logger.info(f"Parsing class {class_code}, {max_page} pages total.")
                time.sleep(1.4)

                batch = []
                records_since_last_save = 0

                for page in range(1, max_page + 1):
                    url_page = f"{url_class}?page={page}"
                    html_page = click_on_link(url_page)
                    if not html_page:
                        continue

                    companies = html_page.find_all("a", class_="link-details link-open")
                    for idx, comp in enumerate(companies):
                        company_code = comp.text.split(",")[0].strip()
                        url_details = "https://youcontrol.com.ua" + comp.get("href")

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
                            records_since_last_save += 1
                            human_delay()

                            if records_since_last_save >= 50:
                                df = pd.DataFrame(batch)
                                file_name = f"kved_{class_code}_batch_{uuid4().hex[:6]}.csv"
                                df.to_csv(file_name, index=False)
                                logger.info(f"Saved {len(batch)} rows → {file_name}")
                                save_checkpoint({"last_url": url_details})
                                batch.clear()
                                records_since_last_save = 0
                                smart_sleep(20, 40)

                        smart_sleep(3, 8)

                if batch:
                    df = pd.DataFrame(batch)
                    file_name = f"kved_{class_code}_final_{uuid4().hex[:6]}.csv"
                    df.to_csv(file_name, index=False)
                    logger.info(f"Saved remaining {len(batch)} rows → {file_name}")




                    smart_sleep(6, 10)

                save_checkpoint({"last_url": None})
                logger.info(f"Completed class {class_code}")
                smart_sleep(15, 25)

    logger.info("Completed parsing all KVEDs.")


if __name__ == "__main__":
    while True:
        try:
            parse_all_kved()
            logger.info("Parsing complete. Restarting in 10 minutes...")
            time.sleep(600)
        except Exception as e:
            logger.error(f"Global crash: {e}. Restarting in 60s...")
            time.sleep(60)
