import bs4
import pycurl
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
TARGET_PAGES = [1]

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

def smart_sleep(base_min=3, base_max=7, long_pause_chance=0.01):
    delay = random.uniform(base_min, base_max)
    if random.random() < long_pause_chance:
        delay += random.uniform(20, 30)
        logger.debug("Long pause triggered...")
    logger.debug(f"Sleeping {delay:.2f} seconds...")
    time.sleep(delay)

# === Retry-завантаження ===
def click_on_link(url, max_retries=5, backoff_factor=2, long_wait=1800):
    """
    Improved: handles 303 redirects gracefully (e.g. Cloudflare).
    """
    attempt = 0
    consecutive_303 = 0

    while True:
        try:
            buffer = io.BytesIO()
            c = pycurl.Curl()
            c.setopt(c.URL, url)
            c.setopt(c.WRITEDATA, buffer)
            header_list = [f"{k}: {v}" for k, v in get_headers().items()]
            c.setopt(c.HTTPHEADER, header_list)
            c.setopt(c.FOLLOWLOCATION, True)
            c.setopt(c.MAXREDIRS, 5)
            c.setopt(c.CONNECTTIMEOUT, 10)
            c.setopt(c.TIMEOUT, 30)
            c.perform()

            status_code = c.getinfo(c.RESPONSE_CODE)
            c.close()

            # === OK
            if status_code == 200:
                html = buffer.getvalue().decode("utf-8", errors="ignore")
                smart_sleep()
                consecutive_303 = 0
                logger.info(f"✅ Success: {url}")
                return bs4.BeautifulSoup(html, "lxml")

            # === Cloudflare redirect or captcha
            elif status_code == 303:
                consecutive_303 += 1
                delay = 300  # 5 хвилин
                logger.warning(f"Got HTTP 303 (Cloudflare redirect). Sleeping {delay}s...")
                time.sleep(delay)

                # якщо 303 триває довше 5 разів підряд → довга пауза
                if consecutive_303 >= 5:
                    logger.error(f"Site may be in protection mode. Waiting {long_wait/60:.0f} min...")
                    time.sleep(long_wait)
                    consecutive_303 = 0
                continue

            # === Server down
            elif status_code in [429, 500, 502, 503, 504]:
                attempt += 1
                delay = min((backoff_factor ** attempt) + random.uniform(1, 3), 300)
                logger.warning(f"[{attempt}] Server error {status_code}. Retrying in {delay:.1f}s...")
                time.sleep(delay)

                if attempt >= max_retries:
                    logger.error(f"Server still down. Waiting {long_wait/60:.0f} min...")
                    time.sleep(long_wait)
                    attempt = 0
                continue

            else:
                logger.error(f"Unexpected HTTP {status_code}. Waiting 60s...")
                time.sleep(60)
                continue

        except Exception as e:
            logger.warning(f"Network exception {e}. Retrying in 10s...")
            time.sleep(10)



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
                    if last_completed and class_code <= last_completed:
                        continue

                    if class_code != TARGET_CLASS_CODE:
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

                    for page in pages_to_parse:
                        url_page = url_class + f'?page={page}'
                        html_page = click_on_link(url_page)
                        if not html_page:
                            continue

                        raw_edrpous = html_page.find_all("a", class_="link-details link-open")
                        if not raw_edrpous:
                            continue

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
