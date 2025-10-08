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
import asyncio
from concurrent.futures import ThreadPoolExecutor

# === Асинхронна конфігурація ===
executor = ThreadPoolExecutor(max_workers=8)
semaphore = asyncio.Semaphore(5)

async def fetch_async(url):
    """Асинхронне виконання click_on_link через ThreadPool."""
    loop = asyncio.get_running_loop()  
    async with semaphore:
        await asyncio.sleep(random.uniform(1.0, 2.5))  # пауза для антибана
        return await loop.run_in_executor(executor, lambda: click_on_link(url))



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
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://youcontrol.com.ua/catalog/kved/",
        "Upgrade-Insecure-Requests": "1",
    }

# def get_headers():
#     return {"User-Agent": random.choice(USER_AGENTS)}

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


async def fetch_company_details(company_code, url_details, section_code, section_name,
                                chapter_code, chapter_name, current_group_code,
                                current_group_name, class_code, class_name):
    html_details = await fetch_async(url_details)
    if not html_details:
        return None

    # === Profile block ===
    block_profile = html_details.find("div", class_="seo-table-contain", id="catalog-company-file")
    profile_dict = {}
    if block_profile:
        profile_rows = block_profile.find_all("div", class_="seo-table-row")
        profile_data_columns = [r.find("div", class_="seo-table-col-1").text.strip() for r in profile_rows]
        raw_data = [
            (r.find("span", class_="copy-file-field") or
             r.find("div", class_="copy-file-field") or
             r.find("p", class_="ucfirst copy-file-field") or
             r.find("div", class_="seo-table-col-2")).text
            for r in profile_rows
            if (r.find("span", class_="copy-file-field") or
                r.find("div", class_="copy-file-field") or
                r.find("p", class_="ucfirst copy-file-field") or
                r.find("div", class_="seo-table-col-2"))
        ]
        profile_data_text = [clean_text(x) for x in raw_data if x.strip()]
        profile_dict = dict(zip(profile_data_columns, profile_data_text))

    # === Beneficiary block ===
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
    return row_data


# === Основна логіка ===
async def parse_all_kved():
    logger.info("Starting KVED parsing")

    # === Завантаження checkpoint ===
    last_completed = None
    checkpoint_class, checkpoint_page = None, None
    if os.path.exists("checkpoint.txt"):
        with open("checkpoint.txt") as f:
            line = f.read().strip()
            if "|" in line:
                checkpoint_class, checkpoint_page = line.split("|")
                checkpoint_page = int(checkpoint_page)
            else:
                last_completed = line
        logger.info(f"Checkpoint found: {line}")

    html_sections = await fetch_async(URL_BASE)
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
            html_chapter = await fetch_async(url_chapter)
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
                if last_completed:
                    if class_code == last_completed:
                        last_completed = None
                        continue
                    elif last_completed is not None:
                        continue

                class_name = class_code_td.find_next_sibling("td").text.strip()
                url_class = url_chapter + f'/{class_code[-2:]}'
                html_class = await fetch_async(url_class)
                if not html_class:
                    continue

                # === Визначаємо кількість сторінок ===
                pagination = html_class.find("ul", class_="pagination")
                max_page = 1
                if pagination:
                    for li in pagination.find_all("li"):
                        a = li.find("a")
                        if a and a.text.isdigit():
                            max_page = max(max_page, int(a.text))

                pages_to_parse = TARGET_PAGES if TARGET_PAGES else range(1, max_page + 1)
                logger.info(f"Parsing {max_page} pages for class {class_code}")

                for page in pages_to_parse:
                    # Пропуск сторінок, які вже оброблені до збою
                    if checkpoint_class == class_code and checkpoint_page and page <= checkpoint_page:
                        logger.info(f"Skipping page {page} (already parsed before crash)")
                        continue

                    url_page = f"{url_class}?page={page}"
                    html_page = await fetch_async(url_page)
                    if not html_page:
                        continue

                    raw_edrpous = html_page.find_all("a", class_="link-details link-open")
                    if not raw_edrpous:
                        continue

                    # === Асинхронна обробка компаній на сторінці ===
                    tasks = []
                    for raw_edrpou in raw_edrpous:
                        company_code = raw_edrpou.text.split(",")[0].strip()
                        url_details = f"https://youcontrol.com.ua{raw_edrpou.get('href')}"
                        tasks.append(fetch_company_details(
                            company_code, url_details,
                            section_code, section_name,
                            chapter_code, chapter_name,
                            current_group_code, current_group_name,
                            class_code, class_name
                        ))

                    results = await asyncio.gather(*tasks)
                    batch_data = [r for r in results if r]

                    # === Збереження після кожної сторінки ===
                    if batch_data:
                        df_batch = pd.DataFrame(batch_data)
                        file_path = f"kved_{class_code}_p{page}.csv"
                        df_batch.to_csv(file_path, mode='w', header=True, index=False)
                        logger.info(f"Saved class {class_code} page {page} to {file_path}")

                        with open("checkpoint.txt", "w") as f:
                            f.write(f"{class_code}|{page}")

                        await asyncio.sleep(random.uniform(10, 25))

                # === Фінальне збереження класу ===
                logger.info(f"Completed class {class_code}, saving combined CSV")
                all_parts = [f for f in os.listdir() if f.startswith(f'kved_{class_code}_p')]
                if all_parts:
                    df_final = pd.concat([pd.read_csv(f) for f in all_parts], ignore_index=True)
                    df_final.to_csv(f'kved_{class_code}.csv', index=False)
                    logger.info(f"Saved merged file kved_{class_code}.csv")
                    for f in all_parts:
                        os.remove(f)

                    with open("checkpoint.txt", "w") as f:
                        f.write(class_code)

                    await asyncio.sleep(random.uniform(20, 40))

    logger.info("Completed parsing all KVEDs.")

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(parse_all_kved())
            logger.info("Parsing complete. Restarting in 5 minutes.")
            time.sleep(300)
        except Exception as e:
            logger.error(f"Global crash: {e}. Restarting in 60s...")
            time.sleep(60)
