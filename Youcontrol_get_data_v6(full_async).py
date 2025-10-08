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
import json
from datetime import datetime
from typing import Optional, Dict, List, Tuple
import traceback

# === Конфігурація ===
TARGET_CLASS_CODE = "01.11"
TARGET_PAGES = None
MAX_RETRIES = 5
LONG_WAIT = 1800
MAX_WORKERS = 5  # Зменшено для стабільності
SEMAPHORE_LIMIT = 3  # Зменшено для уникнення блокування

# === Налаштування логування ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youcontrol_scraper.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# === URLs та User Agents ===
URL_BASE = "https://youcontrol.com.ua/catalog/kved/"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# === Checkpoint Manager ===
class CheckpointManager:
    def __init__(self, filename="checkpoint.json"):
        self.filename = filename
        self.data = self.load()
    
    def load(self) -> Dict:
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading checkpoint: {e}")
                return {}
        return {}
    
    def save(self, class_code: str, page: int, company_index: int = 0):
        self.data = {
            "class_code": class_code,
            "page": page,
            "company_index": company_index,
            "timestamp": datetime.now().isoformat()
        }
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
    
    def should_skip(self, class_code: str, page: int, company_index: int = 0) -> bool:
        if not self.data:
            return False
        
        saved_class = self.data.get("class_code")
        saved_page = self.data.get("page", 0)
        saved_company = self.data.get("company_index", 0)
        
        if saved_class != class_code:
            return saved_class and saved_class > class_code
        
        if saved_page > page:
            return True
        elif saved_page == page:
            return company_index <= saved_company
        
        return False
    
    def clear(self):
        self.data = {}
        if os.path.exists(self.filename):
            os.remove(self.filename)

# === Session Manager ===
class SessionManager:
    def __init__(self):
        self.scraper = None
        self.create_session()
    
    def create_session(self):
        """Створює нову сесію з обходом Cloudflare"""
        self.scraper = cloudscraper.create_scraper(
            delay=10,
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
    
    def get_headers(self) -> Dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://youcontrol.com.ua/catalog/kved/",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0"
        }
    
    def fetch(self, url: str, max_retries: int = MAX_RETRIES) -> Optional[bs4.BeautifulSoup]:
        """Виконує запит з автоматичними retry та обробкою помилок"""
        for attempt in range(max_retries):
            try:
                logger.info(f"[Attempt {attempt+1}/{max_retries}] Fetching: {url}")
                
                # Додаємо випадкову паузу перед запитом
                if attempt > 0:
                    delay = min(2 ** attempt * random.uniform(1, 2), 60)
                    logger.info(f"Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                
                resp = self.scraper.get(url, headers=self.get_headers(), timeout=30)
                
                if resp.status_code == 200:
                    html = resp.text
                    # Перевірка на Cloudflare challenge
                    if "Checking your browser" in html or "cf-browser-verification" in html:
                        logger.warning("Cloudflare challenge detected, recreating session...")
                        self.create_session()
                        continue
                    
                    return bs4.BeautifulSoup(html, "lxml")
                
                elif resp.status_code == 429:
                    # Rate limiting
                    wait_time = min(300 * (attempt + 1), LONG_WAIT)
                    logger.warning(f"Rate limited (429). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    self.create_session()  # Оновлюємо сесію
                    
                elif resp.status_code in [500, 502, 503, 504]:
                    logger.warning(f"Server error {resp.status_code}")
                    continue
                    
                elif resp.status_code == 403:
                    logger.error(f"Access forbidden (403). Waiting 30 min...")
                    time.sleep(1800)
                    self.create_session()
                    
                else:
                    logger.error(f"Unexpected status code: {resp.status_code}")
                    
            except cloudscraper.exceptions.CloudflareChallengeError as e:
                logger.warning(f"Cloudflare challenge error: {e}")
                self.create_session()
                
            except Exception as e:
                logger.error(f"Request error: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    
        return None

# === Async wrapper для SessionManager ===
async def fetch_async(session_manager: SessionManager, url: str, semaphore: asyncio.Semaphore) -> Optional[bs4.BeautifulSoup]:
    """Асинхронна обгортка для синхронного fetch"""
    async with semaphore:
        # Додаємо випадкову затримку для розподілу навантаження
        await asyncio.sleep(random.uniform(0.5, 2.0))
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, session_manager.fetch, url)

# === Утиліти ===
def clean_text(text: str) -> str:
    """Очищення тексту від зайвих пробілів"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def safe_find_text(element, *args, default="", **kwargs) -> str:
    """Безпечний пошук тексту в елементі"""
    try:
        found = element.find(*args, **kwargs) if element else None
        return clean_text(found.text) if found else default
    except Exception as e:
        logger.debug(f"Error finding text: {e}")
        return default

# === Парсинг компаній ===
async def parse_company_details(
    session_manager: SessionManager,
    company_code: str,
    url_details: str,
    section_code: str,
    section_name: str,
    chapter_code: str,
    chapter_name: str,
    group_code: str,
    group_name: str,
    class_code: str,
    class_name: str,
    semaphore: asyncio.Semaphore
) -> Optional[Dict]:
    """Парсинг деталей компанії"""
    
    try:
        html_details = await fetch_async(session_manager, url_details, semaphore)
        if not html_details:
            logger.warning(f"Failed to fetch details for company {company_code}")
            return None
        
        # Базові дані
        row_data = {
            "SECTION_CODE": section_code,
            "SECTION_NAME": section_name,
            "CHAPTER_CODE": chapter_code,
            "CHAPTER_NAME": chapter_name,
            "GROUP_CODE": group_code,
            "GROUP_NAME": group_name,
            "CLASS_CODE": class_code,
            "CLASS_NAME": class_name,
            "EDRPOU_CODE": company_code,
            "URL": url_details,
            "PARSED_AT": datetime.now().isoformat()
        }
        
        # === Парсинг профілю компанії ===
        try:
            block_profile = html_details.find("div", class_="seo-table-contain", id="catalog-company-file")
            if block_profile:
                profile_rows = block_profile.find_all("div", class_="seo-table-row")
                for row in profile_rows:
                    try:
                        col_name = safe_find_text(row, "div", class_="seo-table-col-1")
                        if not col_name:
                            continue
                        
                        # Шукаємо значення в різних можливих елементах
                        value = None
                        for selector in [
                            ("span", {"class": "copy-file-field"}),
                            ("div", {"class": "copy-file-field"}),
                            ("p", {"class": "ucfirst copy-file-field"}),
                            ("div", {"class": "seo-table-col-2"})
                        ]:
                            element = row.find(*selector)
                            if element:
                                value = clean_text(element.text)
                                break
                        
                        if value:
                            # Нормалізуємо назву колонки
                            col_key = re.sub(r'[^\w\s]', '', col_name).replace(' ', '_').upper()
                            row_data[f"PROFILE_{col_key}"] = value
                            
                    except Exception as e:
                        logger.debug(f"Error parsing profile row: {e}")
        except Exception as e:
            logger.warning(f"Error parsing profile block for {company_code}: {e}")
        
        # === Парсинг бенефіціарів ===
        try:
            block_beneficiary = html_details.find("div", class_="seo-table-contain", id="catalog-company-beneficiary")
            if block_beneficiary:
                beneficiary_rows = block_beneficiary.find_all("div", class_="seo-table-row")
                for row in beneficiary_rows:
                    try:
                        col_name = safe_find_text(row, "div", class_="seo-table-col-1")
                        if not col_name:
                            continue
                        
                        # Шукаємо значення
                        value = None
                        span_field = row.find("span", class_="copy-file-field")
                        if span_field:
                            value = clean_text(span_field.text)
                        else:
                            div_field = row.find("div", class_="seo-table-col-2")
                            if div_field and 'copy-hover' not in div_field.get("class", []):
                                value = clean_text(div_field.text)
                        
                        if value:
                            col_key = re.sub(r'[^\w\s]', '', col_name).replace(' ', '_').upper()
                            row_data[f"BENEFICIARY_{col_key}"] = value
                            
                    except Exception as e:
                        logger.debug(f"Error parsing beneficiary row: {e}")
                        
                # Додатково витягуємо ЄДРПОУ з заголовка
                h2_element = html_details.find("h2", class_="seo-table-name case-icon short")
                if h2_element:
                    raw_edrpou = h2_element.text
                    edrpou_match = re.search(r'\d{8,10}', raw_edrpou)
                    if edrpou_match:
                        row_data["EDRPOU_FROM_TITLE"] = edrpou_match.group()
                        
        except Exception as e:
            logger.warning(f"Error parsing beneficiary block for {company_code}: {e}")
        
        return row_data
        
    except Exception as e:
        logger.error(f"Critical error parsing company {company_code}: {e}\n{traceback.format_exc()}")
        return None

# === Основна функція парсингу ===
async def parse_all_kved():
    """Головна функція парсингу всіх КВЕД"""
    
    logger.info("=" * 50)
    logger.info("Starting KVED parsing")
    logger.info("=" * 50)
    
    # Ініціалізація
    session_manager = SessionManager()
    checkpoint = CheckpointManager()
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    
    # Завантаження головної сторінки
    html_sections = session_manager.fetch(URL_BASE)
    if not html_sections:
        logger.error("Cannot fetch main KVED catalog page")
        return
    
    section_blocks = html_sections.find_all("div", class_="kved-catalog-table")
    logger.info(f"Found {len(section_blocks)} section blocks")
    
    total_companies_parsed = 0
    start_time = time.time()
    
    for section_block in section_blocks:
        try:
            section_code = safe_find_text(section_block, "td", class_="green-col-word")
            section_name = safe_find_text(section_block, "td", class_="caps-col")
            
            if not section_code:
                continue
            
            logger.info(f"\nProcessing section: {section_code} - {section_name}")
            
            chapter_tds = section_block.find_all("td", class_="green-col-num")
            
            for chapter_td in chapter_tds:
                try:
                    chapter_code = clean_text(chapter_td.text)
                    chapter_name = safe_find_text(chapter_td.find_next_sibling("td"))
                    
                    if not chapter_code:
                        continue
                    
                    logger.info(f"  Processing chapter: {chapter_code} - {chapter_name}")
                    
                    url_chapter = f"{URL_BASE}{chapter_code}"
                    html_chapter = session_manager.fetch(url_chapter)
                    if not html_chapter:
                        continue
                    
                    table = html_chapter.find("table")
                    if not table:
                        logger.warning(f"    No table found for chapter {chapter_code}")
                        continue
                    
                    rows = table.find_all("tr")
                    current_group_code = None
                    current_group_name = None
                    
                    for row in rows:
                        try:
                            # Перевірка на групу
                            group_code_td = row.find("td", class_="green-col-word")
                            if group_code_td:
                                current_group_code = clean_text(group_code_td.text)
                                current_group_name = safe_find_text(row, "td", class_="caps-col")
                                logger.info(f"    Found group: {current_group_code} - {current_group_name}")
                                continue
                            
                            # Перевірка на клас
                            class_code_td = row.find("td", class_="green-col-num")
                            if not class_code_td or not current_group_code:
                                continue
                            
                            class_code = clean_text(class_code_td.text)
                            
                            # Перевірка checkpoint для пропуску оброблених класів
                            if checkpoint.should_skip(class_code, 0):
                                logger.info(f"      Skipping class {class_code} (already processed)")
                                continue
                            
                            # Фільтр за TARGET_CLASS_CODE якщо вказано
                            if TARGET_CLASS_CODE and class_code != TARGET_CLASS_CODE:
                                continue
                            
                            class_name = safe_find_text(class_code_td.find_next_sibling("td"))
                            logger.info(f"      Processing class: {class_code} - {class_name}")
                            
                            url_class = f"{url_chapter}/{class_code[-2:]}"
                            html_class = session_manager.fetch(url_class)
                            if not html_class:
                                continue
                            
                            # Визначення кількості сторінок
                            pagination = html_class.find("ul", class_="pagination")
                            max_page = 1
                            if pagination:
                                for li in pagination.find_all("li"):
                                    a = li.find("a")
                                    if a and a.text.isdigit():
                                        max_page = max(max_page, int(a.text))
                            
                            pages_to_parse = TARGET_PAGES if TARGET_PAGES else range(1, max_page + 1)
                            logger.info(f"        Found {max_page} pages for class {class_code}")
                            
                            class_companies = []
                            
                            for page in pages_to_parse:
                                # Перевірка checkpoint для сторінки
                                if checkpoint.should_skip(class_code, page):
                                    logger.info(f"          Skipping page {page} (already processed)")
                                    continue
                                
                                logger.info(f"          Parsing page {page}/{max_page}")
                                
                                url_page = f"{url_class}?page={page}"
                                html_page = session_manager.fetch(url_page)
                                if not html_page:
                                    continue
                                
                                # Знаходимо всі компанії на сторінці
                                raw_edrpous = html_page.find_all("a", class_="link-details link-open")
                                if not raw_edrpous:
                                    logger.warning(f"            No companies found on page {page}")
                                    continue
                                
                                logger.info(f"            Found {len(raw_edrpous)} companies on page {page}")
                                
                                # Створюємо завдання для асинхронного парсингу
                                tasks = []
                                for idx, raw_edrpou in enumerate(raw_edrpous):
                                    # Перевірка checkpoint для компанії
                                    if checkpoint.should_skip(class_code, page, idx):
                                        continue
                                    
                                    try:
                                        company_text = raw_edrpou.text.split(",")[0].strip()
                                        company_code = re.search(r'\d{8,10}', company_text)
                                        if not company_code:
                                            continue
                                        company_code = company_code.group()
                                        
                                        url_details = f"https://youcontrol.com.ua{raw_edrpou.get('href')}"
                                        
                                        tasks.append(parse_company_details(
                                            session_manager,
                                            company_code,
                                            url_details,
                                            section_code,
                                            section_name,
                                            chapter_code,
                                            chapter_name,
                                            current_group_code,
                                            current_group_name,
                                            class_code,
                                            class_name,
                                            semaphore
                                        ))
                                        
                                    except Exception as e:
                                        logger.error(f"Error processing company link: {e}")
                                
                                # Виконуємо парсинг батчами
                                batch_size = 10
                                for i in range(0, len(tasks), batch_size):
                                    batch_tasks = tasks[i:i+batch_size]
                                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                                    
                                    # Обробка результатів
                                    batch_data = []
                                    for result in results:
                                        if isinstance(result, Exception):
                                            logger.error(f"Task failed: {result}")
                                        elif result:
                                            batch_data.append(result)
                                            total_companies_parsed += 1
                                    
                                    # Збереження батчу
                                    if batch_data:
                                        class_companies.extend(batch_data)
                                        
                                        # Зберігаємо проміжні результати
                                        df_batch = pd.DataFrame(batch_data)
                                        temp_file = f"temp_kved_{class_code}_p{page}_b{i//batch_size}.csv"
                                        df_batch.to_csv(temp_file, index=False, encoding='utf-8')
                                        logger.info(f"              Saved batch to {temp_file}")
                                    
                                    # Оновлюємо checkpoint
                                    checkpoint.save(class_code, page, min(i+batch_size, len(raw_edrpous)))
                                    
                                    # Пауза між батчами
                                    await asyncio.sleep(random.uniform(2, 5))
                                
                                # Велика пауза між сторінками
                                await asyncio.sleep(random.uniform(10, 20))
                            
                            # Збереження всіх даних для класу
                            if class_companies:
                                df_class = pd.DataFrame(class_companies)
                                final_file = f"kved_{class_code}_final.csv"
                                df_class.to_csv(final_file, index=False, encoding='utf-8')
                                logger.info(f"        Saved {len(class_companies)} companies to {final_file}")
                                
                                # Видалення тимчасових файлів
                                temp_files = [f for f in os.listdir() if f.startswith(f"temp_kved_{class_code}_")]
                                for temp_file in temp_files:
                                    try:
                                        os.remove(temp_file)
                                    except:
                                        pass
                            
                            # Оновлення checkpoint після завершення класу
                            checkpoint.save(class_code, max_page + 1, 0)
                            
                            # Статистика
                            elapsed = time.time() - start_time
                            rate = total_companies_parsed / (elapsed / 3600) if elapsed > 0 else 0
                            logger.info(f"        Progress: {total_companies_parsed} companies parsed ({rate:.0f}/hour)")
                            
                            # Велика пауза між класами
                            await asyncio.sleep(random.uniform(30, 60))
                            
                        except Exception as e:
                            logger.error(f"Error processing row in chapter {chapter_code}: {e}\n{traceback.format_exc()}")
                            
                except Exception as e:
                    logger.error(f"Error processing chapter {chapter_code}: {e}\n{traceback.format_exc()}")
                    
        except Exception as e:
            logger.error(f"Error processing section: {e}\n{traceback.format_exc()}")
    
    # Фінальна статистика
    total_time = time.time() - start_time
    logger.info("=" * 50)
    logger.info(f"Parsing completed!")
    logger.info(f"Total companies parsed: {total_companies_parsed}")
    logger.info(f"Total time: {total_time/3600:.2f} hours")
    logger.info(f"Average rate: {total_companies_parsed/(total_time/3600):.0f} companies/hour")
    logger.info("=" * 50)

# === Головна функція з автоматичним перезапуском ===
async def main():
    """Головна функція з обробкою помилок та перезапуском"""
    
    while True:
        try:
            await parse_all_kved()
            logger.info("Parsing completed successfully. Waiting 30 sec before next run...")
            await asyncio.sleep(30)
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user. Exiting...")
            break
            
        except Exception as e:
            logger.error(f"Critical error in main loop: {e}\n{traceback.format_exc()}")
            logger.info("Restarting in 60 seconds...")
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program terminated by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}\n{traceback.format_exc()}")