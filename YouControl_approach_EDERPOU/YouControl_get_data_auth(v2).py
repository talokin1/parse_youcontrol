# Youcontrol_async_selenium.py
# ПОВНІСТЮ автономний файл — зберегти як .py і запускати
# Потрібно: selenium, beautifulsoup4, pandas
# У Windows/Лінукс повинен бути chromedriver сумісний із локальним Chrome

import asyncio
from concurrent.futures import ThreadPoolExecutor
import time
import random
import csv
import os
import logging
from typing import Optional, List, Dict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import re
import pandas as pd

# ----------------- CONFIG -----------------
LOGIN_EMAIL = "your_email@example.com"
LOGIN_PASSWORD = "your_password"

EDRPOU_CSV = "edrpou_list.csv"   # CSV with single column 'EDRPOU' or plain newline list
OUTPUT_DIR = "out"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "companies_parsed.csv")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "checkpoint.txt")

CONCURRENCY = 3           # number of parallel webdriver instances (adjust to resources)
HEADLESS = False          # True to run Chrome headless (but visible browser is often safer)
IMPLICIT_WAIT = 6         # seconds for Selenium implicit waits
SEARCH_DELAY_MIN = 1.0    # random sleep before issuing search
SEARCH_DELAY_MAX = 2.5
BETWEEN_SEARCH_MIN = 2.0  # sleep between subsequent searches inside same driver
BETWEEN_SEARCH_MAX = 4.0
RETRY_MAX = 3
RETRY_BACKOFF_BASE = 3.0  # exponential backoff base seconds

# -------- logging ----------
os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, "youcontrol_async.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("youcontrol_async")

# --------------- Utilities ----------------
def clean_text(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip()

def load_edrpou_list(csv_path: str) -> List[str]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"{csv_path} not found")
    edrs = []
    # try simple CSV with header or plain lines
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            val = row[0].strip()
            if val.lower() == "edrpou" or val == "":
                continue
            edrs.append(val)
    return edrs

def save_checkpoint(last_index: int):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        f.write(str(last_index))

def load_checkpoint() -> Optional[int]:
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        txt = f.read().strip()
        if txt.isdigit():
            return int(txt)
    return None

def append_to_csv(row: Dict):
    df = pd.DataFrame([row])
    header = not os.path.exists(OUTPUT_CSV)
    df.to_csv(OUTPUT_CSV, mode='a', index=False, header=header, encoding='utf-8-sig')

# --------------- Selenium worker (blocking) ----------------
def create_driver(headless: bool = HEADLESS) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--start-maximized")
    # reduce fingerprinting but don't overdo it here
    opts.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=opts)
    driver.implicitly_wait(IMPLICIT_WAIT)
    return driver

def login_and_save_cookies(driver: webdriver.Chrome, email: str, password: str, save_cookies: bool = False):
    """Login to YouControl via given driver. Assumes chromedriver works."""
    logger.info("Logging in...")
    driver.get("https://youcontrol.com.ua/sign_in/")
    time.sleep(random.uniform(2.0, 4.0))
    # find fields (selectors from your initial script)
    try:
        el_login = driver.find_element(By.NAME, "LoginForm[login]")
        el_pass = driver.find_element(By.NAME, "LoginForm[password]")
        btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
    except NoSuchElementException:
        raise RuntimeError("Login fields not found on sign_in page (page layout changed?)")

    el_login.clear()
    el_login.send_keys(email)
    time.sleep(random.uniform(0.4, 0.9))
    el_pass.clear()
    el_pass.send_keys(password)
    time.sleep(random.uniform(0.3, 0.8))
    btn.click()
    # wait a bit to ensure logged in
    time.sleep(random.uniform(4.0, 6.5))
    # naive check - look for user menu or sign_out link presence
    if "sign_in" in driver.current_url or "login" in driver.current_url:
        logger.warning("Login may have failed — still on sign_in page.")
    else:
        logger.info("Login finished (check page).")

def perform_search_and_get_html(driver: webdriver.Chrome, edrpou: str) -> Optional[str]:
    """Type EDRPOU into search box and return page HTML after navigation."""
    try:
        # find search box: same selector used earlier
        search_box = driver.find_element(By.CSS_SELECTOR, "input[placeholder*='Введіть назву компанії, ЄДРПОУ']")
        # clear, input and submit
        search_box.clear()
        time.sleep(random.uniform(0.2, 0.6))
        search_box.send_keys(edrpou)
        time.sleep(random.uniform(0.2, 0.8))
        search_box.send_keys('\n')  # Enter
        # wait for results; give variable delay to mimic human
        time.sleep(random.uniform(2.0, 5.0))
        # if result redirects to company details, good; otherwise, ensure click the first result
        # try to check for direct company page by url or find link
        current_url = driver.current_url
        if "/company_details/" in current_url:
            html = driver.page_source
            return html
        # otherwise try to click first result link
        try:
            first_link = driver.find_element(By.CSS_SELECTOR, "a.link-details.link-open")
            first_link.click()
            time.sleep(random.uniform(1.8, 3.5))
            return driver.page_source
        except NoSuchElementException:
            logger.warning(f"No result link found for {edrpou}")
            return None
    except Exception as e:
        logger.exception(f"Search failed for {edrpou}: {e}")
        return None

def parse_company_youcontrol_html(html: str) -> Dict:
    """Reuse parsing logic from your auth(v1) — returns dict of fields (EDRPOU_CODE included)."""
    soup = BeautifulSoup(html, "lxml")
    result = {}

    # parse detail-view tables
    for table in soup.select("table.detail-view"):
        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(" ", strip=True)
            key = re.sub(r"\s*\(Актуально.+?\)\s*", "", key).strip()

            td_copy = BeautifulSoup(str(td), "lxml")
            # remove copy buttons and similar
            for bad in td_copy.select(".copy-done, .content-for-copy"):
                bad.decompose()

            if key.lower().startswith("види діяльності"):
                # parse activities as in your v1 script
                main_block = td_copy.select_one("div.localized-item.flex-activity")
                if main_block:
                    db = BeautifulSoup(str(main_block), "lxml")
                    for t in db.select("div.activity-tooltip-p, a"):
                        t.decompose()
                    text = db.get_text(" ", strip=True)
                    m = re.match(r"(\d{2}\.\d{2})\s+(.+)", text)
                    if m:
                        kved, desc = m.groups()
                        result["КВЕД_Основний"] = f"{kved} — {desc.strip()}"
                other_blocks = td_copy.select("li.localized-item.localized-other")[:3]
                for idx, li in enumerate(other_blocks, start=1):
                    li_copy = BeautifulSoup(str(li), "lxml")
                    for t in li_copy.select("div.activity-tooltip-p, a"):
                        t.decompose()
                    text = li_copy.get_text(" ", strip=True)
                    m = re.match(r"(\d{2}\.\d{2})\s+(.+)", text)
                    if m:
                        kved, desc = m.groups()
                        result[f"КВЕД_{idx}_Інші"] = f"{kved} — {desc.strip()}"
            elif key.lower().startswith("перелік засновників"):
                # founders block
                founder_cards = td_copy.select("div.info-additional-file#founder-card-list-block")
                for idx, card in enumerate(founder_cards[:5], start=1):
                    card_copy = BeautifulSoup(str(card), "lxml")
                    for t in card_copy.select(".copy-done, .content-for-copy, .check-individuals-link-search"):
                        t.decompose()
                    text = card_copy.get_text(" ", strip=True)
                    text = re.sub(r"\s+", " ", text).strip()
                    result[f"Cofounder_{idx}"] = text
            else:
                value = td_copy.get_text(" ", strip=True)
                value = re.sub(r"\s+", " ", value).strip()
                result[key] = value

    # EDRPOU from header if present
    edrpou_match = soup.find("h2", class_="seo-table-name case-icon short")
    if edrpou_match:
        m = re.search(r"\d+", edrpou_match.get_text())
        if m:
            result["EDRPOU_CODE"] = m.group()

    # clean all values
    result = {k: clean_text(v) for k, v in result.items() if v is not None}
    return result

def worker_task_blocking(edrpou: str, account_email: str, account_password: str, worker_id: int) -> Optional[Dict]:
    """
    Blocking function to run inside thread:
    - creates driver
    - logs in
    - searches given EDRPOU
    - parses and returns dict (or None)
    """
    logger.info(f"[W{worker_id}] Starting worker for {edrpou}")
    driver = None
    try:
        driver = create_driver()
        login_and_save_cookies(driver, account_email, account_password)
        # small random delay before search to vary timing
        time.sleep(random.uniform(SEARCH_DELAY_MIN, SEARCH_DELAY_MAX))

        # try multiple retries on transient failures
        attempt = 0
        while attempt < RETRY_MAX:
            html = perform_search_and_get_html(driver, edrpou)
            if html:
                parsed = parse_company_youcontrol_html(html)
                logger.info(f"[W{worker_id}] Parsed {edrpou}: {len(parsed)} fields")
                # polite pause before leaving
                time.sleep(random.uniform(BETWEEN_SEARCH_MIN, BETWEEN_SEARCH_MAX))
                return parsed
            else:
                backoff = RETRY_BACKOFF_BASE * (2 ** attempt) + random.uniform(0.5, 1.5)
                logger.warning(f"[W{worker_id}] No HTML for {edrpou}, retry {attempt+1}/{RETRY_MAX} after {backoff:.1f}s")
                time.sleep(backoff)
                attempt += 1
        logger.error(f"[W{worker_id}] Failed to get {edrpou} after retries")
        return None
    except Exception as e:
        logger.exception(f"[W{worker_id}] Worker exception for {edrpou}: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

# --------------- ASYNC ORCHESTRATION ----------------
async def parse_all_edrpous(edrpous: List[str]):
    loop = asyncio.get_running_loop()
    ex = ThreadPoolExecutor(max_workers=CONCURRENCY)
    checkpoint = load_checkpoint()
    start_idx = checkpoint + 1 if checkpoint is not None else 0
    logger.info(f"Starting parsing from index {start_idx} (total {len(edrpous)})")

    # create tasks in batches to avoid scheduling all at once
    for idx in range(start_idx, len(edrpous)):
        edrpou = edrpous[idx]
        # submit to executor
        parsed = await loop.run_in_executor(ex, worker_task_blocking, edrpou, LOGIN_EMAIL, LOGIN_PASSWORD, idx % CONCURRENCY)
        if parsed:
            # attach index and timestamp
            parsed["_EDRPOU_INDEX"] = idx
            parsed["_PARSE_TS"] = time.strftime("%Y-%m-%d %H:%M:%S")
            append_to_csv(parsed)
            logger.info(f"Saved {edrpou} (idx {idx}) to CSV")
        else:
            logger.warning(f"No data for {edrpou} (idx {idx})")

        # update checkpoint after each processed item
        save_checkpoint(idx)

        # slight randomized delay between tasks to reduce pattern
        await asyncio.sleep(random.uniform(0.5, 1.5))

    ex.shutdown(wait=True)
    logger.info("Completed all EDRPOU parsing")

# --------------- MAIN ----------------
def main():
    try:
        edrpous = load_edrpou_list(EDRPOU_CSV)
    except Exception as e:
        logger.error(f"Failed to load edrpou list: {e}")
        return

    logger.info(f"Loaded {len(edrpous)} EDRPOU codes")

    # run async orchestrator
    asyncio.run(parse_all_edrpous(edrpous))

if __name__ == "__main__":
    main()
