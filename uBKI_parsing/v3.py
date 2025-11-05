# ubki_big_parser.py
import asyncio
import csv
import json
import logging
import math
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import re

import httpx
import pandas as pd
from bs4 import BeautifulSoup

# Optional: cloudscraper for fallback when Cloudflare blocks
try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
except Exception:
    CLOUDSCRAPER_AVAILABLE = False

# ----------------------
# CONFIG
# ----------------------
INPUT_CSV = r"C:\OTP Draft\YouControl\uBKI_parsing\production\companies.csv"   
OUTPUT_CSV = "ubki_parsed_results.csv"
CHECKPOINT_FILE = "ubki_checkpoint.json"
LOG_FILE = "ubki_parser.log"

CONCURRENCY = 10                    # скільки одночасних запитів
REQUEST_TIMEOUT = 20                # сек
RETRY_MAX = 4                       # скільки повторів при помилках
RETRY_BACKOFF_BASE = 2.0            # степінь для backoff
RETRY_JITTER = 1.0                  # додаткові секунди jitter
SAVE_EVERY = 50                     # чекпоінт: зберігати кожні 50 компаній
RETRY_FOR_NOT_FOUND = 3             # скільки разів переспробувати коли "Дані не знайдено"
NOT_FOUND_RETRY_DELAY = 60          # секунда початкова затримка перед повторним парсингом (буде зростати)

PROXIES = []  

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:116.0) Gecko/20100101 Firefox/116.0",
]

BASE_URL_TEMPLATE = "https://edrpou.ubki.ua/ua/{edrpou}"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
logger = logging.getLogger(__name__)

# ----------------------
# USER PARSERS: встав / імпортуй сюди свої функції з ноутбука
# ----------------------
# Ти кажуть, що у тебе вже є парсери parse_finrep, parse_msb_score, parse_bankruptcy, parse_tax_data тощо.
# Найпростіший спосіб — скопіювати їх сюди або імпортувати з модуля.
#
# Приклад шаблону функції парсера, яку використовує main parser:
def clean_text(text: str) -> str:
    """Очищує текст від пробілів, табів і переносів."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def get_soup(edrpou):
    """Отримує HTML сторінку UBKI."""
    url = f"https://edrpou.ubki.ua/ua/{edrpou}"
    scraper = cloudscraper.create_scraper()
    try:
        resp = scraper.get(url, timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        return None

# --- USER: paste your parse_* functions here, or import them ---
# Example minimal parser (підклади свої функції):
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from pprint import pprint


def parse_ubki_universal(edrpou):
    """Парсить дані про компанію UBKI за ЄДРПОУ"""
    url = f"https://edrpou.ubki.ua/ua/{edrpou}"
    scraper = cloudscraper.create_scraper()
    html = scraper.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    card = soup.find("div", id="anchor_ident")
    if not card:
        print(f"[!] Дані не знайдено для {edrpou}")
        return None

    data = {"ЄДРПОУ": edrpou}

    # --- Основна інформація ---
    header_block = card.find("div", class_="dr_row_spacebetween")
    if header_block:
        title = header_block.find("h2", class_="dr_value_title")
        if title:
            data["Повна назва"] = " ".join(title.get_text().split())

        status = header_block.find("div", class_="dr_value_state")
        if status:
            data["Статус"] = " ".join(status.get_text().split())

        for div in header_block.select(".dr_margin_16"):
            txt = " ".join(div.get_text().split())
            if "Актуально" in txt:
                data["Актуально на"] = txt.replace("Актуально на", "").strip(". ")
            elif "Останні зміни" in txt:
                data["Останні зміни"] = txt.replace("Останні зміни", "").strip(". ")

    # --- Простi поля ---
    for block in card.select(".dr_column.dr_padding_1"):
        subtitle = block.find("div", class_="dr_value_subtitle")
        value = block.find("div", class_="dr_value")
        if subtitle and value:
            key = subtitle.get_text(strip=True)
            val = " ".join(value.get_text().split())
            data[key] = val

    # --- Статутний капітал ---
    statcap_text = card.find("text", string=lambda s: s and "грн" in s)
    if statcap_text:
        parent = statcap_text.find_parent("text")
        if parent:
            full_text = parent.get_text(" ", strip=True)
            for part in full_text.split():
                if "," in part and any(ch.isdigit() for ch in part):
                    data["Статутний капітал"] = part + " грн"
                    break

    # --- Уповноважені особи ---
    authorized_block = card.find("div", class_="dr_value_subtitle", string=lambda s: s and "Уповноважені особи" in s)
    if authorized_block:
        authorized_list = []
        for li in authorized_block.find_next("ul").select("li"):
            person = {}
            name_span = li.find("span")
            role_span = li.find("span", class_="dr_signer_role")
            if name_span:
                person["ПІБ"] = name_span.get_text(strip=True)
            if role_span:
                person["Роль"] = role_span.get_text(strip=True)
            if person:
                authorized_list.append(person)
        if authorized_list:
            data["Уповноважені особи"] = authorized_list

    # --- Засновники ---
    founders_block = card.find("div", id="anchor_zasovniki")
    if founders_block:
        founders = []
        for f in founders_block.find_all_next("div", class_="dr_margin_12"):
            # Зупинка перед "Бенефіціари"
            if f.find_previous("div", class_="dr_value_subtitle", string=lambda s: s and "Бенефіціари" in s):
                break

            person = {}
            name_tag = f.find("div", class_="dr_value")
            if name_tag:
                person["ПІБ / Назва"] = name_tag.get_text(strip=True)

            country_tag = f.find("div", class_="dr_value_small", string=lambda s: s and "Країна" in s)
            if country_tag and country_tag.find("b"):
                person["Країна"] = country_tag.find("b").get_text(strip=True)

            contrib_tag = f.find("div", class_="dr_value_small", string=lambda s: s and "Розмір внеску" in s)
            if contrib_tag and contrib_tag.find("b"):
                person["Розмір внеску"] = contrib_tag.find("b").get_text(strip=True).replace(u'\xa0', ' ')

            if person:
                founders.append(person)
        if founders:
            data["Засновники"] = founders

    # --- Бенефіціари ---
    ben_block = card.find("div", class_="dr_value_subtitle", string=lambda s: s and "Бенефіціари" in s)
    if ben_block:
        beneficiaries = []
        for b in ben_block.find_all_next("div", class_="dr_margin_12"):
            person = {}
            name_tag = b.find("div", class_="dr_value")
            if name_tag:
                person["ПІБ"] = name_tag.get_text(strip=True)

            country_tag = b.find("div", class_="dr_value_small", string=lambda s: s and "громадянства" in s)
            if country_tag and country_tag.find("b"):
                person["Країна"] = country_tag.find("b").get_text(strip=True)

            type_tag = b.find("div", class_="dr_value_small", string=lambda s: s and "Тип бенефіціарного" in s)
            if type_tag and type_tag.find("b"):
                person["Тип володіння"] = type_tag.find("b").get_text(strip=True)

            share_tag = b.find("div", class_="dr_value_small", string=lambda s: s and "Відсоток" in s)
            if share_tag:
                person["Частка"] = share_tag.get_text(strip=True).split(":")[-1].strip()

            if person:
                beneficiaries.append(person)
        if beneficiaries:
            data["Бенефіціари"] = beneficiaries

    # --- Види діяльності ---
    kveds = [a.get_text(strip=True) for a in card.select("a.dr_kved_blk_lnk")]
    if kveds:
        data["Види діяльності"] = "; ".join(kveds)

    return data

def parse_ubki_violations(edrpou):
    """Парсить блок 'Заборона брати участь у тендерах від АМКУ'"""
    url = f"https://edrpou.ubki.ua/ua/{edrpou}"
    scraper = cloudscraper.create_scraper()
    html = scraper.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    card = soup.find("div", id="anchor_violations")
    data = {"ЄДРПОУ": edrpou}

    if not card:
        data["АМКУ тендери"] = "Блок не знайдено"
        return data

    text_block = card.find_next("div", class_="dr_value_state")
    if text_block:
        data["АМКУ тендери"] = " ".join(text_block.get_text().split())
    else:
        data["АМКУ тендери"] = "Інформація відсутня"

    return data

def parse_msb_score(soup, edrpou):
    data = {"ЄДРПОУ": edrpou}

    card = soup.find("div", id="anchor_score")
    if not card:
        return {}

    score_value = soup.find("span", id="scoremsb")
    if score_value:
        data["МСБ скоринг (балів)"] = score_value.get_text(strip=True)

    score_level = soup.find("div", class_="vw-rating-value-text")
    if score_level:
        data["МСБ скоринг (рівень)"] = score_level.get_text(strip=True)

    score_date = soup.find("div", class_="vw-rating-datecnt")
    if score_date and "Дата розрахунку" in score_date.get_text():
        strong = score_date.find("strong")
        if strong:
            data["Дата скорингу"] = strong.get_text(strip=True)

    return data


def parse_bankruptcy(soup, edrpou):
    """Парсить блок 'Процедура банкрутства'"""
    data = {"ЄДРПОУ": edrpou}
    card = soup.find("div", id="anchor_bankruptcy")
    if not card:
        return {}

    info_block = card.find_next("div", class_="dr_value_state")
    if info_block:
        data["Банкрутство"] = clean_text(info_block.get_text())
    return data


def parse_finrep(soup, edrpou):
    """
    Парсить блок 'Фінансова звітність' (до поля 'Дохід' включно).
    """
    data = {"ЄДРПОУ": edrpou}

    # Знаходимо сам блок за id
    anchor = soup.find("div", id="anchor_finrep")
    if not anchor:
        return {}

    # Увесь контейнер із фінпоказниками — це батьківський div .dr_card
    card = anchor.find_parent("div", class_="dr_card")
    if not card:
        return {}

    # Збираємо всі фінансові колонки
    columns = card.select(".dr_column.dr_fin_column_1")

    for col in columns:
        subtitle = col.find("div", class_="dr_value_subtitle")
        value = col.find("div", class_="dr_value")
        if subtitle and value:
            key = clean_text(subtitle.get_text())
            val = clean_text(value.get_text())
            data[key] = val

            # Зупиняємось після поля "Дохід"
            if key.strip().lower().startswith("дохід"):
                break

    return data

def parse_tax_data(soup, edrpou):
    """
    Парсить блок 'Податкові дані' (anchor_podatki)
    — включно до кінця секції, з усіма фінансовими полями.
    """
    data = {"ЄДРПОУ": edrpou}

    # Знаходимо якір
    anchor = soup.find("div", id="anchor_podatki")
    if not anchor:
        return {}

    # Піднімаємося до контейнера .dr_card
    card = anchor.find_parent("div", class_="dr_card")
    if not card:
        return {}

    # --- Назва секції ---
    title = anchor.find("h2", class_="dr_value_title")
    if title:
        data["Податкові дані (назва)"] = clean_text(title.get_text())

    # --- Попереджувальна панель (якщо є) ---
    orange_panel = card.find("div", class_="dr_orange_panel")
    if orange_panel:
        data["Податкові дані (примітка)"] = clean_text(orange_panel.get_text())

    # --- Платник ПДВ / не є платником ---
    vat_state = card.find("b", class_="dr_value_state")
    if vat_state:
        data["Статус ПДВ"] = clean_text(vat_state.get_text())

    # --- Основні фінансові пункти ---
    for col in card.select(".dr_column.dr_padding_1"):
        subtitle = col.find("div", class_="dr_value_subtitle")
        value = col.find("div", class_="dr_value")
        if subtitle and value:
            key = clean_text(subtitle.get_text())
            val = clean_text(value.get_text())
            data[key] = val

    return data


from bs4 import BeautifulSoup

def clean_text(s: str) -> str:
    return " ".join(s.split()) if s else ""

def parse_courts(soup, edrpou):
    """
    Парсить блок 'Суди' (id='anchor_susd') — саме табличну частину.
    Повертає список справ із вкладеними документами.
    """
    block = soup.find("div", id="anchor_susd")
    if not block:
        return {}

    table_body = block.select_one("#tsusd_cases_table_body")
    if not table_body:
        return {"ЄДРПОУ": edrpou, "Суди (справи)": []}

    cases = []
    current_case = None

    for row in table_body.find_all("div", class_="dr_court-table-row", recursive=False):
        # верхній ряд — сама справа
        counter = row.find("div", class_="counter")
        cols = row.find_all("div", recursive=False)

        # якщо це основна справа (має лінк або роль)
        if len(cols) >= 5 and "Номер справи" not in row.get_text():
            n_case = clean_text(cols[1].get_text())
            role = clean_text(cols[2].get_text())
            instance = clean_text(cols[3].get_text())
            status = clean_text(cols[4].get_text())

            current_case = {
                "Номер справи": n_case,
                "Роль": role,
                "Інстанція": instance,
                "Стан розгляду": status,
                "Документи": []
            }
            cases.append(current_case)

        # якщо це вкладена підтаблиця документів
        elif "dr_court-subtable-row" in row.get("class", []):
            cols = row.find_all("div", recursive=False)
            if len(cols) >= 5 and current_case:
                doc = {
                    "№": clean_text(cols[0].get_text()),
                    "Номер документа": clean_text(cols[1].get_text()),
                    "Дата": clean_text(cols[2].get_text()),
                    "Тип": clean_text(cols[3].get_text()),
                    "Суд": clean_text(cols[4].get_text())
                }
                current_case["Документи"].append(doc)

    return {"ЄДРПОУ": edrpou, "Суди (справи)": cases}



# Якщо в тебе повні функції, заміни parse_ubki_full нижче, або виклич їх всередині.

def parse_ubki_full(soup, edrpou):

    # окремі блоки
    base = parse_ubki_universal(edrpou)
    score = parse_msb_score(soup, edrpou)
    bankrupt = parse_bankruptcy(soup, edrpou)
    finrep = parse_finrep(soup, edrpou)
    tax = parse_tax_data(soup, edrpou)
    courts = parse_courts(soup, edrpou)

    merged = {**(base or {}), **(score or {}), **(bankrupt or {}), **(finrep or {}), **(tax or {}), **(courts or {})}
    return merged

# ----------------------
# HTTP helpers
# ----------------------
def pick_user_agent() -> str:
    return random.choice(USER_AGENTS)


def pick_proxy() -> Optional[str]:
    if not PROXIES:
        return None
    return random.choice(PROXIES)


async def fetch_with_httpx(client: httpx.AsyncClient, url: str, edrpou: str, attempt: int) -> Optional[str]:
    headers = {"User-Agent": pick_user_agent(), "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8"}
    proxy = pick_proxy()
    try:
        resp = await client.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        text = resp.text
        return text
    except httpx.HTTPStatusError as e:
        logger.warning("HTTP error for %s (attempt %d): %s", edrpou, attempt, e)
    except (httpx.TransportError, httpx.ReadTimeout) as e:
        logger.warning("Transport/Timeout for %s (attempt %d): %s", edrpou, attempt, e)
    except Exception as e:
        logger.exception("Unexpected fetch error for %s (attempt %d): %s", edrpou, attempt, e)
    return None


def fetch_with_cloudscraper_sync(url: str, edrpou: str) -> Optional[str]:
    if not CLOUDSCRAPER_AVAILABLE:
        logger.debug("cloudscraper not available")
        return None
    try:
        scraper = cloudscraper.create_scraper()
        html = scraper.get(url, timeout=REQUEST_TIMEOUT).text
        return html
    except Exception as e:
        logger.warning("cloudscraper fallback failed for %s: %s", edrpou, e)
        return None

# ----------------------
# Retry/backoff utility
# ----------------------
def backoff_delay(attempt: int, base: float = RETRY_BACKOFF_BASE) -> float:
    # exponential with jitter
    delay = (base ** (attempt - 1)) + random.random() * RETRY_JITTER
    return delay

# ----------------------
# Checkpointing
# ----------------------
def load_checkpoint() -> Dict[str, Any]:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed": {}, "pending": {}, "retry": {}}


def save_checkpoint(state: Dict[str, Any]):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)


# ----------------------
# Main worker logic
# ----------------------
class UBKIParser:
    def __init__(self, edrpou_list: List[str]):
        self.edrpou_list = edrpou_list
        self.results: List[Dict[str, Any]] = []
        self.processed = {}   # edrpou -> metadata
        self.pending = {}     # edrpou -> metadata
        self.retry_queue = {} # edrpou -> {"attempts": int, "next_try_ts": float}
        self._load_state()
        self._lock = asyncio.Lock()
        self._save_counter = 0

    def _load_state(self):
        state = load_checkpoint()
        self.processed = state.get("processed", {})
        self.pending = state.get("pending", {})
        self.retry_queue = state.get("retry", {})
        # fill pending from input if not present and not processed
        for e in self.edrpou_list:
            if e not in self.processed and e not in self.pending and e not in self.retry_queue:
                self.pending[e] = {"attempts": 0}

    async def worker(self, client: httpx.AsyncClient, sem: asyncio.Semaphore, thread_pool: ThreadPoolExecutor):
        while True:
            # choose next edrpou that is ready to try (pending or retry whose time has come)
            edrpou = None
            now_ts = time.time()
            async with self._lock:
                # first try pending list
                for e, meta in list(self.pending.items()):
                    edrpou = e
                    break
                # if no pending, try retry queue whose next_try_ts <= now
                if edrpou is None:
                    for e, meta in list(self.retry_queue.items()):
                        if meta.get("next_try_ts", 0) <= now_ts:
                            edrpou = e
                            break
                if edrpou is None:
                    # nothing to do
                    return
                # reserve it
                if edrpou in self.pending:
                    meta = self.pending.pop(edrpou)
                else:
                    meta = self.retry_queue.pop(edrpou)

                meta.setdefault("attempts", 0)
                meta["attempts"] += 1
                # store as in-progress in processed with 'in_progress': True (checkpointing)
                self.processed[edrpou] = {"status": "in_progress", "attempts": meta["attempts"], "last_try": now_ts}

            url = BASE_URL_TEMPLATE.format(edrpou=edrpou)
            async with sem:
                success = False
                for attempt in range(1, RETRY_MAX + 1):
                    html = await fetch_with_httpx(client, url, edrpou, attempt)
                    if html is None:
                        # try backoff then retry
                        await asyncio.sleep(backoff_delay(attempt))
                        continue

                    # got html — parse
                    soup = BeautifulSoup(html, "html.parser")
                    # Quick heuristics for "Дані не знайдено" — налаштуй під конкретний текст сторінки
                    page_text = soup.get_text(separator=" ").strip().lower()
                    if "дані не знайдено" in page_text or "інформація відсутня" in page_text or "не знайдено" in page_text:
                        logger.info("[!] Дані не знайдено для %s (attempt %d)", edrpou, meta["attempts"])
                        # schedule retry with exponential delay
                        next_delay = NOT_FOUND_RETRY_DELAY * (2 ** (meta["attempts"] - 1))
                        next_ts = time.time() + next_delay
                        async with self._lock:
                            if meta["attempts"] < RETRY_FOR_NOT_FOUND:
                                self.retry_queue[edrpou] = {"attempts": meta["attempts"], "next_try_ts": next_ts}
                                self.processed[edrpou] = {"status": "scheduled_retry_not_found", "attempts": meta["attempts"], "next_try": next_ts}
                            else:
                                # mark as not_found_final
                                self.processed[edrpou] = {"status": "not_found_final", "attempts": meta["attempts"], "last_try": time.time()}
                        success = True
                        break

                    # If page seems valid, call parse function
                    try:
                        parsed = parse_ubki_full(soup, edrpou)
                        if not parsed or len(parsed.keys()) <= 1:
                            # fallback to cloudscraper sync in threadpool
                            if CLOUDSCRAPER_AVAILABLE:
                                html_cs = await asyncio.get_event_loop().run_in_executor(thread_pool, fetch_with_cloudscraper_sync, url, edrpou)
                                if html_cs:
                                    soup_cs = BeautifulSoup(html_cs, "html.parser")
                                    parsed = parse_ubki_full(soup_cs, edrpou)
                        # save parsed result
                        async with self._lock:
                            self.results.append(parsed)
                            self.processed[edrpou] = {"status": "done", "attempts": meta["attempts"], "last_try": time.time()}
                            self._save_counter += 1
                        logger.info("✅ Parsed %s (attempts=%d)", edrpou, meta["attempts"])
                        success = True
                        break
                    except Exception as e:
                        logger.exception("Parser error for %s on attempt %d: %s", edrpou, attempt, e)
                        await asyncio.sleep(backoff_delay(attempt))

                if not success:
                    # exhausted attempts: schedule retry with backoff or mark failed
                    async with self._lock:
                        if meta["attempts"] < RETRY_FOR_NOT_FOUND + 2:
                            delay = NOT_FOUND_RETRY_DELAY * (2 ** (meta["attempts"] - 1))
                            self.retry_queue[edrpou] = {"attempts": meta["attempts"], "next_try_ts": time.time() + delay}
                            self.processed[edrpou] = {"status": "scheduled_retry_error", "attempts": meta["attempts"], "next_try": time.time() + delay}
                            logger.info("Scheduled retry for %s after failure (attempts=%d)", edrpou, meta["attempts"])
                        else:
                            self.processed[edrpou] = {"status": "failed_final", "attempts": meta["attempts"], "last_try": time.time()}
                            logger.error("Failed final for %s (attempts=%d)", edrpou, meta["attempts"])

            # checkpoint save periodically
            if self._save_counter >= SAVE_EVERY:
                await self.save_progress()

    async def save_progress(self):
        async with self._lock:
            # merge results into dataframe safe format
            df_new = pd.DataFrame(self.results)
            if os.path.exists(OUTPUT_CSV):
                df_old = pd.read_csv(OUTPUT_CSV)
                df = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df = df_new
            df.to_csv(OUTPUT_CSV, index=False)
            # save checkpoint state
            state = {"processed": self.processed, "pending": self.pending, "retry": self.retry_queue}
            save_checkpoint(state)
            logger.info("Checkpoint saved: %s (total saved rows ~%d)", OUTPUT_CSV, len(df))
            # clear in-memory results buffer
            self.results = []
            self._save_counter = 0

    async def run(self):
        sem = asyncio.Semaphore(CONCURRENCY)
        timeout = httpx.Timeout(REQUEST_TIMEOUT)
        limits = httpx.Limits(max_keepalive_connections=CONCURRENCY, max_connections=CONCURRENCY * 2)
        # setup client with optional proxies
        client_args = {"timeout": timeout, "limits": limits}
        async with httpx.AsyncClient(**client_args) as client:
            thread_pool = ThreadPoolExecutor(max_workers=4)
            # start workers
            workers = [asyncio.create_task(self.worker(client, sem, thread_pool)) for _ in range(CONCURRENCY)]
            await asyncio.gather(*workers)
            # final save (for any remaining results)
            await self.save_progress()

# ----------------------
# RUN SCRIPT
# ----------------------
def read_input_csv(path: str) -> List[str]:
    df = pd.read_csv(path, dtype=str)
    if "IDENTIFYCODE" not in df.columns:
        raise ValueError("Input CSV must contain IDENTIFYCODE column")
    codes = df["IDENTIFYCODE"].dropna().astype(str).str.strip().tolist()
    return codes

def main():
    logger.info("Starting UBKI big parser")
    if not os.path.exists(INPUT_CSV):
        logger.error("Input file not found: %s", INPUT_CSV)
        return
    codes = read_input_csv(INPUT_CSV)
    parser = UBKIParser(codes)
    asyncio.run(parser.run())
    logger.info("Parser finished")

if __name__ == "__main__":
    main()


# 16:59:34
# 17:00:48