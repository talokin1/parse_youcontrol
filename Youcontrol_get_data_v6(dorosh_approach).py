# === 1) Imports & Config ===
import os, sys, re, json, time, random, logging, asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import bs4
import pandas as pd
import cloudscraper

# ---------- CONFIG ----------
URL_BASE = "https://youcontrol.com.ua/catalog/kved/"
OUT_DIR = "out"
CHECKPOINT_FILE = "checkpoint.json"
ERROR_LOG = "youcontrol_errors.log"
INFO_LOG  = "youcontrol_info.log"

# Обмеження
LIST_CONCURRENCY = 6           # скільки сторінок класів качати паралельно
DETAILS_WORKERS  = 12          # скільки воркерів парсити деталі (через ThreadPool)
QUEUE_MAXSIZE    = 200         # скільки компаній максимум у черзі

# Антибан
SLEEP_LIST_MIN,   SLEEP_LIST_MAX   = 0.8, 2.0
SLEEP_DETAIL_MIN, SLEEP_DETAIL_MAX = 0.3, 1.0

MAX_RETRIES = 6
TIMEOUT_SEC = 30

# Фільтри
TARGET_CLASS_CODE = 0.11  # "01.11" щоб парсити лише один клас; або None — всі
TARGET_PAGES      = [1, 2, 3]  # наприклад [1,2,3] або None — всі

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36",

    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) "
    "Gecko/20100101 Firefox/123.0",

    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": URL_BASE,
        "Upgrade-Insecure-Requests": "1",
    }

os.makedirs(OUT_DIR, exist_ok=True)

# ---------- LOGGING ----------
logger = logging.getLogger("youcontrol")
logger.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh_info = logging.FileHandler(INFO_LOG, encoding="utf-8")
fh_info.setFormatter(fmt)
fh_info.setLevel(logging.INFO)
fh_err = logging.FileHandler(ERROR_LOG, encoding="utf-8")
fh_err.setFormatter(fmt)
fh_err.setLevel(logging.WARNING)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
sh.setLevel(logging.INFO)

logger.handlers.clear()
logger.addHandler(fh_info)
logger.addHandler(fh_err)
logger.addHandler(sh)

# ---------- Async infra ----------
executor = ThreadPoolExecutor(max_workers=DETAILS_WORKERS)
company_queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
result_queue:  asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

# Один глобальний cloudscraper (як у v5)
scraper = cloudscraper.create_scraper(delay=10, browser={
    'browser': 'chrome', 'platform': 'windows', 'mobile': False
})

# === 2) Checkpoint & HTTP helpers ===
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_checkpoint(class_code=None, page=None):
    data = load_checkpoint()
    if class_code is not None and page is not None:
        data["class_code"] = class_code
        data["page"] = page
        data["ts"] = datetime.utcnow().isoformat()
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def http_get_soup(url: str):
    """
    Cloudflare-safe GET → BeautifulSoup or None
    (блочна функція, викликається через executor)
    """
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = scraper.get(url, headers=get_headers(), timeout=TIMEOUT_SEC)
            code = resp.status_code
            if code == 200:
                return bs4.BeautifulSoup(resp.text, "lxml")
            elif code in (429, 500, 502, 503, 504):
                delay = min(2**attempt + random.uniform(0.5, 2.0), 60)
                logger.warning(f"[{code}] retry in {delay:.1f}s → {url}")
                time.sleep(delay)
                attempt += 1
            else:
                logger.error(f"Unexpected HTTP {code} for {url}, sleep 60s and retry")
                time.sleep(60)
                attempt += 1
        except Exception as e:
            time.sleep(min(2**attempt, 30))
            attempt += 1
    logger.error(f"HTTP failed after retries: {url}")
    return None

async def fetch_html(url: str):
    """Async обгортка над http_get_soup через ThreadPoolExecutor."""
    loop = asyncio.get_running_loop()
    await asyncio.sleep(random.uniform(SLEEP_LIST_MIN, SLEEP_LIST_MAX))
    return await loop.run_in_executor(executor, http_get_soup, url)


# === 2) Checkpoint & HTTP helpers ===
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_checkpoint(class_code=None, page=None):
    data = load_checkpoint()
    if class_code is not None and page is not None:
        data["class_code"] = class_code
        data["page"] = page
        data["ts"] = datetime.utcnow().isoformat()
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def http_get_soup(url: str):
    """
    Cloudflare-safe GET → BeautifulSoup or None
    (блочна функція, викликається через executor)
    """
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = scraper.get(url, headers=get_headers(), timeout=TIMEOUT_SEC)
            code = resp.status_code
            if code == 200:
                return bs4.BeautifulSoup(resp.text, "lxml")
            elif code in (429, 500, 502, 503, 504):
                delay = min(2**attempt + random.uniform(0.5, 2.0), 60)
                logger.warning(f"[{code}] retry in {delay:.1f}s → {url}")
                time.sleep(delay)
                attempt += 1
            else:
                logger.error(f"Unexpected HTTP {code} for {url}, sleep 60s and retry")
                time.sleep(60)
                attempt += 1
        except Exception as e:
            time.sleep(min(2**attempt, 30))
            attempt += 1
    logger.error(f"HTTP failed after retries: {url}")
    return None

async def fetch_html(url: str):
    """Async обгортка над http_get_soup через ThreadPoolExecutor."""
    loop = asyncio.get_running_loop()
    await asyncio.sleep(random.uniform(SLEEP_LIST_MIN, SLEEP_LIST_MAX))
    return await loop.run_in_executor(executor, http_get_soup, url)

# === 3) HTML parsers (profile + beneficiary) ===
def parse_profile_block(soup: bs4.BeautifulSoup) -> dict:
    block = soup.find("div", class_="seo-table-contain", id="catalog-company-file")
    if not block:
        return {}
    rows = block.find_all("div", class_="seo-table-row")
    cols = [clean_text(r.find("div", class_="seo-table-col-1").get_text()) for r in rows]
    vals_raw = []
    for r in rows:
        node = (r.find("span", class_="copy-file-field") or
                r.find("div", class_="copy-file-field") or
                r.find("p", class_="ucfirst copy-file-field") or
                r.find("div", class_="seo-table-col-2"))
        if node and clean_text(node.get_text()):
            vals_raw.append(clean_text(node.get_text()))
    return dict(zip(cols, vals_raw))

def parse_beneficiary_block(soup: bs4.BeautifulSoup) -> dict:
    block = soup.find("div", class_="seo-table-contain", id="catalog-company-beneficiary")
    if not block:
        return {}
    rows = block.find_all("div", class_="seo-table-row")
    cols = [clean_text(r.find("div", class_="seo-table-col-1").get_text()) for r in rows]
    # EDRPOU з заголовка
    h2 = soup.find("h2", class_="seo-table-name case-icon short")
    edrpou = None
    if h2:
        m = re.search(r'\d+', h2.get_text())
        edrpou = m.group(0) if m else None
    if edrpou:
        cols.append("EDRPOU_CODE")

    spans = [r.find("span", class_="copy-file-field") for r in rows]
    spans = [x for x in spans if x]
    persons = []
    for r in rows:
        c2 = r.find("div", class_="seo-table-col-2")
        if c2 and 'copy-hover' not in (c2.get("class") or []):
            persons.append(c2)
    data_nodes = spans + persons
    vals = [clean_text(n.get_text()) for n in data_nodes]
    if edrpou:
        vals.append(edrpou)
    return dict(zip(cols, vals))


# === 4) PRODUCER: put company detail jobs into queue ===
async def producer():
    logger.info("Starting KVED crawl (producer)")
    cp = load_checkpoint()
    resume_class = cp.get("class_code")
    resume_page  = cp.get("page")
    skip_until_class = bool(resume_class)
    skip_until_page  = bool(resume_class and resume_page)

    sections = await fetch_html(URL_BASE)
    if not sections:
        logger.error("Cannot fetch main KVED catalog page.")
        return

    section_blocks = sections.find_all("div", class_="kved-catalog-table")
    logger.info(f"Found {len(section_blocks)} section blocks")

    for section_block in section_blocks:
        section_code = clean_text(section_block.find("td", class_="green-col-word").get_text())
        section_name = clean_text(section_block.find("td", class_="caps-col").get_text())

        for chapter_td in section_block.find_all("td", class_="green-col-num"):
            chapter_code = clean_text(chapter_td.get_text())
            chapter_name = clean_text(chapter_td.find_next_sibling("td").get_text())

            url_chapter = URL_BASE + chapter_code
            html_chapter = await fetch_html(url_chapter)
            if not html_chapter:
                continue

            table = html_chapter.find("table")
            if not table:
                continue

            current_group_code, current_group_name = None, None
            for row in table.find_all("tr"):
                group_code_td = row.find("td", class_="green-col-word")
                if group_code_td:
                    current_group_code = clean_text(group_code_td.get_text())
                    current_group_name = clean_text(row.find("td", class_="caps-col").get_text())
                    continue

                class_code_td = row.find("td", class_="green-col-num")
                if not (class_code_td and current_group_code):
                    continue

                class_code = clean_text(class_code_td.get_text())
                class_name = clean_text(class_code_td.find_next_sibling("td").get_text())

                # Фільтр на один клас (якщо задано)
                if TARGET_CLASS_CODE and class_code != TARGET_CLASS_CODE:
                    continue

                # Резюм після аварії: пропускаємо до потрібного класу
                if skip_until_class:
                    if class_code == resume_class:
                        skip_until_class = False
                    else:
                        continue

                url_class = url_chapter + f'/{class_code[-2:]}'
                html_class = await fetch_html(url_class)
                if not html_class:
                    continue

                # Пагінація
                max_page = 1
                pag = html_class.find("ul", class_="pagination")
                if pag:
                    for li in pag.find_all("li"):
                        a = li.find("a")
                        if a and a.get_text().strip().isdigit():
                            max_page = max(max_page, int(a.get_text().strip()))

                pages_iter = TARGET_PAGES if TARGET_PAGES else range(1, max_page+1)
                logger.info(f"[{class_code}] pages: {max_page}")

                for page in pages_iter:
                    # Резюм: пропустити до потрібної сторінки
                    if skip_until_page:
                        if page == resume_page:
                            skip_until_page = False
                        else:
                            continue

                    url_page = f"{url_class}?page={page}"
                    html_page = await fetch_html(url_page)
                    if not html_page:
                        continue

                    items = html_page.find_all("a", class_="link-details link-open")
                    if not items:
                        logger.info(f"[{class_code}] page {page}: no items")
                        # все одно оновимо чекпоінт
                        await result_queue.put({"type": "PAGE_DONE", "class_code": class_code, "page": page})
                        continue

                    for a in items:
                        edrpou = clean_text((a.get_text() or "").split(",")[0])
                        url_details = f"https://youcontrol.com.ua{a.get('href')}"
                        job = {
                            "type": "DETAIL",
                            "url": url_details,
                            "company_code": edrpou,
                            "SECTION_CODE": section_code,
                            "SECTION_NAME": section_name,
                            "CHAPTER_CODE": chapter_code,
                            "CHAPTER_NAME": chapter_name,
                            "GROUP_CODE": current_group_code,
                            "GROUP_NAME": current_group_name,
                            "CLASS_CODE": class_code,
                            "CLASS_NAME": class_name,
                            "page": page
                        }
                        await company_queue.put(job)

                    # сигнал райтеру: цю сторінку поставили в роботу
                    await result_queue.put({"type": "PAGE_DONE", "class_code": class_code, "page": page})

    # сигнал кінця продюсера
    for _ in range(DETAILS_WORKERS):
        await company_queue.put({"type": "STOP"})
    await result_queue.put({"type": "CRAWL_DONE"})


# === 5) CONSUMERS & WRITER ===
def _fetch_detail_blocking(url: str):
    """Блокуюча функція: HTML деталей компанії → soup або None."""
    return http_get_soup(url)

async def detail_worker(worker_id: int):
    """Async-воркер деталей (OLX-стиль: окремий етап на деталі)."""
    loop = asyncio.get_running_loop()
    while True:
        job = await company_queue.get()
        if job.get("type") == "STOP":
            company_queue.task_done()
            break

        if job.get("type") != "DETAIL":
            company_queue.task_done()
            continue

        try:
            await asyncio.sleep(random.uniform(SLEEP_DETAIL_MIN, SLEEP_DETAIL_MAX))
            soup = await loop.run_in_executor(executor, _fetch_detail_blocking, job["url"])
            if not soup:
                company_queue.task_done()
                continue

            profile = parse_profile_block(soup)
            benef   = parse_beneficiary_block(soup)

            row = {
                "SECTION_CODE": job["SECTION_CODE"],
                "SECTION_NAME": job["SECTION_NAME"],
                "CHAPTER_CODE": job["CHAPTER_CODE"],
                "CHAPTER_NAME": job["CHAPTER_NAME"],
                "GROUP_CODE": job["GROUP_CODE"],
                "GROUP_NAME": job["GROUP_NAME"],
                "CLASS_CODE": job["CLASS_CODE"],
                "CLASS_NAME": job["CLASS_NAME"],
                "EDRPOU_CODE": job["company_code"],
                "_page": job["page"],
                "_fetched_at": datetime.utcnow().isoformat(),
                "_detail_url": job["url"]
            }
            row.update(profile)
            # benef може містити EDRPOU_CODE з заголовка — не перезаписуємо наш
            for k, v in benef.items():
                if k == "EDRPOU_CODE":
                    continue
                row[k] = v

            await result_queue.put({"type": "ROW", "class_code": job["CLASS_CODE"], "row": row})
        except Exception as e:
            logger.warning(f"[worker {worker_id}] error: {e}")
        finally:
            company_queue.task_done()

async def writer_task():
    """
    Приймає:
      - ROW → пише у out/kved_{CLASS}.jsonl
      - PAGE_DONE → оновлює checkpoint
      - CRAWL_DONE → завершується після спорожнення черг
    """
    open_files = {}  # class_code -> file handle
    pending_pages = {}  # class_code -> найбільша завершена сторінка (для грубого чекпоінта)

    try:
        while True:
            msg = await result_queue.get()
            typ = msg.get("type")

            if typ == "ROW":
                cls = msg["class_code"]
                row = msg["row"]
                fp = open_files.get(cls)
                if fp is None:
                    path = os.path.join(OUT_DIR, f"kved_{cls}.jsonl")
                    fp = open(path, "a", encoding="utf-8")
                    open_files[cls] = fp
                fp.write(json.dumps(row, ensure_ascii=False) + "\n")

            elif typ == "PAGE_DONE":
                cls = msg["class_code"]
                page = msg["page"]
                prev = pending_pages.get(cls, 0)
                if page > prev:
                    pending_pages[cls] = page
                    # цей чекпоінт є «досягнули сторінку page» у класі cls
                    save_checkpoint(class_code=cls, page=page)
                    logger.info(f"[checkpoint] {cls}|{page}")

            elif typ == "CRAWL_DONE":
                # дочекаємося повного спорожнення черг
                await company_queue.join()
                # після цього більше ROW не прийде
                break

            result_queue.task_done()
    finally:
        for fp in open_files.values():
            try:
                fp.close()
            except Exception:
                pass


# === 6) MAIN ===
async def main():
    logger.info("YouControl v6 hybrid started")
    # tasks
    writer = asyncio.create_task(writer_task())
    consumers = [asyncio.create_task(detail_worker(i+1)) for i in range(DETAILS_WORKERS)]
    prod = asyncio.create_task(producer())

    # порядок завершення:
    await prod
    await company_queue.join()
    await result_queue.join()

    # зупинка воркерів (вже надіслані STOP у producer)
    for c in consumers:
        await c
    await writer
    logger.info("Done.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
