import asyncio, time, random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from fetcher import fetch_page, realistic_prefetch
from parser_blocks import parse_ubki_full
from utils import logger, backoff_delay, load_checkpoint, save_checkpoint
import httpx
import os

BASE_URL_TEMPLATE = "https://edrpou.ubki.ua/ua/{edrpou}"
SAVE_EVERY = 50

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
NOT_FOUND_RETRY_DELAY = 5          # секунда початкова затримка перед повторним парсингом (буде зростати)
ERROR_WINDOW_SECONDS = 120       # вікно для recent_errors
ERROR_THRESHOLD = 8              # якщо більше помилок за вікно -> включити cooldown
COOLDOWN_SECONDS = 60            # початковий cooldown при перевищенні порогу
REALISTIC_PREFETCH_PROB = 0.12   # ймовірність робити "реалістичний трафік" перед запитом
MOBILE_UA_PROB = 0.25    


PROXIES = []  

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:116.0) Gecko/20100101 Firefox/116.0",
]

BASE_URL_TEMPLATE = "https://edrpou.ubki.ua/ua/{edrpou}"

class UBKIParser:
    def __init__(self, edrpou_list):
        self.edrpou_list = edrpou_list
        self.results = []
        self.processed, self.pending, self.retry_queue = {}, {}, {}
        self._lock = asyncio.Lock()
        self._save_counter = 0
        self._load_state()
        self.session_cookies = {}

    def _load_state(self):
        state = load_checkpoint()
        self.processed = state.get("processed", {})
        for e in self.edrpou_list:
            if e not in self.processed:
                self.pending[e] = {"attempts": 0}

    async def worker(self, client, sem):
        counter = 0
        while True:
            async with self._lock:
                if not self.pending:
                    return
                edrpou, meta = self.pending.popitem()
                meta["attempts"] += 1
                self.processed[edrpou] = {"status": "in_progress", "attempts": meta["attempts"]}

            counter += 1
            if counter % 200 == 0:
                logger.info("Resting 20 sec after 200 companies")
                await asyncio.sleep(random.randint(10, 30))

            await realistic_prefetch(client, REALISTIC_PREFETCH_PROB)

            async with sem:
                url = BASE_URL_TEMPLATE.format(edrpou=edrpou)
                html = await fetch_page(client, url, self.session_cookies)
                if not html:
                    # повернемо у чергу з затримкою
                    if meta["attempts"] < RETRY_MAX:
                        await asyncio.sleep(NOT_FOUND_RETRY_DELAY * meta["attempts"])
                        async with self._lock:
                            self.pending[edrpou] = meta
                    else:
                        logger.warning(f"[!] Порожня відповідь для {edrpou} після {meta['attempts']} спроб")
                    continue

                soup = BeautifulSoup(html, "html.parser")
                parsed = parse_ubki_full(soup, edrpou)

                # Якщо базових даних немає — це неуспішний парсинг, ретраїмо
                if not parsed or not parsed.get("Повна назва"):
                    if meta["attempts"] < RETRY_FOR_NOT_FOUND:
                        delay = NOT_FOUND_RETRY_DELAY * meta["attempts"]
                        logger.info(f"[!] Дані не знайдено для {edrpou} — ретрай через {delay}s")
                        await asyncio.sleep(delay)
                        async with self._lock:
                            self.pending[edrpou] = meta
                    else:
                        logger.warning(f"[X] Дані не знайдено для {edrpou} після {meta['attempts']} спроб")
                    continue

                async with self._lock:
                    self.results.append(parsed)
                    self.processed[edrpou] = {"status": "done", "attempts": meta["attempts"]}
                    self._save_counter += 1

                if self._save_counter >= SAVE_EVERY:
                    await self.save_progress()

    async def save_progress(self):
        async with self._lock:
            if not self.results:
                return
            df_new = pd.DataFrame(self.results)
            first_write = not os.path.exists(OUTPUT_CSV)
            df_new.to_csv(OUTPUT_CSV, mode="a", index=False, header=first_write)
            save_checkpoint({"processed": self.processed})
            self.results.clear()
            self._save_counter = 0
            logger.info("Checkpoint saved")

    async def run(self):
        sem = asyncio.Semaphore(CONCURRENCY)
        async with httpx.AsyncClient() as client:
            tasks = [asyncio.create_task(self.worker(client, sem)) for _ in range(CONCURRENCY)]
            await asyncio.gather(*tasks)
            await self.save_progress()