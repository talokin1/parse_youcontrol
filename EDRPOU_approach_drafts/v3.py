import asyncio
import aiohttp
import logging
import random
import re
import time
import sys
import os
import pickle
import pandas as pd
from bs4 import BeautifulSoup

# ----------------------------------------------------------
# Logging setup (no emojis, safe for Windows console)
# ----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("youcontrol_parser_fast.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------
# HTML Parser (your structure preserved)
# ----------------------------------------------------------
def parse_company_youcontrol_page(html: str) -> dict:
    """Парсить сторінку компанії у структурований словник."""
    soup = BeautifulSoup(html, "lxml")
    result = {}

    def clean_text(text):
        return re.sub(r"\s+", " ", text.strip())

    # --- основна таблиця ---
    for table in soup.select("table.detail-view"):
        for tr in table.select("tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            key = clean_text(th.get_text(" ", strip=True))
            val = clean_text(td.get_text(" ", strip=True))
            result[key] = val

    # --- EDRPOU ---
    edrpou = soup.find("h2", class_="seo-table-name case-icon short")
    if edrpou:
        match = re.search(r"\d+", edrpou.get_text())
        if match:
            result["EDRPOU_CODE"] = match.group()

    # --- Назва ---
    name = soup.find("h1") or soup.find("h2", class_="seo-table-name")
    if name:
        result["Company Name"] = clean_text(name.get_text())

    # --- Види діяльності ---
    activities_block = soup.select_one("div.localized-item.flex-activity")
    if activities_block:
        text = clean_text(activities_block.get_text(" ", strip=True))
        result["КВЕД (Основний)"] = text
    for idx, li in enumerate(soup.select("li.localized-item.localized-other")[:3], start=1):
        result[f"КВЕД_{idx} (Інші)"] = clean_text(li.get_text(" ", strip=True))

    # --- Бенефіціари ---
    beneficiary_block = soup.select("div#catalog-company-beneficiary div.seo-table-row")
    for idx, row in enumerate(beneficiary_block[:5], start=1):
        result[f"Beneficiary_{idx}"] = clean_text(row.get_text(" ", strip=True))

    return result

# ----------------------------------------------------------
# Utility: Load proxies
# ----------------------------------------------------------
def load_proxies(path: str):
    """Повертає список проксі у форматі http://host:port або http://user:pass@host:port"""
    proxies = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw or raw.startswith("#"):
                    continue
                if "://" in raw:
                    proxies.append(raw)
                elif "@" in raw:
                    proxies.append(f"http://{raw}")
                else:
                    proxies.append(f"http://{raw}")
    except Exception as e:
        logger.warning(f"Не вдалося завантажити проксі: {e}")
    return proxies

# ----------------------------------------------------------
# Fetch company page (async)
# ----------------------------------------------------------
async def fetch_company(session, edrpou, proxies=None, max_retries=5):
    url = f"https://youcontrol.com.ua/catalog/company_details/{edrpou}/"

    def pick_proxy():
        if not proxies:
            return None
        return random.choice(proxies)

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        proxy = pick_proxy()
        try:
            async with session.get(url, proxy=proxy) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    data = parse_company_youcontrol_page(html)
                    data["EDRPOU_INPUT"] = edrpou
                    logger.info(f"Parsed {edrpou}")
                    return data

                if resp.status in (429, 503, 403):
                    logger.warning(f"Status {resp.status} for {edrpou}, retry {attempt} after {backoff:.1f}s")
                    await asyncio.sleep(backoff + random.uniform(0.3, 0.6))
                    backoff = min(backoff * 2, 20)
                    continue

                text = await resp.text()
                raise Exception(f"HTTP {resp.status}: {text[:200]!r}")

        except asyncio.TimeoutError:
            logger.warning(f"Timeout {edrpou}, retry {attempt} after {backoff:.1f}s")
            await asyncio.sleep(backoff + random.uniform(0.3, 0.6))
            backoff = min(backoff * 2, 20)
        except Exception as e:
            logger.warning(f"Error {edrpou} ({e}), retry {attempt} after {backoff:.1f}s")
            await asyncio.sleep(backoff + random.uniform(0.3, 0.6))
            backoff = min(backoff * 2, 20)

    logger.error(f"Error {edrpou}: failed after {max_retries} attempts")
    return {"EDRPOU_INPUT": edrpou, "Error": f"Failed after {max_retries} attempts"}

# ----------------------------------------------------------
# Async parser runner
# ----------------------------------------------------------
async def run_fast_parser(edrpou_list, concurrency=4, proxy_file=None):
    cookies = pickle.load(open("cookies.pkl", "rb"))
    cookie_dict = {c["name"]: c["value"] for c in cookies}

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://youcontrol.com.ua/",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="141", "Chromium";v="141", "Not?A_Brand";v="99"',
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua-mobile": "?0",
    }

    proxies = load_proxies(proxy_file) if proxy_file else None
    connector = aiohttp.TCPConnector(limit_per_host=concurrency, ssl=False)
    timeout = aiohttp.ClientTimeout(total=45)

    async with aiohttp.ClientSession(headers=headers, cookies=cookie_dict, connector=connector, timeout=timeout) as session:
        sem = asyncio.Semaphore(concurrency)
        tasks = []

        for code in edrpou_list:
            async def bounded_fetch(c=code):
                async with sem:
                    await asyncio.sleep(random.uniform(0.4, 1.2))
                    return await fetch_company(session, c, proxies=proxies)
            tasks.append(asyncio.create_task(bounded_fetch()))

        return await asyncio.gather(*tasks)

# ----------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------
def run_parser_fast(csv_path, proxy_file=None):
    df = pd.read_csv(csv_path, dtype=str)
    edrpou_list = df.iloc[:, 0].dropna().tolist()
    start = time.time()
    results = asyncio.run(run_fast_parser(edrpou_list, concurrency=4, proxy_file=proxy_file))
    df_out = pd.DataFrame(results)
    df_out.to_csv("youcontrol_fast.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved {len(df_out)} records → youcontrol_fast.csv")
    logger.info(f"Total time: {time.time() - start:.2f}s")


if __name__ == "__main__":
    run_parser_fast(
        csv_path=r"C:\OTP Draft\YouControl\test_dataset.csv")
