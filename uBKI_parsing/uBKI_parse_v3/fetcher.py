import asyncio, random, time
import httpx
from bs4 import BeautifulSoup
from typing import Optional
try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
except Exception:
    CLOUDSCRAPER_AVAILABLE = False

REALISTIC_PREFETCH_PROB = 0.12   # ймовірність робити "реалістичний трафік" перед запитом
MOBILE_UA_PROB = 0.25   
MOBILE_USER_AGENTS = [
    "Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
]

DESKTOP_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
] 

pre_urls = [
    f"https://edrpou.ubki.ua/ua/search?q={random.randint(1000,999999)}"
]



def get_scraper_with_cookies(existing_cookies: dict = None):
    """
    Повертає cloudscraper scraper з існуючими cookies (якщо є).
    Викликається у threadpool (синхронна).
    """
    if not CLOUDSCRAPER_AVAILABLE:
        return None
    scraper = cloudscraper.create_scraper()
    if existing_cookies:
        try:
            scraper.cookies.update(existing_cookies)
        except Exception:
            pass
    return scraper


async def human_delay(base: float = 0.6, var: float = 1.5):
    """
    Імітація людської затримки перед читанням сторінки.
    Викликати перед основним fetch-ом.
    """
    await asyncio.sleep(random.uniform(base, base + var))


def rotate_browser_fingerprint():
    """
    Повертає набір заголовків, які імітують різні браузери/платформи.
    Викликати для кожного запиту.
    """
    is_mobile = random.random() < MOBILE_UA_PROB
    ua = random.choice(MOBILE_USER_AGENTS) if is_mobile else random.choice(DESKTOP_USER_AGENTS)

    accept = random.choice([
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "text/html;q=0.9,*/*;q=0.8"
    ])
    accept_lang = random.choice(["uk-UA,uk;q=0.9,en;q=0.8", "ru-RU,ru;q=0.9,en;q=0.8", "en-US,en;q=0.9"])
    referer = random.choice([
        "https://www.google.com/",
        "https://www.bing.com/",
        "https://edrpou.ubki.ua/ua/"
    ]) + ("search?q=" + str(random.randint(1000, 999999)) if random.random() < 0.4 else "")

    headers = {
        "User-Agent": ua,
        "Accept": accept,
        "Accept-Language": accept_lang,
        "Referer": referer,
        "Cache-Control": random.choice(["max-age=0", "no-cache"]),
        "DNT": random.choice(["1", "0"]),
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

    if random.random() < 0.5:
        headers["Sec-CH-UA"] = '"Chromium";v="116", "Not)A;Brand";v="24"'
        headers["Sec-CH-UA-Mobile"] = "?0" if not is_mobile else "?1"

    return headers

async def realistic_prefetch(client: "httpx.AsyncClient", realistic_prefer_prob):
    """
    Іноді викликається перед основним fetch, щоб створити "шум" трафіку.
    Використовувати з імовірністю REALISTIC_PREFETCH_PROB.
    """
    if random.random() > realistic_prefer_prob:
        return
    pre_urls = [
        "https://edrpou.ubki.ua/ua/",
        f"https://edrpou.ubki.ua/ua/search?q={random.randint(1000,999999)}"
    ]
    picks = random.sample(pre_urls, k=1)
    for pre in picks:
        try:
            headers = rotate_browser_fingerprint()
            await client.get(pre, headers=headers, timeout=10)
            await asyncio.sleep(random.uniform(0.3, 1.3)) 
        except Exception:
            pass


# --- посильний ретрай у fetch_page ---
async def fetch_page(client, url, cookies, attempt=1, max_attempts=3):
    for att in range(1, max_attempts + 1):
        await human_delay()
        headers = rotate_browser_fingerprint()
        try:
            resp = await client.get(url, headers=headers, timeout=20)
            status = resp.status_code
            html = resp.text or ""

            # Успіх
            if status == 200 and html.strip():
                return html

            # Тимчасові помилки / бан — підретраїмо
            if status in (403, 429) or status >= 500:
                await asyncio.sleep(random.uniform(1.5, 3.5) * att)
                continue

            # 404/інше — сенсу ретраїти мало
            if status == 404:
                return None

        except Exception:
            # один раз пробуємо cloudscraper як фолбек
            if CLOUDSCRAPER_AVAILABLE:
                scraper = await asyncio.get_event_loop().run_in_executor(None, get_scraper_with_cookies, cookies)
                if scraper:
                    try:
                        html = await asyncio.get_event_loop().run_in_executor(None, lambda: scraper.get(url, timeout=20).text)
                        cookies.update(scraper.cookies.get_dict())
                        if html and html.strip():
                            return html
                    except Exception:
                        pass
            await asyncio.sleep(random.uniform(1.0, 2.5) * att)
    return None
