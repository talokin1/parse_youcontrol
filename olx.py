import pandas as pd
from bs4 import BeautifulSoup
import nest_asyncio
import requests
import asyncio
import aiohttp
import async_timeout

# Allow nested event loops
nest_asyncio.apply()

# Semaphore for concurrent requests
tasks_semaphore = asyncio.Semaphore(100)

directory = "/Users/serhiidoroshenko/Downloads/OLX_DATA/"

# HTTP Headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
}

# Price ranges
groups = [
    [(p, p + 499) for p in range(0, 100500, 500)],
    [(p, p + 4999) for p in range(100500, 205500, 5000)],
    [(p, p + 9999) for p in range(205500, 1010500, 10000)],
    [(1010500, 40000000)]
]

price_ranges = [rng for group in groups for rng in group]

# Categories: directory name -> URL slug
CATEGORIES = {
    'houses': 'doma/prodazha-domov',
    'commercials': 'kommercheskaya-nedvizhimost/prodazha-kommercheskoy-nedvizhimosti',
    'garages': 'garazhy-parkovki/prodazha-garazhey-parkovok',
    'flats': 'kvartiry/prodazha-kvartir',
    'lands': 'zemlya/prodazha-zemli'
}

async def fetch_ids(session, url):
    """Fetch all item IDs on a given page URL."""
    async with tasks_semaphore:
        try:
            async with async_timeout.timeout(15):
                async with session.get(url, headers=HEADERS, ssl=False) as response:
                    text = await response.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    cards = soup.find_all('div', attrs={'data-testid': 'l-card'})
                    return [card.get('id') for card in cards if card.get('id')]
        except Exception as e:
            print(f"X Ошибка запроса {url}: {e}")
            return []

async def get_page_count(session, url):
    """Determine how many pages are available for given base URL."""
    try:
        async with session.get(url, headers=HEADERS, ssl=False) as response:
            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
            cards = soup.find_all('div', attrs={'data-testid': 'l-card'})
            if not cards:
                print(f"▲ Нет объявлений по ссылке: {url}")
                return 0

            pagination = soup.find('ul', attrs={'data-testid': 'pagination-list'})
            if pagination:
                pages = pagination.find_all('li')
                if pages:
                    last = pages[-1].find('a')
                    if last and last.text.strip().isdigit():
                        return int(last.text.strip())
    except Exception as e:
        print(f"X Ошибка получения количества страниц {url}: {e}")
        return 0

def build_url(slug, price_from, price_to, page=None):
    """Construct URL for a given category, price range, and optional page."""
    base = (
        f'https://www.olx.ua/uk/nedvizhimost/{slug}/?currency=USD'
        f'&search%5Bfilter_float_price:from%5D={price_from}'
        f'&search%5Bfilter_float_price:to%5D={price_to}'
    )
    if page:
        base += f'&page={page}'
    return base

async def scrape_category(session, category, slug):
    """Scrape all item IDs for one category."""
    item_ids = set()
    tasks = []
    for price_from, price_to in price_ranges:
        base_url = build_url(slug, price_from, price_to)
        page_count = await get_page_count(session, base_url)
        print(f"□ [{category}] {base_url} - страниц: {page_count}")
        for p in range(1, page_count + 1):
            url = build_url(slug, price_from, price_to, p)
            tasks.append(fetch_ids(session, url))

    results = await asyncio.gather(*tasks)
    for res in results:
        item_ids.update(res)
    return item_ids

async def main():
    async with aiohttp.ClientSession() as session:
        for category, slug in CATEGORIES.items():
            ids = await scrape_category(session, category, slug)
            # Ensure directory exists
            os.makedirs(os.path.join(directory + category), exist_ok=True)
            filepath = os.path.join(directory + category, f"items_ids_{category}.csv")
            # Save to CSV with header
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['ids'])
                for _id in ids:
                    writer.writerow([_id])
            print(f"▽ [{category}] Уникальных объявлений собрано: {len(ids)}. Файл сохранен: {filepath}")

if __name__ == '__main__':
    asyncio.run(main())

import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm

# ========= Константы =========
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

# Локи для потоков
file_lock = threading.Lock()
count_lock = threading.Lock()

# ========= Функции =========
def get_random_user_agent():
    return random.choice(USER_AGENTS)

def save_data_to_file(file_name, new_entry):
    with open(file_name, 'a', encoding='utf-8') as file:
        json.dump(new_entry, file, ensure_ascii=False)
        file.write('\n')

def get_data_by_id(ad_id, retries=3, delay=2):
    base_url = (
        '/www.olx.ua/api/v1/targeting/data/'
        '?page=ad&params%5Bad_id%5D={}&dfp_user_id=0'
    ).format(ad_id)
    headers = {**HEADERS, "User-Agent": get_random_user_agent()}

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(base_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                time.sleep(delay)
                continue

            data = resp.json()
            ad_url = data['data']['targeting'].get('ad_url')
            if not ad_url:
                return None

            page = requests.get(ad_url, headers=headers, timeout=10)
            if page.status_code != 200:
                time.sleep(delay)
                continue

            soup = BeautifulSoup(page.text, 'html.parser')
            date_span = soup.find('span', {'data-cy': 'ad-posted-at'})
            date_text = date_span.text.strip() if date_span else ""
            desc_div = soup.find('div', {'data-testid': 'ad_description'}) 
            description = desc_div.text.strip() if desc_div and desc_div.find('div') else ""
            params = soup.find('div', {'data-testid': 'ad-parameters-container'})
            attrs = [p.text.strip() for p in params.find_all('p')] if params else []

            data['data']['targeting']['date'] = date_text
            data['data']['targeting']['description'] = description
            data['data']['targeting']['attributes'] = attrs
            return data
        except Exception:
            time.sleep(delay)

    return None

def process_category(category, max_workers=8, limit=None):
    """Парсинг по категории: чтение CSV, многопоточность, сохранение в JSON"""
    csv_path = f"{directory}{category}/items_ids_{category}.csv"
    ids = pd.read_csv(csv_path)['ids'].tolist()
    if limit:
        ids = ids[:limit]

    json_path = os.path.join(directory + category + '/', f'olx_{category}.json')
    # очистка файла
    open(json_path, 'w').close()

    saved_count = 0
    pbar = tqdm(total=len(ids), desc=f"[{category}]", unit="it")

    def worker(ad_id):
        nonlocal saved_count
        result = get_data_by_id(ad_id)
        if result:
            save_data_to_file(json_path, result)
            with count_lock:
                saved_count += 1
                pbar.update(1)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, ad) for ad in ids]
        for _ in as_completed(futures):
            pass
    pbar.close()

if __name__ == '__main__':
    # Для всех категорий
    categories = ['houses', 'commercials', 'garages', 'flats', 'lands']
    for cat in categories:
        print(f"Запуск парсинга категории: {cat}")
        process_category(cat, max_workers=8, limit=200)