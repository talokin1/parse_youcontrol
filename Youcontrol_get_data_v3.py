import bs4
import pandas as pd
import re
import time
import logging
import sys
from uuid import uuid4
import random
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException

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

_driver = None
SELENIUM_PAGE_TIMEOUT = 45


def get_driver():
    """Initialize or reuse a headless Chrome WebDriver instance."""
    global _driver
    if _driver is None:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=uk-UA")
        selected_user_agent = random.choice(USER_AGENTS)
        options.add_argument(f"--user-agent={selected_user_agent}")

        try:
            _driver = webdriver.Chrome(options=options)
        except WebDriverException as exc:
            logger.error(f"Failed to initialize Selenium WebDriver: {exc}")
            raise

        _driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT)
        logger.debug("Initialized Selenium WebDriver with user agent %s", selected_user_agent)

    return _driver


def wait_for_dom_ready(driver, timeout=30):
    """Block until document.readyState is 'complete', then return."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException as exc:
        logger.warning(f"Timed out waiting for DOM ready state: {exc}")
        raise


def shutdown_driver():
    """Dispose Selenium WebDriver if it was started."""
    global _driver
    if _driver is not None:
        logger.debug("Shutting down Selenium WebDriver")
        _driver.quit()
        _driver = None


def smart_sleep(base_min=3, base_max=7, long_pause_chance=0.01):
    delay = random.uniform(base_min, base_max)
    if random.random() < long_pause_chance:
        delay += random.uniform(20, 30)
        logger.debug("Long pause triggered...")
    logger.debug(f"Sleeping {delay:.2f} seconds...")
    time.sleep(delay)


def click_on_link(url):
    """Fetches URL content via Selenium and returns a BeautifulSoup object."""
    logger.info(f"Fetching URL with Selenium: {url}")
    driver = get_driver()

    try:
        driver.get(url)
        wait_for_dom_ready(driver, timeout=30)
        html = driver.page_source
        return bs4.BeautifulSoup(html, "lxml")
    except (TimeoutException, WebDriverException) as exc:
        logger.error(f"Error fetching {url} with Selenium: {str(exc)}")
        raise
    finally:
        smart_sleep()

def clean_text(text):
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_all_kved():
    logger.info("Starting KVED parsing")

    try:
        html_sections = click_on_link(URL_BASE)
        logger.info("Retrieved main KVED catalog page")
    except Exception as e:
        logger.error(f"Failed to retrieve main KVED page: {str(e)}")
        return

    section_blocks = html_sections.find_all("div", class_="kved-catalog-table")
    logger.info(f"Found {len(section_blocks)} section blocks")

    for section_block in section_blocks:
        section_code = section_block.find("td", class_="green-col-word").text.strip()
        section_name = section_block.find("td", class_="caps-col").text.strip()
        logger.info(f"Processing section: {section_code} - {section_name}")

        chapter_tds = section_block.find_all("td", class_="green-col-num")
        for chapter_td in chapter_tds:
            chapter_code = chapter_td.text.strip()
            chapter_name = chapter_td.find_next_sibling("td").text.strip()
            logger.info(f"Processing chapter: {chapter_code} - {chapter_name}")

            url_chapter = URL_BASE + chapter_code
            try:
                html_chapter = click_on_link(url_chapter)
                logger.info(f"Retrieved chapter page: {url_chapter}")
            except Exception as e:
                logger.error(f"Failed to retrieve chapter page {url_chapter}: {str(e)}")
                continue

            table = html_chapter.find("table")
            if not table:
                logger.warning(f"No table found for chapter {chapter_code}")
                continue
            rows = table.find_all("tr")

            current_group_code = None
            current_group_name = None

            for row in rows:
                group_code_td = row.find("td", class_="green-col-word")
                if group_code_td:
                    current_group_code = group_code_td.text.strip()
                    current_group_name = row.find("td", class_="caps-col").text.strip()
                    logger.info(f"Processing group: {current_group_code} - {current_group_name}")
                    continue

                class_code_td = row.find("td", class_="green-col-num")
                if class_code_td and current_group_code:
                    class_code = class_code_td.text.strip()
                    class_name = class_code_td.find_next_sibling("td").text.strip()
                    logger.info(f"Processing class: {class_code} - {class_name}")

                    file_path = f"kved_{class_code}.csv"

                    if os.path.exists(file_path):
                        logger.info(f"Skipping already parsed class {class_code} (found {file_path})")
                        continue



                    batch_data = []

                    url_class = url_chapter + f'/{class_code[-2:]}'
                    try:
                        html_class = click_on_link(url_class)
                        logger.info(f"Retrieved class page: {url_class}")
                    except Exception as e:
                        logger.error(f"Failed to retrieve class page {url_class}: {str(e)}")
                        continue

                    pagination = html_class.find("ul", class_="pagination")
                    max_page = 1
                    if pagination:
                        pages = pagination.find_all("li")
                        for li in pages:
                            a = li.find("a")
                            if a and a.text.isdigit():
                                max_page = max(max_page, int(a.text))
                        logger.debug(f"Found {max_page} pages for class {class_code}")

                    pages_to_parse = TARGET_PAGES if TARGET_PAGES else range(1, max_page + 1)

                    for page in pages_to_parse:
                        url_page = url_class + f'?page={page}'
                        try:
                            html_page = click_on_link(url_page)
                            logger.info(f"Retrieved page {page} of class {class_code}")
                            delay = random.uniform(7, 18)  # 5–15 сек
                            logger.debug(f"Sleeping {delay:.2f} seconds to mimic human browsing...")
                            time.sleep(delay)   
                        except Exception as e:
                            logger.error(f"Failed to retrieve page {url_page}: {str(e)}")
                            continue

                        raw_edrpous = html_page.find_all("a", class_="link-details link-open")
                        if not raw_edrpous:
                            logger.warning(f"No companies found on page {page} of class {class_code}")
                            continue

                        for raw_edrpou in raw_edrpous:
                            company_code = raw_edrpou.text.split(",")[0].strip()
                            truncated_link = raw_edrpou.get("href")
                            url_details = f'https://youcontrol.com.ua{truncated_link}'
                            logger.info(f"Processing company: {company_code}")

                            try:
                                html_details = click_on_link(url_details)
                                logger.info(f"Retrieved company details: {company_code}")
                                delay = random.uniform(7, 18)  # 5–15 сек
                                logger.debug(f"Sleeping {delay:.2f} seconds to mimic human browsing...")
                                time.sleep(delay)
                            except Exception as e:
                                logger.error(f"Failed to retrieve company details {url_details}: {str(e)}")
                                continue

                            block_profile = html_details.find("div", class_="seo-table-contain", id="catalog-company-file")
                            if block_profile:
                                profile_rows = block_profile.find_all("div", class_="seo-table-row")
                                logger.debug(f"Found {len(profile_rows)} profile rows for company {company_code}")

                                profile_data_columns = []
                                for row in profile_rows:
                                    text_column = row.find("div", class_="seo-table-col-1").text.strip()
                                    profile_data_columns.append(text_column.split("\n")[0].strip())

                                if 'Організаційно-правова форма' in profile_data_columns and 'Розмір статутного капіталу' in profile_data_columns:
                                    idx_org_form = profile_data_columns.index('Організаційно-правова форма')
                                    idx_capital = profile_data_columns.index('Розмір статутного капіталу')
                                    profile_data_columns.pop(idx_org_form)
                                    profile_data_columns.insert(idx_capital, 'Організаційно-правова форма')

                                names_for_df = {
                                    "Повне найменування юридичної особи": "JUR_PER_NAME",
                                    "Скорочена назва": "JUR_PER_NAME_TRUNC",
                                    "Статус юридичної особи": "JUR_PER_STATUS",
                                    "Статус з ЄДР": "EDR_STATUS",
                                    "Код ЄДРПОУ": "EDRPOU_CODE",
                                    "Дата реєстрації": "REGISTRATION_DATE",
                                    "Уповноважені особи": "AUTHORISED_PERSON",
                                    "Організаційно-правова форма": "ORG_FORM",
                                    "Розмір статутного капіталу": "CAPITAL_SIZE",
                                    "Види діяльності": "ACTIVITES",
                                    "Контактна інформація": "CONTACT_INFO"
                                }

                                profile_data_columns = [names_for_df.get(col.strip(), col) for col in profile_data_columns]

                                text_profile_spans = [row.find("span", class_="copy-file-field") for row in profile_rows]
                                text_profile_divs = [row.find("div", class_="copy-file-field") for row in profile_rows]
                                text_profile_activity = [row.find("p", class_="ucfirst copy-file-field") for row in profile_rows]
                                text_profile_capital = [row.find("div", class_="seo-table-col-2") if row.find("div", class_="seo-table-col-2") and 'copy-hover' not in row.find("div", class_="seo-table-col-2").get("class", []) else None for row in profile_rows]

                                text_profile_spans = [x for x in text_profile_spans if x is not None]
                                text_profile_divs = [x for x in text_profile_divs if x is not None]
                                text_profile_activity = [x for x in text_profile_activity if x is not None]
                                text_profile_capital = [x for x in text_profile_capital if x is not None]

                                data_profile = text_profile_spans + text_profile_divs + text_profile_activity + text_profile_capital
                                raw_data = [data.text for data in data_profile]

                                profile_data_text = []
                                for item in raw_data:
                                    text = " ".join(item.split())
                                    text = re.sub(r"(копіювати|скопійовано|Детальніше)", "", text, flags=re.IGNORECASE)
                                    text = re.sub(r"Дата оновлення.*?(ліцензію\.)", "", text, flags=re.IGNORECASE)
                                    text = re.sub(r"Всього за цим КВЕД:.*?(ФОП)", "", text, flags=re.IGNORECASE)
                                    text = re.sub(r"\s{2,}", " ", text).strip()
                                    if text:
                                        profile_data_text.append(text)

                                profile_dict = dict(zip(profile_data_columns, profile_data_text))
                                logger.debug(f"Extracted profile data for company {company_code}: {profile_dict}")
                            else:
                                profile_dict = {}
                                logger.warning(f"No profile block found for company {company_code}")

                            block_beneficiary = html_details.find("div", class_="seo-table-contain", id="catalog-company-beneficiary")
                            if block_beneficiary:
                                beneficiary_rows = block_beneficiary.find_all("div", class_="seo-table-row")
                                logger.debug(f"Found {len(beneficiary_rows)} beneficiary rows for company {company_code}")

                                beneficiary_data_columns = []
                                for rows in beneficiary_rows:
                                    beneficiary_data_columns.append(rows.find("div", class_="seo-table-col-1").text.strip())

                                raw_edrpou = html_details.find("h2", class_="seo-table-name case-icon short").text
                                edrpou = re.search(r'\d+', raw_edrpou).group() if re.search(r'\d+', raw_edrpou) else None

                                beneficiary_data_columns.append("EDRPOU_CODE")

                                text_beneficiary_spans = [row.find("span", class_="copy-file-field") for row in beneficiary_rows]
                                text_beneficiary_persons = [row.find("div", class_="seo-table-col-2") if row.find("div", class_="seo-table-col-2") and 'copy-hover' not in row.find("div", class_="seo-table-col-2").get("class", []) else None for row in beneficiary_rows]

                                text_beneficiary_spans = [x for x in text_beneficiary_spans if x is not None]
                                text_beneficiary_persons = [x for x in text_beneficiary_persons if x is not None]

                                data_beneficiary = text_beneficiary_spans + text_beneficiary_persons
                                data_beneficiary_text = [data.text for data in data_beneficiary]
                                data_beneficiary_text = data_beneficiary_text + [edrpou]

                                data_beneficiary_dict = dict(zip(beneficiary_data_columns, data_beneficiary_text))
                                data_beneficiary_dict = {k: clean_text(v) for k, v in data_beneficiary_dict.items()}
                                logger.debug(f"Extracted beneficiary data for company {company_code}: {data_beneficiary_dict}")
                            else:
                                data_beneficiary_dict = {}
                                logger.warning(f"No beneficiary block found for company {company_code}")

                            # --- Створюємо базовий запис компанії ---
                            row = {
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

                            # --- Об’єднуємо всі поля з різних блоків у один словник ---
                            combined_data = {}

                            for source in [profile_dict, data_beneficiary_dict]:
                                for key, value in source.items():
                                    if key not in combined_data:
                                        combined_data[key] = value
                                    else:
                                        # Якщо поле вже існує — об’єднуємо через "; "
                                        if isinstance(combined_data[key], str):
                                            combined_data[key] = combined_data[key] + "; " + str(value)
                                        else:
                                            combined_data[key] = str(value)

                            row.update(combined_data)

                            # --- Перевіряємо унікальність ---
                            existing = next((x for x in batch_data if x["EDRPOU_CODE"] == company_code), None)
                            if existing:
                                for key, value in row.items():
                                    if key not in existing or not existing[key]:
                                        existing[key] = value
                            else:
                                batch_data.append(row)


                    if batch_data:
                        df_batch = pd.DataFrame(batch_data)
                        file_path = f"kved_{class_code}.csv"
                        df_batch.to_csv(file_path, mode='w', header=True, index=False)
                        logger.info(f"Saved class {class_code} to {file_path}")

                        # очищуємо batch_data з пам’яті
                        del df_batch
                        del batch_data
                        smart_sleep(base_min=20, base_max=40, long_pause_chance=0.3)

    logger.info("Completed parsing all KVEDs.")

if __name__ == "__main__":
    logger.info("Starting main execution")
    try:
        parse_all_kved()
        logger.info("Data saved to separate kved_#.csv files")
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
    finally:
        shutdown_driver()
