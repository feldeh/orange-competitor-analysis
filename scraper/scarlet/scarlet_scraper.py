from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time
import logging

from utils import save_to_json, save_to_ndjson

URL = {
    'internet_subscription': 'https://www.scarlet.be/en/abonnement-internet.html'
}


def goto_page(browser, url):
    """Navigate to a web page, handle cookies consent, and return the page."""
    page = browser.new_page()
    page.goto(url, wait_until='domcontentloaded')
    page.wait_for_selector('#onetrust-accept-btn-handler')
    page.query_selector('#onetrust-accept-btn-handler ').click(force=True)
    return page


def extract_internet_table_data(page_content, url):
    """Extract internet subscription table from web page."""
    soup = BeautifulSoup(page_content, 'html.parser')

    try:
        internet_data = []
        tables = soup.find_all('div', class_='small-12 medium-6 large-6 columns')

        for table in tables:

            subscription_name = table.find('h3', class_='rs-ctable-panel-title').get_text().strip().lower()
            internet_volume = table.select_one('ul.rs-ctable-nobulletlist li:nth-of-type(1)').get_text().strip().replace(' Internet volume', '').replace(' GB', '').replace('Unlimited surfing', '-1')
            max_surfing_speed = table.select_one('ul.rs-ctable-nobulletlist li:nth-of-type(2)').get_text().replace('Max surfing speed of ', '').replace(' Mbps', '').strip()
            upload_speed = table.select_one('ul.rs-ctable-nobulletlist li:nth-of-type(3)').get_text().replace('Upload speed ', '').replace('of ', '').replace(' Mbps', '').strip()
            price_per_month = table.find('span', class_='rs-unit').get_text().strip()

            internet_data.append({
                'product_name': subscription_name + '_internet_subscription',
                'competitor_name': 'scarlet',
                'product_category': 'internet_subscription',
                'product_url': url,
                'price': float(price_per_month),
                'date': time.strftime("%Y-%m-%d"),
                'data': float(internet_volume),
                "minutes": None,
                "sms": None,
                'upload_speed': float(upload_speed),
                'download_speed': float(max_surfing_speed),
            })

    except Exception as e:
        error_message = f"Error extracting internet table data: {str(e)}"
        print(error_message)
        logging.error(error_message)

    return internet_data


def extract_internet_data(page, url):
    """Extract data from internet subscription tables."""
    page_content = page.content()

    try:
        first_table_data = extract_internet_table_data(page_content, url)

        internet_data = []
        internet_data.extend(first_table_data)

        return internet_data

    except Exception as e:
        error_message = f"Error extracting internet data: {str(e)}"
        print(error_message)
        logging.error(error_message)


def get_internet_subscription_data(browser, url):

    page = goto_page(browser, url)
    time.sleep(5)
    logging.info(f"Extracting internet subscription data from URL: {url}")
    internet_subscription_data = extract_internet_data(page, url)
    page.close()

    return internet_subscription_data


def get_products(browser, url):
    internet_subscription_data = get_internet_subscription_data(browser, url['internet_subscription'])

    product_dict = {'products': internet_subscription_data}

    return product_dict


def scarlet_internet_scraper():

    with sync_playwright() as p:
        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        start_time_seconds = time.time()

        log_file_name = 'scarlet_scraper.log'
        log_file_path = f"scraper/scarlet/logs/{log_file_name}"
        log_format = '%(asctime)s [%(levelname)s] - %(message)s'
        logging.basicConfig(filename=log_file_path, level=logging.INFO, format=log_format)
        logging.info(f"=========== scarlet_scraper start: {start_time} ===========")

        browser = p.chromium.launch(headless=False, slow_mo=50)
        try:
            product_dict = get_products(browser, URL)
            save_to_ndjson(product_dict['products'], 'products')
            save_to_json(product_dict, 'products')

        except Exception as e:
            error_message = f"Error in main function: {str(e)}"
            print(error_message)
            logging.error(error_message)
        finally:
            browser.close()
            end_time_seconds = time.time()
            execution_time_message = "scarlet_scraper execution time: {:.3f}s".format(end_time_seconds - start_time_seconds)
            print(execution_time_message)
            logging.info(execution_time_message)

            end_time = time.strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"=========== scarlet_scraper end: {end_time} ===========")


if __name__ == "__main__":
    scarlet_internet_scraper()
