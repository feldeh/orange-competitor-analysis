from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json
import re
import time
import logging
from pathlib import Path

from utils import save_to_json

URL = {
    'mobile_prepaid': 'https://mobilevikings.be/en/offer/prepaid/',
    'mobile_subscriptions': 'https://mobilevikings.be/en/offer/subscriptions/',
    'internet_subscription': 'https://mobilevikings.be/en/offer/internet/'
}


def goto_page(browser, url):
    page = browser.new_page()
    page.goto(url, wait_until='domcontentloaded')
    page.wait_for_selector('#btn-accept-cookies')
    page.query_selector('#btn-accept-cookies').click(force=True)

    return page


def extract_prepaid_selector_data(page_content, url):
    prepaid_data = []
    soup = BeautifulSoup(page_content, 'html.parser')

    prepaid_elements = soup.select('.PrepaidSelectorProduct')

    for prepaid_element in prepaid_elements:
        try:
            prepaid_rates_major = prepaid_element.select('.PrepaidSelectorProduct__rates__major')
            sms = prepaid_rates_major[2].get_text().lower()
            price_per_minute = prepaid_element.select('.PrepaidSelectorProduct__rates__minor')[2].get_text().replace(',', '.').replace('per minute', '').strip()
            data_focus = prepaid_element['data-focus']
            data_gb = prepaid_element['data-gb']
            data_min = prepaid_element['data-min']
            data_price = prepaid_element['data-price']

            prepaid_data.append({
                'product_name': f"mobile_prepaid_{data_focus}_{data_gb}_gb",
                'competitor_name': 'mobile_viking',
                'product_category': 'mobile_prepaid',
                'product_url': url,
                'price': data_price,
                'data': data_gb,
                'network': '',
                'minutes': data_min,
                'price_per_minute': price_per_minute,
                'sms': sms,
                'upload_speed': '',
                'download_speed': '',
                'line_type': ''
            })

            return prepaid_data

        except Exception as e:
            error_message = f"Error extracting prepaid data: {str(e)}"
            print(error_message)
            logging.error(error_message)

    return prepaid_data


def activate_toggles(page):
    toggles = page.query_selector_all('.slider')
    for i in range(len(toggles)):
        toggles[i].click()


def extract_prepaid_data(page, url):
    try:
        # TODO: implement status code logger with requests
        page_content = page.content()
        prepaid_data = extract_prepaid_selector_data(page_content, url)
        activate_toggles(page)

        page_content = page.content()
        prepaid_data_calls = extract_prepaid_selector_data(page_content, url)
        prepaid_data.extend(prepaid_data_calls)

        return prepaid_data
    except Exception as e:
        error_message = f"Error extracting prepaid data: {str(e)}"
        print(error_message)
        logging.error(error_message)


def extract_subscription_data(page_content, url):
    subscription_data = []

    try:
        soup = BeautifulSoup(page_content, 'html.parser')
        subscription_elements = soup.select('.PostpaidOption')

        for subscription_element in subscription_elements:

            mobile_data = subscription_element.select_one('.data-amount').get_text().lower().replace('gb', '').strip()
            network = '5g' if subscription_element.select_one('.FourGFiveG--has5g') else '4g'
            calls_texts = subscription_element.select_one('.PostpaidOption__voiceTextAmount').get_text().lower()
            price_per_month = subscription_element.select_one('.monthlyPrice__price').get_text().strip().replace(',-', '')

            minutes_match = re.search(r'(\d+) minutes', calls_texts)
            sms_match = re.search(r'(\d+) texts', calls_texts)

            minutes = minutes_match.group(1) if minutes_match else 'unlimited'
            sms = sms_match.group(1) if sms_match else 'unlimited'

            subscription_data.append({
                'product_name': f"mobile_subscription_{mobile_data}_gb",
                'competitor_name': 'mobile_viking',
                'product_category': 'mobile_subscription',
                'product_url': url,
                'price': price_per_month,
                'data': mobile_data,
                'network': network,
                'minutes': minutes,
                'price_per_minute': '',
                'sms': sms,
                'upload_speed': '',
                'download_speed': '',
                'line_type': ''
            })

            return subscription_data

    except Exception as e:
        error_message = f"Error extracting subscription data: {str(e)}"
        print(error_message)
        logging.error(error_message)


def extract_internet_table_data(page_content, url):
    soup = BeautifulSoup(page_content, 'html.parser')

    internet_data = {}

    try:
        price = soup.select_one('tr.matrix__price td').get_text().strip()
        cleaned_price = price.replace(',-', '')

        monthly_data = soup.select_one('tr.matrix__data td').get_text().lower()

        download_speed = soup.select_one('tr.matrix__downloadSpeed td').get_text().encode('ascii', 'ignore').decode('ascii').lower().strip()
        upload_speed = soup.select_one('tr.matrix__voice td').get_text().encode('ascii', 'ignore').decode('ascii').lower().strip()
        line_type = soup.select_one('tr.matrix__lineType td').get_text().encode('ascii', 'ignore').decode('ascii').lower().strip()

        internet_data['competitor_name'] = 'mobile_viking'
        internet_data['product_category'] = 'internet_subscription'
        internet_data['product_url'] = url
        internet_data['price'] = cleaned_price
        internet_data['data'] = monthly_data
        internet_data['network'] = ''
        internet_data['minutes'] = ''
        internet_data['price_per_minute'] = ''
        internet_data['sms'] = ''
        internet_data['download_speed'] = download_speed
        internet_data['upload_speed'] = upload_speed
        internet_data['line_type'] = line_type

        return internet_data

    except Exception as e:
        error_message = f"Error extracting internet table data: {str(e)}"
        print(error_message)
        logging.error(error_message)


def extract_internet_data(page, url):

    # TODO: implement status code logger with requests
    page_content = page.content()

    try:
        internet_type_btn = page.query_selector_all('.wideScreenFilters__budgetItem__label')

        first_table_data = extract_internet_table_data(page_content, url)
        first_btn_text = internet_type_btn[0].inner_text().lower().replace(' ', '_')
        first_table_data = {'product_name': first_btn_text, **first_table_data}

        internet_type_btn[1].click()

        second_table_data = extract_internet_table_data(page_content, url)
        second_btn_text = internet_type_btn[1].inner_text().lower().replace(' ', '_')
        second_table_data = {'product_name': second_btn_text, **second_table_data}

        internet_data = []
        internet_data.append(first_table_data)
        internet_data.append(second_table_data)

        return internet_data

    except Exception as e:
        error_message = f"Error extracting internet data: {str(e)}"
        print(error_message)
        logging.error(error_message)


def get_mobile_prepaid_data(browser, url):
    page = goto_page(browser, url)
    time.sleep(5)
    logging.info(f"Extracting mobile prepaid data from URL: {url}")
    mobile_prepaid_data = extract_prepaid_data(page, url)
    page.close()

    return mobile_prepaid_data


def get_mobile_subscription_data(browser, url):
    page = goto_page(browser, url)
    time.sleep(5)
    logging.info(f"Extracting mobile subscription from URL: {url}")
    # TODO: implement status code logger with requests
    page_content = page.content()
    mobile_subscription_data = extract_subscription_data(page_content, url)
    page.close()

    return mobile_subscription_data


def get_internet_subscription_data(browser, url):
    page = goto_page(browser, url)
    time.sleep(5)
    logging.info(f"Extracting internet subscription data from URL: {url}")
    internet_subscription_data = extract_internet_data(page, url)
    page.close()

    return internet_subscription_data


def get_products(browser, url):

    prepaid_data = get_mobile_prepaid_data(browser, url['mobile_prepaid'])
    mobile_subscription_data = get_mobile_subscription_data(browser, url['mobile_subscriptions'])
    internet_subscription_data = get_internet_subscription_data(browser, url['internet_subscription'])

    product_list = []
    product_list.extend(prepaid_data)
    product_list.extend(mobile_subscription_data)
    product_list.extend(internet_subscription_data)

    product_dict = {'products': product_list}

    return product_dict


def product_scraper():
    with sync_playwright() as p:

        log_file_name = 'product_scraper.log'
        log_file_path = Path.cwd() / f"scraper/mobileviking/logs/{log_file_name}"
        log_format = '%(asctime)s [%(levelname)s] - %(message)s'
        logging.basicConfig(filename=log_file_path, level=logging.INFO, format=log_format)
        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        start_time_seconds = time.time()
        start_message = f"=========== product_scraper start: {start_time} ==========="
        logging.info(start_message)

        browser = p.chromium.launch(slow_mo=50)

        try:
            product_dict = get_products(browser, URL)

            product_json = json.dumps(product_dict, indent=4)
            print(product_json)

            save_to_json(product_json, 'products.json')
        except Exception as e:
            error_message = f"Error in main function: {str(e)}"
            print(error_message)
            logging.error(error_message)
        finally:
            browser.close()

            end_time_seconds = time.time()
            end_time = time.strftime("%Y-%m-%d %H:%M:%S")
            end_message = f"=========== product_scraper end: {end_time} ==========="
            logging.info("product_scraper execution time: {:.3f}s".format(end_time_seconds - start_time_seconds))
            logging.info(end_message)


if __name__ == "__main__":
    product_scraper()
