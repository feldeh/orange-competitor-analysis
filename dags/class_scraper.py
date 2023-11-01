from data_model import validate_products
from playwright.sync_api import sync_playwright, Playwright
from bs4 import BeautifulSoup
import requests
import re
import time
import logging
import traceback
from typing import List, Dict
# from airflow import AirflowException
from utils import *


# TODO: create a class for log
# ---------- LOGGER START ----------
start_time = time.strftime("%Y-%m-%d %H:%M:%S")
start_time_seconds = time.time()
error_details = 'no error'

log_file_name = 'test.log'
log_file_path = f"logs/{log_file_name}"

log_format = '%(asctime)s [%(levelname)s] - %(message)s'
logging.basicConfig(filename=log_file_path, level=logging.INFO, format=log_format)
logging.info(f"=========== mobileviking_scraper start: {start_time} ===========")
# ---------- LOGGER END ----------


"""
When working with configurations like the one you provided, the common approach is to pass them as arguments during the instantiation of the class. This provides better encapsulation and makes the class more reusable, as you can easily change the config without modifying the class itself.
"""

# config for mobileviking
config = {
    'mobileviking': {
        'competitor': 'mobileviking',
        'baseurl': 'https://mobilevikings.be/en/',
        'endpoint': {
            'mobile_prepaid': 'offer/prepaid/',
            'mobile_subscription': 'offer/subscriptions/',
            'internet_subscription': 'offer/internet/',
            'combo': 'offer/combo/'
        },
        'products': ['mobile_prepaid', 'mobile_subscription', 'internet_subscription'],
        'selector': {
            'cookie_btn': '#btn-accept-cookies',
            'toggle_switch': '.slider',
            'mobile_prepaid': {
                'prepaid_card': '.PrepaidSelectorProduct',
                'sms_parent': '.PrepaidSelectorProduct__rates__major',
                # html data-* attributes
                'data_focus': 'data-focus',
                'data': 'data-gb',
                'minutes': 'data-min',
                'price': 'data-price'
            },
            'mobile_subscription': {
                'mobile_subscription_card': '.PostpaidOption',
                'data': '.data-amount',
                'calls_texts': '.PostpaidOption__voiceTextAmount',
                'price': '.monthlyPrice__price'
            },
            'internet_subscription': {
                'internet_type_btn': '.wideScreenFilters__budgetItem__label',
                'price': 'tr.matrix__price td',
                'data': 'tr.matrix__data td',
                'download_speed': 'tr.matrix__downloadSpeed td',
                'upload_speed': 'tr.matrix__voice td',
            },
            'combo_discount': '.monthlyPrice__discountMessage'
        }}

}


class Scraper:
    # Initialize variables
    def __init__(
        self,
        pw: Playwright,
        config: dict,
        url=None,
        page=None,
        page_content=None
    ) -> None:
        self.browser = pw.chromium.launch(headless=True, slow_mo=50)
        self.config = config
        self.date = time.strftime("%Y-%m-%d")
        self.url = url
        self.page = page
        self.page_content = page_content

    # Scraper class methods

    def goto_page(self, endpoint: str):
        self.url = self.config['baseurl'] + self.config['endpoint'][endpoint]

        # Check url for errors
        check_request(self.url)

        logging.info(f"Navigating to url: {self.url}")
        self.page = self.browser.new_page()
        self.page.goto(self.url, wait_until='domcontentloaded')

        try:
            btn_selector = self.config['selector']['cookie_btn']
            self.page.wait_for_selector(btn_selector)
            self.page.query_selector(btn_selector).click(force=True)
        except Exception as e:
            error_message = f"Accept cookie button error: {str(e)}"
            logging.error(error_message)

            # raise AirflowException(error_message)
            raise

    # Mobileviking methods

    def extract_prepaid_selector_data(self):
        prepaid_data = []
        soup = BeautifulSoup(self.page_content, 'html.parser')

        prepaid_elements = soup.select(self.config['selector']['mobile_prepaid']['prepaid_card'])

        for prepaid_element in prepaid_elements:
            try:
                prepaid_rates_major = prepaid_element.select(self.config['selector']['mobile_prepaid']['sms_parent'])
                sms = prepaid_rates_major[2].get_text().lower()
                data_focus = prepaid_element[self.config['selector']['mobile_prepaid']['data_focus']]
                data = prepaid_element[self.config['selector']['mobile_prepaid']['data']]
                minutes = prepaid_element[self.config['selector']['mobile_prepaid']['minutes']]
                price = prepaid_element[self.config['selector']['mobile_prepaid']['price']]

                sms = unlimited_check_to_float(sms)
                minutes = unlimited_check_to_float(minutes)

                prepaid_data.append({
                    'product_name': f"mobile_prepaid_{data_focus}_{data}_gb",
                    'competitor_name': self.config['competitor'],
                    'product_category': 'mobile_prepaid',
                    'product_url': self.url,
                    'price': float(price),
                    'scraped_at': self.date,
                    'data': float(data),
                    'minutes': minutes,
                    'sms': sms,
                    'upload_speed': None,
                    'download_speed': None,
                })

                return prepaid_data

            except Exception as e:
                error_message = f"Error extracting prepaid selector data: {str(e)}"
                logging.error(error_message)
                traceback.print_exc()
                # raise AirflowException(error_message)
                raise

    def activate_toggle_switch(self):
        try:
            logging.info(self.page)
            toggles = self.page.query_selector_all(self.config['selector']['toggle_switch'])
            for i in range(len(toggles)):
                toggles[i].click()
        except Exception as e:
            error_message = f"Error activating toggles: {str(e)}"
            logging.error(error_message)
            traceback.print_exc()
            # raise AirflowException(error_message)
            raise

    def extract_prepaid_data(self):
        self.page_content = self.page.content()
        prepaid_data = self.extract_prepaid_selector_data()
        self.activate_toggle_switch()

        self.page_content = self.page.content()
        prepaid_data_calls = self.extract_prepaid_selector_data()
        prepaid_data.extend(prepaid_data_calls)

        return prepaid_data

    def extract_subscription_data(self):
        subscription_data = []

        try:
            soup = BeautifulSoup(self.page_content, 'html.parser')
            subscription_elements = soup.select(self.config['selector']['mobile_subscription']['mobile_subscription_card'])

            for subscription_element in subscription_elements:

                mobile_data = subscription_element.select_one(self.config['selector']['mobile_subscription']['data']).get_text().lower().replace('gb', '').strip()
                calls_texts = subscription_element.select_one(self.config['selector']['mobile_subscription']['calls_texts']).get_text().lower()
                price_per_month = subscription_element.select_one(self.config['selector']['mobile_subscription']['price']).get_text().strip().replace(',-', '')

                minutes_match = re.search(r'(\d+) minutes', calls_texts)
                sms_match = re.search(r'(\d+) texts', calls_texts)

                minutes = float(minutes_match.group(1)) if minutes_match else -1
                sms = int(sms_match.group(1)) if sms_match else -1

                subscription_data.append({
                    'product_name': f"mobile_subscription_{mobile_data}_gb",
                    'competitor_name': self.config['competitor'],
                    'product_category': 'mobile_subscription',
                    'product_url': self.url,
                    'price': float(price_per_month),
                    'scraped_at': self.date,
                    'data': float(mobile_data),
                    'minutes': minutes,
                    'sms': sms,
                    'upload_speed': None,
                    'download_speed': None,
                })

            return subscription_data

        except Exception as e:
            error_message = f"Error extracting subscription data: {str(e)}"
            logging.error(error_message)
            traceback.print_exc()
            # raise AirflowException(error_message)
            raise

    def extract_internet_table_data(self):
        soup = BeautifulSoup(self.page_content, 'html.parser')

        internet_data = {}

        try:
            price = soup.select_one(self.config['selector']['internet_subscription']['price']).get_text().strip()
            cleaned_price = price.replace(',-', '')

            monthly_data = soup.select_one(self.config['selector']['internet_subscription']['data']).get_text().lower()

            download_speed = soup.select_one(self.config['selector']['internet_subscription']['download_speed']).get_text().encode('ascii', 'ignore').decode('ascii').lower().strip()
            upload_speed = soup.select_one(self.config['selector']['internet_subscription']['upload_speed']).get_text().encode('ascii', 'ignore').decode('ascii').lower().strip()

            monthly_data = unlimited_check_to_float(monthly_data)

            internet_data['competitor_name'] = self.config['competitor']
            internet_data['product_category'] = 'internet_subscription'
            internet_data['product_url'] = self.url
            internet_data['price'] = float(cleaned_price)
            internet_data['scraped_at'] = self.date
            internet_data['data'] = monthly_data
            internet_data['minutes'] = None
            internet_data['sms'] = None
            internet_data['download_speed'] = download_speed
            internet_data['upload_speed'] = upload_speed

            return internet_data

        except Exception as e:
            error_message = f"Error extracting internet table data: {str(e)}"
            logging.error(error_message)
            traceback.print_exc()
            # raise AirflowException(error_message)
            raise

    def extract_internet_data(self):

        self.page_content = self.page.content()

        try:
            internet_type_btn = self.page.query_selector_all(self.config['selector']['internet_subscription']['internet_type_btn'])

            first_table_data = self.extract_internet_table_data()
            first_btn_text = internet_type_btn[0].inner_text().lower().replace(' ', '_')
            first_table_data = {'product_name': first_btn_text, **first_table_data}

            internet_type_btn[1].click()

            self.page_content = self.page.content()

            second_table_data = self.extract_internet_table_data()
            second_btn_text = internet_type_btn[1].inner_text().lower().replace(' ', '_')
            second_table_data = {'product_name': second_btn_text, **second_table_data}

            internet_data = []
            internet_data.append(first_table_data)
            internet_data.append(second_table_data)

            return internet_data

        except Exception as e:
            error_message = f"Error extracting internet data: {str(e)}"
            logging.error(error_message)
            traceback.print_exc()
            # raise AirflowException(error_message)
            raise

    # TODO: create a single method for these 3

    def get_mobile_prepaid_data(self):

        self.goto_page('mobile_prepaid')
        time.sleep(5)
        logging.info(f"Extracting mobile prepaid data from: {self.url}")
        mobile_prepaid_data = self.extract_prepaid_data()
        self.page.close()

        return mobile_prepaid_data

    def get_mobile_subscription_data(self):

        self.goto_page('mobile_subscription')
        time.sleep(5)
        logging.info(f"Extracting mobile subscription from: {self.url}")
        self.page_content = self.page.content()
        mobile_subscription_data = self.extract_subscription_data()
        self.page.close()

        return mobile_subscription_data

    def get_internet_subscription_data(self):

        self.goto_page('internet_subscription')
        time.sleep(5)
        logging.info(f"Extracting internet subscription data from: {self.url}")
        internet_subscription_data = self.extract_internet_data()
        self.page.close()

        return internet_subscription_data

    def get_products(self):

        try:
            prepaid_data = self.get_mobile_prepaid_data()
            mobile_subscription_data = self.get_mobile_subscription_data()
            internet_subscription_data = self.get_internet_subscription_data()

            product_list = []
            product_list.extend(prepaid_data)
            product_list.extend(mobile_subscription_data)
            product_list.extend(internet_subscription_data)

            # Validate product_list
            validate_products(product_list)

            return product_list

        except Exception as e:
            error_message = f"Validation error: {str(e)}"
            logging.error(error_message)
            traceback.print_exc()
            # raise AirflowException(error_message)
            raise

    def extract_combo_advantage(self):
        try:
            self.url = self.config['baseurl'] + self.config['endpoint']['combo']
            self.page_content = requests.get(self.url).text
            soup = BeautifulSoup(self.page_content, "html.parser")
            combo_text = soup.select_one(self.config['selector']['combo_discount']).get_text()
            match = re.search(r'\d+', combo_text)
            combo_advantage = int(match.group())

            return combo_advantage

        except Exception as e:
            error_message = f'Error extracting combo: {str(e)}'
            logging.error(error_message)
            traceback.print_exc()
            # raise AirflowException(error_message)
            raise

    # def generate_packs(self, combo_advantage):
    #     """
    #     Generate packs based on mobile + internet products combinations
    #     """
    #     logging.info('Generating packs')
    #     try:
    #         packs_list = []
    #         mobile_products = [product for product in products_list if 'mobile' in product['product_name']]
    #         internet_products = [product for product in products_list if 'internet' in product['product_name']]

    #         for internet_product in internet_products:
    #             for mobile_product in mobile_products:
    #                 price = float(mobile_product['price']) + float(internet_product['price']) - combo_advantage

    #                 pack_name = f"{mobile_product['product_name']}_{internet_product['product_name']}"
    #                 competitor_name = internet_product['competitor_name']

    #                 # mobile_product_name = mobile_product['product_name']
    #                 # internet_product_name = internet_product['product_name']

    #                 packs_list.append(
    #                     {
    #                         'competitor_name': competitor_name,
    #                         'pack_name': pack_name,
    #                         'pack_url': self.url,
    #                         'pack_description': None,
    #                         'price': price,
    #                         'scraped_at': self.date,
    #                         # 'mobile_product_name': mobile_product_name,
    #                         # 'internet_product_name': internet_product_name
    #                     })

    #         packs_dict = {'packs': packs_list}

    #         return packs_dict

        # except Exception as e:
        #     error_message = f'Error generating packs: {str(e)}'
        #     logging.error(error_message)
        #     traceback.print_exc()
            # raise AirflowException(error_message)
            raise


def mobileviking_scraper(config):
    with sync_playwright() as pw:
        bro_obj = Scraper(pw, config)
        print(bro_obj.get_products())


mobileviking_scraper(config['mobileviking'])
