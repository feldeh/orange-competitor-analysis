from data_model import validate_products
from playwright.sync_api import sync_playwright, Playwright
from bs4 import BeautifulSoup
import requests
import re
import time
import logging
import traceback
from typing import List, Dict
from airflow import AirflowException
from utils import *


class Scraper:
    def __init__(
        self,
        browser,
        config: dict,
        url=None,
        page=None,
        page_content=None,
        logger=None
    ) -> None:
        self._browser = browser
        self._config = config
        self.logger = logger
        self._initialize_logging()
        self._date = time.strftime("%Y-%m-%d")
        self._url = url
        self._page = page
        self._page_content = page_content

    def _initialize_logging(self):
        if self.logger is None:
            # Create a logger
            self.logger = logging.getLogger()

            # Set the logging level
            self.logger.setLevel(logging.INFO)

            log_file_name = 'scraper.log'
            log_file_path = f"./logs/{log_file_name}"

            # Create a handler to log to a file
            handler = logging.FileHandler(log_file_path)
            formatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')
            handler.setFormatter(formatter)

            # Add the handler to the logger
            self.logger.addHandler(handler)

    def _accept_cookie(self):
        try:
            btn_selector = self._config['selector']['cookie_btn']
            self._page.wait_for_selector(btn_selector)
            self._page.query_selector(btn_selector).click(force=True)
        except Exception as e:
            error_message = f"Accept cookie button error: {str(e)}"
            self.logger.error(error_message)

            raise AirflowException(error_message)


    def _navigate(self, endpoint: str):
        self._url = self._config['baseurl'] + self._config['endpoint'][endpoint]

        # Check url for errors
        check_request(self._url)

        self.logger.info(f"Navigating to url: {self._url}")
        self._page = self._browser.new_page()
        self._page.goto(self._url, wait_until='domcontentloaded')

        self._accept_cookie()
        time.sleep(3)

    def _extract_prepaid_selector_data(self):
        prepaid_data = []
        soup = BeautifulSoup(self._page_content, 'html.parser')
        try:
            prepaid_elements = soup.select(self._config['selector']['mobile_prepaid']['prepaid_card'])

            check_empty_el(prepaid_elements, self._config['selector']['mobile_prepaid']['prepaid_card'])

            for prepaid_element in prepaid_elements:

                prepaid_rates_major = prepaid_element.select(self._config['selector']['mobile_prepaid']['sms_parent'])
                check_empty_el(prepaid_rates_major, self._config['selector']['mobile_prepaid']['sms_parent'])

                sms = prepaid_rates_major[2].get_text().lower()
                # Access html data-* attributes instead of css
                data_focus = prepaid_element[self._config['selector']['mobile_prepaid']['data_focus']]
                data = prepaid_element[self._config['selector']['mobile_prepaid']['data']]
                minutes = prepaid_element[self._config['selector']['mobile_prepaid']['minutes']]
                price = prepaid_element[self._config['selector']['mobile_prepaid']['price']]

                sms = unlimited_check_to_float(sms)
                minutes = unlimited_check_to_float(minutes)

                prepaid_data.append({
                    'product_name': f"mobile_prepaid_{data_focus}_{data}_gb",
                    'competitor_name': self._config['competitor'],
                    'product_category': 'mobile_prepaid',
                    'product_url': self._url,
                    'price': float(price),
                    'scraped_at': self._date,
                    'data': float(data),
                    'minutes': minutes,
                    'sms': sms,
                    'upload_speed': None,
                    'download_speed': None,
                })

            return prepaid_data

        except Exception as e:
            error_message = f"Error extracting prepaid selector data: {str(e)}"
            self.logger.error(error_message)
            traceback.print_exc()
            raise AirflowException(error_message)


    def _activate_toggle_switch(self):
        try:
            toggles = self._page.query_selector_all(self._config['selector']['toggle_switch'])
            check_empty_el(toggles, self._config['selector']['toggle_switch'])
            for i in range(len(toggles)):
                toggles[i].click()
        except Exception as e:
            error_message = f"Error activating toggles: {str(e)}"
            self.logger.error(error_message)
            traceback.print_exc()
            raise AirflowException(error_message)


    def _extract_prepaid_data(self):
        self._page_content = self._page.content()
        prepaid_data = self._extract_prepaid_selector_data()
        self._activate_toggle_switch()
        self._page_content = self._page.content()
        prepaid_data_calls = self._extract_prepaid_selector_data()
        prepaid_data.extend(prepaid_data_calls)

        return prepaid_data

    def _extract_subscription_data(self):
        subscription_data = []
        self._page_content = self._page.content()
        try:
            soup = BeautifulSoup(self._page_content, 'html.parser')
            subscription_elements = soup.select(self._config['selector']['mobile_subscription']['mobile_subscription_card'])

            check_empty_el(subscription_elements, self._config['selector']['mobile_subscription']['mobile_subscription_card'])

            for subscription_element in subscription_elements:
                mobile_data_element = subscription_element.select_one(self._config['selector']['mobile_subscription']['data'])
                check_empty_el(mobile_data_element, self._config['selector']['mobile_subscription']['data'])
                mobile_data = mobile_data_element.get_text().lower().replace('gb', '').strip()

                calls_texts_element = subscription_element.select_one(self._config['selector']['mobile_subscription']['calls_texts'])
                check_empty_el(calls_texts_element, self._config['selector']['mobile_subscription']['calls_texts'])
                calls_texts = calls_texts_element.get_text().lower()

                price_per_month_element = subscription_element.select_one(self._config['selector']['mobile_subscription']['price'])
                check_empty_el(price_per_month_element, self._config['selector']['mobile_subscription']['price'])
                price_per_month = price_per_month_element.get_text().strip().replace(',-', '')

                minutes_match = re.search(r'(\d+) minutes', calls_texts)
                sms_match = re.search(r'(\d+) texts', calls_texts)

                minutes = float(minutes_match.group(1)) if minutes_match else -1
                sms = int(sms_match.group(1)) if sms_match else -1

                subscription_data.append({
                    'product_name': f"mobile_subscription_{mobile_data}_gb",
                    'competitor_name': self._config['competitor'],
                    'product_category': 'mobile_subscription',
                    'product_url': self._url,
                    'price': float(price_per_month),
                    'scraped_at': self._date,
                    'data': float(mobile_data),
                    'minutes': minutes,
                    'sms': sms,
                    'upload_speed': None,
                    'download_speed': None,
                })

            return subscription_data

        except Exception as e:
            error_message = f"Error extracting subscription data: {str(e)}"
            self.logger.error(error_message)
            traceback.print_exc()
            raise AirflowException(error_message)


    def _extract_internet_table_data(self):
        soup = BeautifulSoup(self._page_content, 'html.parser')

        internet_data = {}

        try:
            price_element = soup.select_one(self._config['selector']['internet_subscription']['price'])
            check_empty_el(price_element, self._config['selector']['internet_subscription']['price'])
            price = price_element.get_text().strip()
            cleaned_price = price.replace(',-', '')

            monthly_data_element = soup.select_one(self._config['selector']['internet_subscription']['data'])
            check_empty_el(monthly_data_element, self._config['selector']['internet_subscription']['data'])
            monthly_data = monthly_data_element.get_text().lower()

            download_speed_element = soup.select_one(self._config['selector']['internet_subscription']['download_speed'])
            check_empty_el(download_speed_element, self._config['selector']['internet_subscription']['download_speed'])
            download_speed = download_speed_element.get_text().encode('ascii', 'ignore').decode('ascii').lower().strip()

            upload_speed_element = soup.select_one(self._config['selector']['internet_subscription']['upload_speed'])
            check_empty_el(upload_speed_element, self._config['selector']['internet_subscription']['upload_speed'])
            upload_speed = upload_speed_element.get_text().encode('ascii', 'ignore').decode('ascii').lower().strip()

            monthly_data = unlimited_check_to_float(monthly_data)

            internet_data['competitor_name'] = self._config['competitor']
            internet_data['product_category'] = 'internet_subscription'
            internet_data['product_url'] = self._url
            internet_data['price'] = float(cleaned_price)
            internet_data['scraped_at'] = self._date
            internet_data['data'] = monthly_data
            internet_data['minutes'] = None
            internet_data['sms'] = None
            internet_data['download_speed'] = download_speed
            internet_data['upload_speed'] = upload_speed

            return internet_data

        except Exception as e:
            error_message = f"Error extracting internet table data: {str(e)}"
            self.logger.error(error_message)
            traceback.print_exc()
            raise AirflowException(error_message)


    def _extract_internet_data(self):

        self._page_content = self._page.content()

        try:
            internet_type_btn = self._page.query_selector_all(self._config['selector']['internet_subscription']['internet_type_btn'])
            check_empty_el(internet_type_btn, self._config['selector']['internet_subscription']['internet_type_btn'])

            first_table_data = self._extract_internet_table_data()
            first_btn_text = internet_type_btn[0].inner_text().lower().replace(' ', '_')
            first_table_data = {'product_name': first_btn_text, **first_table_data}

            internet_type_btn[1].click()

            self._page_content = self._page.content()

            second_table_data = self._extract_internet_table_data()
            second_btn_text = internet_type_btn[1].inner_text().lower().replace(' ', '_')
            second_table_data = {'product_name': second_btn_text, **second_table_data}

            internet_data = []
            internet_data.append(first_table_data)
            internet_data.append(second_table_data)

            return internet_data

        except Exception as e:
            error_message = f"Error extracting internet data: {str(e)}"
            self.logger.error(error_message)
            traceback.print_exc()
            raise AirflowException(error_message)

    def _extract_page_data(self, product_type):
        self._navigate(product_type)
        self.logger.info(f"Extracting mobile prepaid data from: {self._url}")

        if product_type == 'mobile_prepaid':
            data = self._extract_prepaid_data()
        elif product_type == 'mobile_subscription':
            data = self._extract_subscription_data()
        elif product_type == 'internet_subscription':
            data = self._extract_internet_data()

        self._page.close()

        return data

    def get_products(self):

        try:
            prepaid_data = self._extract_page_data('mobile_prepaid')
            mobile_subscription_data = self._extract_page_data('mobile_subscription')
            internet_subscription_data = self._extract_page_data('internet_subscription')

            product_list = []
            product_list.extend(prepaid_data)
            product_list.extend(mobile_subscription_data)
            product_list.extend(internet_subscription_data)

            # Validate product_list
            product_dict = validate_products(product_list)

            return product_dict

        except Exception as e:
            error_message = f"Validation error: {str(e)}"
            self.logger.error(error_message)
            traceback.print_exc()
            raise AirflowException(error_message)


    def _extract_discount(self):
        try:
            self._url = self._config['baseurl'] + self._config['endpoint']['discount']
            self._page_content = requests.get(self._url).text
            soup = BeautifulSoup(self._page_content, "html.parser")
            combo_text = soup.select_one(self._config['selector']['discount']).get_text()
            match = re.search(r'\d+', combo_text)
            discount = int(match.group())

            return discount

        except Exception as e:
            error_message = f'Error extracting discount: {str(e)}'
            self.logger.error(error_message)
            traceback.print_exc()
            raise AirflowException(error_message)


    def generate_packs(self, products_list):
        """
        Generate packs based on mobile + internet products combinations
        """
        self.logger.info('Generating packs')
        try:
            discount = self._extract_discount()
            packs_list = []
            mobile_products = [product for product in products_list if 'mobile' in product['product_name']]
            internet_products = [product for product in products_list if 'internet' in product['product_name']]

            for internet_product in internet_products:
                for mobile_product in mobile_products:
                    price = float(mobile_product['price']) + float(internet_product['price']) - discount

                    pack_name = f"{mobile_product['product_name']}_{internet_product['product_name']}"
                    competitor_name = internet_product['competitor_name']

                    packs_list.append(
                        {
                            'competitor_name': competitor_name,
                            'pack_name': pack_name,
                            'pack_url': self._url,
                            'pack_description': None,
                            'price': price,
                            'scraped_at': self._date,

                        })

            packs_dict = {'packs': packs_list}

            return packs_dict

        except Exception as e:
            error_message = f'Error generating packs: {str(e)}'
            self.logger.error(error_message)
            traceback.print_exc()
            raise AirflowException(error_message)



def mobileviking_scraper(config):
    with sync_playwright() as pw:
        error_details = 'no error'
        try:
            browser = pw.chromium.launch(headless=True, slow_mo=50)
            scraper_object = Scraper(browser, config)
            product_dict = scraper_object.get_products()
            save_to_json(product_dict, "mobileviking", 'products')
            packs_dict = scraper_object.generate_packs(product_dict['products'])
            save_to_json(packs_dict, "mobileviking", 'packs')
        except Exception as e:
            error_message = f"Error scraping mobileviking: {str(e)}"
            error_details = error_message
            traceback.print_exc()
            raise AirflowException(error_message)
        finally:
            browser.close()
            save_scraping_log(error_details, 'mobileviking')