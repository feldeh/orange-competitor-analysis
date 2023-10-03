from playwright.sync_api import sync_playwright
import json
import re

from utils import save_to_json
from pathlib import Path


URL = 'https://mobilevikings.be/en/offer/internet/'


def internet_speed_cleanup(string):
    pattern = r'(\d+)(gb|mb)'
    gb_match = re.search(pattern, string)
    value = int(gb_match.group(1))

    if gb_match:
        unit = gb_match.group(2)
        if unit == "gb":
            cleaned_value = value * 1000
        else:
            cleaned_value = value

    return cleaned_value


def extract_internet_data(page):

    internet_data = {}

    price = page.query_selector_all('tr.matrix__price td')[0].inner_text()
    cleaned_price = re.sub(r'\D', '', price)

    monthly_data = page.query_selector_all('tr.matrix__data td')[0].inner_text().lower()

    download_speed = page.query_selector_all('tr.matrix__downloadSpeed td')[0].inner_text().encode('ascii', 'ignore').decode('ascii').lower().strip()
    upload_speed = page.query_selector_all('tr.matrix__voice td')[0].inner_text().encode('ascii', 'ignore').decode('ascii').lower().strip()

    cleaned_download_speed = internet_speed_cleanup(download_speed)
    cleaned_upload_speed = internet_speed_cleanup(upload_speed)

    internet_data['competitor_name'] = 'mobile_viking'
    internet_data['product_category'] = 'internet_subscription'
    internet_data['product_url'] = URL
    internet_data['price'] = cleaned_price
    internet_data['data'] = monthly_data
    internet_data['network'] = ''
    internet_data['minutes'] = ''
    internet_data['price_per_minute'] = ''
    internet_data['sms'] = ''
    internet_data['download_speed'] = cleaned_download_speed
    internet_data['upload_speed'] = cleaned_upload_speed

    return internet_data


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL, wait_until="load")
        page.get_by_role("button", name="Accept").click()

        extract_internet_data(page)

        internet_names = page.query_selector_all('.wideScreenFilters__budgetItem__label')

        internet_data_fast = extract_internet_data(page)
        first_product_name = internet_names[0].inner_text().lower().replace(' ', '_')
        internet_data_fast = {'product_name': first_product_name, **internet_data_fast}

        internet_names[1].click()

        internet_data_superfast = extract_internet_data(page)
        second_product_name = internet_names[1].inner_text().lower().replace(' ', '_')
        internet_data_superfast = {'product_name': second_product_name, **internet_data_superfast}

        internet_data = []
        internet_data.append(internet_data_fast)
        internet_data.append(internet_data_superfast)

        internet_dict = {'internet_subscription_product': internet_data}

        json_data = json.dumps(internet_dict, indent=4)

        print(json_data)
        save_to_json(json_data, 'internet_subscription_product.json')

        browser.close()


if __name__ == "__main__":
    main()
