from playwright.sync_api import sync_playwright
import json
import re

from utils import save_to_json
from pathlib import Path


URL = 'https://mobilevikings.be/en/offer/internet/'


def internet_speed_cleanup(string):
    pattern = r'(\d+)(gb|mb)'
    match = re.search(pattern, string)
    value = int(match.group(1))

    if match:
        unit = match.group(2)
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
    line_type = page.query_selector_all('tr.matrix__lineType td')[0].inner_text().encode('ascii', 'ignore').decode('ascii').lower().strip()

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
    internet_data['line_type'] = line_type

    return internet_data


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL)

        page.wait_for_selector('#btn-cookie-settings')
        page.query_selector('#btn-cookie-settings').click()
        page.query_selector('#btn-accept-custom-cookies').click(force=True)

        internet_type_btn = page.query_selector_all('.wideScreenFilters__budgetItem__label')

        first_table_data = extract_internet_data(page)
        first_btn_text = internet_type_btn[0].inner_text().lower().replace(' ', '_')
        first_table_data = {'product_name': first_btn_text, **first_table_data}

        internet_type_btn[1].click()

        second_table_data = extract_internet_data(page)
        second_btn_text = internet_type_btn[1].inner_text().lower().replace(' ', '_')
        second_table_data = {'product_name': second_btn_text, **second_table_data}

        internet_data = []
        internet_data.append(first_table_data)
        internet_data.append(second_table_data)

        internet_dict = {'internet_subscription_product': internet_data}

        json_data = json.dumps(internet_dict, indent=4)

        print(json_data)
        save_to_json(json_data, 'internet_subscription_product.json')

        browser.close()


if __name__ == "__main__":
    main()
