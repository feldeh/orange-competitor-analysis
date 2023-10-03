from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import json

URL = 'https://mobilevikings.be/en/offer/prepaid/'


def extract_prepaid_data(page_content, url):
    prepaid_data = []
    soup = BeautifulSoup(page_content, 'html.parser')

    prepaid_elements = soup.select('.PrepaidSelectorProduct')

    for prepaid_element in prepaid_elements:
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


def activate_toggles(page):
    toggles = page.query_selector_all('.slider')
    for i in range(len(toggles)):
        toggles[i].click()


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL)
        page.wait_for_selector('#btn-cookie-settings')
        page.query_selector('#btn-cookie-settings').click()
        page.query_selector('#btn-accept-custom-cookies').click(force=True)

        page_content = page.content()

        prepaid_data = extract_prepaid_data(page_content, URL)

        activate_toggles(page)

        second_page_content = page.content()

        prepaid_data_calls = extract_prepaid_data(second_page_content, URL)

        prepaid_data.extend(prepaid_data_calls)

        prepaid_dict = {'mobile_prepaid_product': prepaid_data}

        json_data = json.dumps(prepaid_dict, indent=4)

        print(json_data)


if __name__ == "__main__":
    main()
