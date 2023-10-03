from playwright.sync_api import sync_playwright
import json

from utils import save_to_json


URL = 'https://mobilevikings.be/en/offer/prepaid/'


def extract_prepaid_data(page):
    prepaid_data = []

    page.wait_for_selector('.PrepaidSelectorProduct')

    prepaid_elements = page.query_selector_all('.PrepaidSelectorProduct')

    for prepaid_element in prepaid_elements:

        prepaid_rates_major = prepaid_element.query_selector_all('.PrepaidSelectorProduct__rates__major')

        sms = prepaid_rates_major[2].inner_text().lower()

        price_per_minute = prepaid_element.query_selector_all('.PrepaidSelectorProduct__rates__minor')[2].inner_text().replace(',', '.').replace('per minute', '').strip()
        data_focus = prepaid_element.get_attribute('data-focus')
        data_gb = prepaid_element.get_attribute('data-gb')
        data_min = prepaid_element.get_attribute('data-min')
        data_price = prepaid_element.get_attribute('data-price')

        prepaid_data.append({
            'product_name': f"mobile_prepaid_{data_focus}_{data_gb}_gb",
            'competitor_name': 'mobile_viking',
            'product_category': 'mobile_prepaid',
            'product_url': URL,
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

        page.wait_for_selector('#btn-accept-cookies')
        page.query_selector('#btn-accept-cookies').click()

        prepaid_data = extract_prepaid_data(page)
        activate_toggles(page)
        prepaid_data_calls = extract_prepaid_data(page)

        prepaid_data.extend(prepaid_data_calls)

        indexed_data = [{'product_id': i+1, **item} for i, item in enumerate(prepaid_data)]

        prepaid_dict = {'mobile_prepaid_product': indexed_data}

        json_data = json.dumps(prepaid_dict, indent=4)

        print(json_data)
        save_to_json(json_data, 'mobile_prepaid_product.json')

        browser.close()


if __name__ == "__main__":
    main()
