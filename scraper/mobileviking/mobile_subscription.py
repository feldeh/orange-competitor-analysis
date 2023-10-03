from playwright.sync_api import sync_playwright
import json
import re

from utils import save_to_json

URL = 'https://mobilevikings.be/en/offer/subscriptions/'


def extract_subscription_data(page):
    subscription_data = []

    page.wait_for_selector('.PostpaidOption')

    subscription_elements = page.query_selector_all('.PostpaidOption')

    for id, subscription_element in enumerate(subscription_elements, start=1):
        mobile_data = subscription_element.query_selector('.PostpaidOption__dataAmount__text').inner_text().lower().replace('gb', '').strip()
        network = subscription_element.query_selector('.PostpaidOption__dataAmount__networkTag')
        if network.query_selector('.FourGFiveG--has5g'):
            network = '5g'
        else:
            network = '4g'
        calls_texts = subscription_element.query_selector('.PostpaidOption__voiceTextAmount').inner_text().lower()
        price_per_month = subscription_element.query_selector('.monthlyPrice__price').inner_text().replace(',-', '')

        minutes_match = re.search(r'(\d+) minutes', calls_texts)
        sms_match = re.search(r'(\d+) texts', calls_texts)

        minutes = minutes_match.group(1) if minutes_match else 'unlimited'
        minutes = minutes_match.group(1) if minutes_match else 'unlimited'
        sms = sms_match.group(1) if sms_match else 'unlimited'

        subscription_data.append({
            'product_id': id,
            'product_name': f"mobile_subscription_{mobile_data}_gb",
            'competitor_name': 'mobile_viking',
            'product_category': 'mobile_subscription',
            'product_url': URL,
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


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL)

        page.wait_for_selector('#btn-accept-cookies')
        page.query_selector('#btn-accept-cookies').click()

        subscription_data = extract_subscription_data(page)

        subscription_dict = {'mobile_subscription_product': subscription_data}
        json_data = json.dumps(subscription_dict, indent=4)

        print(json_data)
        save_to_json(json_data, 'mobile_subscription_product.json')

        browser.close()


if __name__ == "__main__":
    main()
