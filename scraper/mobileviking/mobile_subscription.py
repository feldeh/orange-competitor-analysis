from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json
import re

URL = 'https://mobilevikings.be/en/offer/subscriptions/'


def extract_subscription_data(page_content):
    subscription_data = []

    soup = BeautifulSoup(page_content, 'html.parser')
    subscription_elements = soup.select('.PostpaidOption')

    for subscription_element in subscription_elements:
        mobile_data = subscription_element.select_one('.PostpaidOption__dataAmount__text').get_text().lower().replace('gb', '').strip()
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
        page.wait_for_selector('#btn-cookie-settings')
        page.query_selector('#btn-cookie-settings').click()
        page.query_selector('#btn-accept-custom-cookies').click(force=True)

        # TODO: implement status code logger with requests
        page_content = page.content()

        subscription_data = extract_subscription_data(page_content)

        subscription_dict = {'mobile_subscription_product': subscription_data}
        json_data = json.dumps(subscription_dict, indent=4)
        print(json_data)

        browser.close()


if __name__ == "__main__":
    main()
