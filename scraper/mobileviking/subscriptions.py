from playwright.sync_api import sync_playwright
import json
import re

URL = 'https://mobilevikings.be/en/offer/subscriptions/'


def extract_subs_data(page):
    subs_data = []

    page.wait_for_selector('.PostpaidOption')

    subs_elements = page.query_selector_all('.PostpaidOption')

    for id, subs_element in enumerate(subs_elements, start=1):
        mobile_data = subs_element.query_selector('.PostpaidOption__dataAmount__text').inner_text().lower()
        network = subs_element.query_selector('.PostpaidOption__dataAmount__networkTag')
        if network.query_selector('.FourGFiveG--has5g'):
            network = '5g'
        else:
            network = '4g'
        calls_texts = subs_element.query_selector('.PostpaidOption__voiceTextAmount').inner_text().lower()
        price_per_month = subs_element.query_selector('.monthlyPrice__price').inner_text().replace(',-', '')

        minutes_match = re.search(r'(\d+) minutes', calls_texts)
        sms_match = re.search(r'(\d+) texts', calls_texts)

        minutes = minutes_match.group(1) if minutes_match else 'unlimited'
        sms = sms_match.group(1) if sms_match else 'unlimited'

        subs_data.append({
            'id': id,
            'price_per_month': price_per_month,
            'mobile_data': mobile_data,
            'network': network,
            'minutes': minutes,
            'sms': sms
        })

    return subs_data


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL)
        page.get_by_role("button", name="Accept").click()

        subs_data = extract_subs_data(page)

        subs_dict = {'subscription_plans': subs_data}
        json_data = json.dumps(subs_dict, indent=4)

        print(json_data)

        browser.close()


if __name__ == "__main__":
    main()
