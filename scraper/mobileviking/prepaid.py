from playwright.sync_api import sync_playwright
import json

URL = 'https://mobilevikings.be/en/offer/prepaid/'


def extract_prepaid_data(page):
    prepaid_data = []

    page.wait_for_selector('.PrepaidSelectorProduct')

    prepaid_elements = page.query_selector_all('.PrepaidSelectorProduct')

    for id, prepaid_element in enumerate(prepaid_elements, start=1):
        price = prepaid_element.query_selector('.PrepaidSelectorProduct__price').inner_text()

        prepaid_rates_major = prepaid_element.query_selector_all('.PrepaidSelectorProduct__rates__major')
        mobile_data = prepaid_rates_major[0].inner_text().lower().replace('gb', '').strip()
        call_up_to = prepaid_rates_major[1].inner_text().replace('min', '').strip()
        sms_rate = prepaid_rates_major[2].inner_text().lower()

        price_per_minute = prepaid_element.query_selector_all('.PrepaidSelectorProduct__rates__minor')[2].inner_text().replace(',', '.').replace('per minute', '').strip()

        prepaid_data.append({
            'id': id,
            'price': price,
            'mobile_data_gb': mobile_data,
            'call_up_to_min': call_up_to,
            'price_per_minute': price_per_minute,
            'sms_rate': sms_rate
        })

    return prepaid_data


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL)
        page.get_by_role("button", name="Accept").click()

        prepaid_data = extract_prepaid_data(page)

        data_dict = {'prepaid_plans': prepaid_data}

        json_data = json.dumps(data_dict, indent=4)

        print(json_data)

        browser.close()


if __name__ == "__main__":
    main()
