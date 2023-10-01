from playwright.sync_api import sync_playwright
import json

URL = 'https://mobilevikings.be/en/offer/prepaid/'


def extract_prepaid_data(page):
    prepaid_data = []

    page.wait_for_selector('.PrepaidSelectorProduct')

    prepaid_elements = page.query_selector_all('.PrepaidSelectorProduct')

    for prepaid_element in prepaid_elements:
        price = prepaid_element.query_selector('.PrepaidSelectorProduct__price').inner_text()

        prepaid_rates_major = prepaid_element.query_selector_all('.PrepaidSelectorProduct__rates__major')
        mobile_data = prepaid_rates_major[0].inner_text().lower()
        minutes = prepaid_rates_major[1].inner_text().replace('min', '').strip()
        sms = prepaid_rates_major[2].inner_text().lower()

        price_per_minute = prepaid_element.query_selector_all('.PrepaidSelectorProduct__rates__minor')[2].inner_text().replace(',', '.').replace('per minute', '').strip()

        prepaid_data.append({
            'price': price,
            'mobile_data': mobile_data,
            'minutes': minutes,
            'price_per_minute': price_per_minute,
            'sms': sms
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
        page.get_by_role("button", name="Accept").click()

        prepaid_data = extract_prepaid_data(page)
        activate_toggles(page)
        prepaid_data_calls = extract_prepaid_data(page)

        prepaid_data.extend(prepaid_data_calls)

        indexed_data = [{'id': i+1, **item} for i, item in enumerate(prepaid_data)]

        prepaid_dict = {'prepaid_plans': indexed_data}

        json_data = json.dumps(prepaid_dict, indent=4)

        print(json_data)

        browser.close()


if __name__ == "__main__":
    main()
