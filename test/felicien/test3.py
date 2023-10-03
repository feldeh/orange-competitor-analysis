from playwright.sync_api import sync_playwright
import json

from pathlib import Path


def save_to_json(json_data, filename):
    raw_data_path = Path.cwd() / f"data/raw_data/{filename}"
    with raw_data_path.open(mode="w", encoding="utf-8") as json_file:
        json_file.write(json_data)

    print(f"JSON data saved to {raw_data_path}")


URL = 'https://mobilevikings.be/en/offer/internet/'


def extract_internet_data(page):

    internet_data = {}

    price = page.query_selector_all('tr.matrix__price td')[0].inner_text()
    data = page.query_selector_all('tr.matrix__data td')[0].inner_text().lower()
    download_speed = page.query_selector_all('tr.matrix__downloadSpeed td')[0].inner_text().encode('ascii', 'ignore').decode('ascii').lower()
    upload_speed = page.query_selector_all('tr.matrix__voice td')[0].inner_text().encode('ascii', 'ignore').decode('ascii').lower()

    internet_data['competitor_name'] = 'mobile_viking'
    internet_data['product_category'] = 'internet_subscription'
    internet_data['product_url'] = URL
    internet_data['price'] = price
    internet_data['data'] = data
    internet_data['network'] = ''
    internet_data['minutes'] = ''
    internet_data['sms'] = ''
    internet_data['download_speed'] = download_speed
    internet_data['upload_speed'] = upload_speed

    return internet_data


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL)
        page.get_by_role("button", name="Accept").click()

        extract_internet_data(page)

        internet_names = page.query_selector_all('.wideScreenFilters__budgetItem__label')

        internet_data_fast = extract_internet_data(page)
        internet_data_fast['product_name'] = internet_names[0].inner_text().lower().replace(' ', '_')

        internet_names[1].click()
        internet_data_superfast = extract_internet_data(page)
        internet_data_superfast['product_name'] = internet_names[1].inner_text().lower().replace(' ', '_')

        internet_data = []
        internet_data.append(internet_data_fast)
        internet_data.append(internet_data_superfast)

        internet_dict = {'internet_subscription_product': internet_data}

        json_data = json.dumps(internet_dict, indent=4)

        print(json_data)
        # save_to_json(json_data, 'internet_subscription_product.json')

        browser.close()


if __name__ == "__main__":
    main()
