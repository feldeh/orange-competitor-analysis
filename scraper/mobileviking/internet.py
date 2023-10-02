from playwright.sync_api import sync_playwright
import json
from pathlib import Path

from utils import save_to_json

URL = 'https://mobilevikings.be/en/offer/internet/'


def extract_internet_data(page):
    headers_to_scrape = [
        "price",
        "data",
        "downloadSpeed",
        "voice",
        "lineType",
    ]

    internet_data = {}

    for header in headers_to_scrape:
        row = page.locator(f'tr.matrix__{header}').all_inner_texts()

        cleaned_data = [part.split('\t')[0:2] for part in row]

        key = cleaned_data[0][0]
        value = cleaned_data[0][1].encode('ascii', 'ignore').decode('ascii')
        internet_data[key] = value

    return internet_data


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL)
        page.get_by_role("button", name="Accept").click()

        internet_names = page.query_selector_all('.wideScreenFilters__budgetItem__label')

        internet_data_fast = extract_internet_data(page)
        internet_data_fast['name'] = internet_names[0].inner_text()

        internet_names[1].click()
        internet_data_superfast = extract_internet_data(page)
        internet_data_superfast['name'] = internet_names[1].inner_text()

        internet_data = []
        internet_data.append(internet_data_fast)
        internet_data.append(internet_data_superfast)

        internet_dict = {'internet_plans': internet_data}

        json_data = json.dumps(internet_dict, indent=4)

        print(json_data)
        save_to_json(json_data, 'internet_data.json')

        browser.close()


if __name__ == "__main__":
    main()
