from playwright.sync_api import sync_playwright
import json
import re

URL = 'https://mobilevikings.be/en/offer/combo/'


def extract_combo_data(page):
    headers_to_scrape = [
        "price",
        "data",
        "downloadSpeed",
        "voice",
        "combo",
        "lineType",
    ]

    combo_data = {}

    for header in headers_to_scrape:
        row = page.locator(f'tr.matrix__{header}').all_inner_texts()

        cleaned_data = [part.split('\t')[0:2] for part in row]

        key = cleaned_data[0][0]
        value = cleaned_data[0][1].encode('ascii', 'ignore').decode('ascii')
        combo_data[key] = value

    return combo_data


def find_combo_advantage(page):
    combo_str = page.get_by_text('combo advantage').all_inner_texts()[1]
    combo_match = re.search(r'(\d+)\scombo', combo_str)
    if combo_match:
        combo_advantage = combo_match.group(1)
        print("combo_advantage: ", combo_advantage)
        return combo_advantage
    else:
        print("Combo advantage match not found")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL)
        page.get_by_role("button", name="Accept").click()

        combo_advantage = find_combo_advantage(page)
        combo_names = page.query_selector_all('.wideScreenFilters__budgetItem__label')

        combo_data_fast = extract_combo_data(page)
        combo_data_fast['combo_advantage'] = combo_advantage
        combo_data_fast['name'] = combo_names[0].inner_text()

        combo_names[1].click()
        combo_data_superfast = extract_combo_data(page)
        combo_data_superfast['combo_advantage'] = combo_advantage
        combo_data_superfast['name'] = combo_names[1].inner_text()

        combo_data = []
        combo_data.append(combo_data_fast)
        combo_data.append(combo_data_superfast)

        combo_dict = {'combo_plans': combo_data}
        json_data = json.dumps(combo_dict, indent=4)

        print(json_data)

        browser.close()


if __name__ == "__main__":
    main()
