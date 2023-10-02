from playwright.sync_api import sync_playwright
import json
import re



URL = 'https://mobilevikings.be/en/offer/only-data/'

def extract_only_data(page):

    only_data = {}
    row = page.locator(f'span.callToAction__text').all_inner_texts()
    cleaned_data = [part.split(' ')[0:4] for part in row]
    print(cleaned_data)

    return only_data

def main():    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL)
        page.get_by_role("button", name="Accept").click()
        extract_only_data(page)
        browser.close()


if __name__ == "__main__":
    main()



