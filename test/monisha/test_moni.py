from playwright.sync_api import sync_playwright
import json
import re



URL = 'https://mobilevikings.be/en/offer/only-data/'

def extract_only_data(page):

    only_data = []
    row = page.locator(f'span.callToAction__text').all_inner_texts()
    cleaned_data = [part.split() for part in row]
    print(cleaned_data)
    data_gb = cleaned_data[0][1]
    data_price = cleaned_data[0][4].replace(',-', '')
    
    only_data.append({
        'product_name': f"mobile_prepaid_only_data_{data_gb}_gb",
        'competitor_name': 'mobile_viking',
        'product_category': 'mobile_prepaid',
        'product_url': URL,
        'price': data_price,
        'data': data_gb,
        'minutes': "unlimited",
        'price_per_minute': "0",
        'sms': "no",
        'internet_speed': ''
    })
    
    print(only_data)
    #print(row)
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



