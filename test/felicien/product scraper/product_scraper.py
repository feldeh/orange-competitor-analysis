from playwright.sync_api import sync_playwright
import json
import re


# from utils import save_to_json


URL = {
    'mobile_prepaid': 'https://mobilevikings.be/en/offer/prepaid/',
    'mobile_subscriptions': 'https://mobilevikings.be/en/offer/subscriptions/',
    'internet_subscription': 'https://mobilevikings.be/en/offer/internet/'

}


def extract_prepaid_panel_data(page, url):
    panel_data = []

    page.wait_for_selector('.PrepaidSelectorProduct')

    prepaid_elements = page.query_selector_all('.PrepaidSelectorProduct')

    for prepaid_element in prepaid_elements:

        prepaid_rates_major = prepaid_element.query_selector_all('.PrepaidSelectorProduct__rates__major')

        sms = prepaid_rates_major[2].inner_text().lower()

        price_per_minute = prepaid_element.query_selector_all('.PrepaidSelectorProduct__rates__minor')[2].inner_text().replace(',', '.').replace('per minute', '').strip()
        data_focus = prepaid_element.get_attribute('data-focus')
        data_gb = prepaid_element.get_attribute('data-gb')
        data_min = prepaid_element.get_attribute('data-min')
        data_price = prepaid_element.get_attribute('data-price')

        panel_data.append({
            'product_name': f"mobile_prepaid_{data_focus}_{data_gb}_gb",
            'competitor_name': 'mobile_viking',
            'product_category': 'mobile_prepaid',
            'product_url': url,
            'price': data_price,
            'data': data_gb,
            'network': '',
            'minutes': data_min,
            'price_per_minute': price_per_minute,
            'sms': sms,
            'upload_speed': '',
            'download_speed': '',
            'line_type': ''
        })

    return panel_data


def activate_toggles(page):
    toggles = page.query_selector_all('.slider')
    for i in range(len(toggles)):
        toggles[i].click()


def extract_prepaid_data(page, url):

    prepaid_data = extract_prepaid_panel_data(page, url)
    activate_toggles(page)
    prepaid_data_calls = extract_prepaid_panel_data(page, url)
    prepaid_data.extend(prepaid_data_calls)

    return prepaid_data


def goto_page(browser, url):
    page = browser.new_page()
    page.goto(url, wait_until='domcontentloaded')
    page.wait_for_selector('#btn-cookie-settings')
    page.query_selector('#btn-cookie-settings').click()
    page.query_selector('#btn-accept-custom-cookies').click(force=True)

    return page


def extract_subscription_data(page, url):
    subscription_data = []

    page.wait_for_selector('.PostpaidOption')

    subscription_elements = page.query_selector_all('.PostpaidOption')

    for subscription_element in subscription_elements:
        # subscription_element.wait_for_selector('.PostpaidOption__dataAmount__text', timeout=100000)
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
            'product_name': f"mobile_subscription_{mobile_data}_gb",
            'competitor_name': 'mobile_viking',
            'product_category': 'mobile_subscription',
            'product_url': url,
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


def internet_speed_cleanup(string):
    pattern = r'(\d+)(gb|mb)'
    match = re.search(pattern, string)
    value = int(match.group(1))

    if match:
        unit = match.group(2)
        if unit == "gb":
            cleaned_value = value * 1000
        else:
            cleaned_value = value

    return cleaned_value


def extract_internet_table_data(page, url):

    internet_data = {}
    page.wait_for_selector('tr.matrix__price td')
    price = page.query_selector_all('tr.matrix__price td')[0].inner_text()
    cleaned_price = re.sub(r'\D', '', price)

    monthly_data = page.query_selector_all('tr.matrix__data td')[0].inner_text().lower()

    download_speed = page.query_selector_all('tr.matrix__downloadSpeed td')[0].inner_text().encode('ascii', 'ignore').decode('ascii').lower().strip()
    upload_speed = page.query_selector_all('tr.matrix__voice td')[0].inner_text().encode('ascii', 'ignore').decode('ascii').lower().strip()
    line_type = page.query_selector_all('tr.matrix__lineType td')[0].inner_text().encode('ascii', 'ignore').decode('ascii').lower().strip()

    print(download_speed)
    print(upload_speed)

    cleaned_download_speed = internet_speed_cleanup(download_speed)
    cleaned_upload_speed = internet_speed_cleanup(upload_speed)

    internet_data['competitor_name'] = 'mobile_viking'
    internet_data['product_category'] = 'internet_subscription'
    internet_data['product_url'] = url
    internet_data['price'] = cleaned_price
    internet_data['data'] = monthly_data
    internet_data['network'] = ''
    internet_data['minutes'] = ''
    internet_data['price_per_minute'] = ''
    internet_data['sms'] = ''
    internet_data['download_speed'] = cleaned_download_speed
    internet_data['upload_speed'] = cleaned_upload_speed
    internet_data['line_type'] = line_type

    return internet_data


def extract_internet_data(page, url):

    internet_type_btn = page.query_selector_all('.wideScreenFilters__budgetItem__label')

    first_table_data = extract_internet_table_data(page, url)
    first_btn_text = internet_type_btn[0].inner_text().lower().replace(' ', '_')
    first_table_data = {'product_name': first_btn_text, **first_table_data}

    internet_type_btn[1].click()

    second_table_data = extract_internet_table_data(page, url)
    second_btn_text = internet_type_btn[1].inner_text().lower().replace(' ', '_')
    second_table_data = {'product_name': second_btn_text, **second_table_data}

    internet_data = []
    internet_data.append(first_table_data)
    internet_data.append(second_table_data)

    return internet_data


def get_mobile_prepaid_data(browser, url):
    page = goto_page(browser, url)
    mobile_prepaid_data = extract_prepaid_data(page, url)
    page.close()

    return mobile_prepaid_data


def get_mobile_subscription_data(browser, url):
    page = goto_page(browser, url)
    mobile_subscription_data = extract_subscription_data(page, url)
    page.close()

    return mobile_subscription_data


def get_internet_subscription_data(browser, url):
    page = goto_page(browser, url)
    internet_subscription_data = extract_internet_data(page, url)
    page.close()

    return internet_subscription_data


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        prepaid_data = get_mobile_prepaid_data(browser, URL['mobile_prepaid'])
        mobile_subscription_data = get_mobile_subscription_data(browser, URL['mobile_subscriptions'])
        internet_subscription_data = get_internet_subscription_data(browser, URL['internet_subscription'])

        print(prepaid_data)
        print(internet_subscription_data)

        browser.close()


if __name__ == "__main__":
    main()
