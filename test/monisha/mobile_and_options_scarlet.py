from playwright.sync_api import sync_playwright
import json
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import traceback




URL = 'https://www.scarlet.be/en/abonnement-gsm.html'

def goto_page(browser, url):
    page = browser.new_page()
    page.goto(url, wait_until='domcontentloaded')
    try:
        page.wait_for_selector('#onetrust-accept-btn-handler')
        page.query_selector('#onetrust-accept-btn-handler').click(force=True)
    except Exception as e:
        error_message = f"Accept cookie button error: {str(e)}"
        logging.error(error_message)

    return page

def unlimited_check_to_float(string):
    return -1 if string.lower() == 'unlimited' else float(string)

def extract_mobile_subscription_data(page_content, url):

    mobile_subscription_data = []
    date = time.strftime("%Y-%m-%d")
    
    try:
        soup = BeautifulSoup(page_content, 'html.parser')
        mobile_subscription_elements = soup.find_all('div', class_="rs-ctable-panel jsrs-resizerContainer")
        
        for element in mobile_subscription_elements:
            product_name = element.find('h3', class_="rs-ctable-panel-title").get_text().strip()
            price_per_month = element.find('span', class_="rs-unit").get_text()
            elms = element.find_all('li', class_="jsrs-resizerPart")
            mobile_data = elms[0].get_text().replace('GB','').strip()
            minutes = elms[2].get_text().split(' ')[0]
            sms = elms[1].get_text().replace('texting','').strip()
            sms = unlimited_check_to_float(sms)
            minutes = unlimited_check_to_float(minutes)
            
            mobile_subscription_data.append({
                'product_name': f"mobile_subscription_{product_name}",
                'competitor_name': 'scarlet',
                'product_category': 'mobile_subscription',
                'product_url': url,
                'price': float(price_per_month),
                'date' : date,
                'data': float(mobile_data),
                'network': None,
                'minutes': minutes,
                'price_per_minute': None,
                'sms': sms,
                'upload_speed': None,
                'download_speed': None
                    })

        return mobile_subscription_data

    except Exception as e:
        error_message = f"Error extracting mobile subscription data: {str(e)}"
        logging.error(error_message)
        traceback.print_exc()

def extract_options_data(page_content, url):
    options_data = []
    date = time.strftime("%Y-%m-%d")
    try:
        soup = BeautifulSoup(page_content, 'html.parser')
        options_data_element = soup.find('div', class_="rs-checkbox")
    
        option_info = options_data_element.get_text().replace('Option:','').strip()
        price = re.findall(r'€(\d+)', option_info)
        option_details = option_info.encode('ascii', 'ignore').decode('ascii').lower().strip()
        
        options_data.append({
            'product_category':'mobile_subscription',
            'option_name': "extra_internet",
            'option_details' : option_details,
            'date': date,
            'price' : float(price[0])
                })
        return options_data

    except Exception as e:
        print(f"Error extracting options data: {str(e)}")

  
def get_mobile_subscription_data(browser, url):
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()

    mobile_subscription_data = extract_mobile_subscription_data(page_content, url)

    page.close()
    return mobile_subscription_data

def get_options_data(browser, url):
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()
    
    options_data = extract_options_data(page_content, url)

    page.close()
    return options_data


def get_products(browser, url):
    start = time.time()
    mobile_subscription_data = get_mobile_subscription_data(browser, url)
    options_data = get_options_data(browser, url)
    product_list =[]
    options_list = []
    product_list.extend(mobile_subscription_data)
    options_list.extend(options_data)
    product_dict = {'products': product_list}
    options_dict = {'options' : options_list}
    end = time.time()
    print("Time taken to scrape products: {:.3f}s".format(end - start))
    return product_dict, options_dict


def main():    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try: 
            product_dict, options_dict = get_products(browser, URL)
            product_json = json.dumps(product_dict, indent=4)
            options_json = json.dumps(options_dict, indent=4)
            print(product_json)
            print(options_json)
        except Exception as e:
            print(f"Error in main function:{str(e)}")
        finally:
            browser.close()


if __name__ == "__main__":
    main()



