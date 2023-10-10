from playwright.sync_api import sync_playwright
import json
import re
import time
from bs4 import BeautifulSoup
from datetime import date
import logging
import traceback
import requests





URL = {
    'mobile_subscription': 'https://www.scarlet.be/en/abonnement-gsm.html',
    'option_mobile_subscription': 'https://www.scarlet.be/en/abonnement-gsm.html',
    'internet_subscription': 'https://www.scarlet.be/en/abonnement-internet.html',
    'packs':'https://www.scarlet.be/en/packs.html'
}

def goto_page(browser, url):
    """function to open a page with the given url using a browser"""
    page = browser.new_page()
    page.goto(url, wait_until='domcontentloaded')
    time.sleep(5)
    try:
        page.wait_for_selector('#onetrust-accept-btn-handler')
        page.query_selector('#onetrust-accept-btn-handler').click(force=True)
    except Exception as e:
        error_message = f"Accept cookie button error: {str(e)}"
        logging.error(error_message)

    return page

def unlimited_check_to_float(string):
    """function to convert unlimited to -1 and string to float"""
    return -1 if string.lower() == 'unlimited' else float(string)

def extract_mobile_subscription_data(page_content, url):
 """extract mobile subscription data for three different categories from scarlet"""

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
                'minutes': minutes,
                'sms': sms,
                'upload_speed': None,
                'download_speed': None
                    })

        return mobile_subscription_data

    except Exception as e:
        error_message = f"Error extracting mobile subscription data: {str(e)}"
        logging.error(error_message)
        traceback.print_exc()

def extract_internet_table_data(page_content, url):
    """Extract internet subscription table from web page."""
    soup = BeautifulSoup(page_content, 'html.parser')

    try:
        internet_data = []
        tables = soup.find_all('div', class_='small-12 medium-6 large-6 columns')

        for table in tables:

            subscription_name = table.find('h3', class_='rs-ctable-panel-title').get_text().strip().lower()
            internet_volume = table.select_one('ul.rs-ctable-nobulletlist li:nth-of-type(1)').get_text().strip().replace(' Internet volume', '').replace(' GB', '').replace('Unlimited surfing', '-1')
            max_surfing_speed = table.select_one('ul.rs-ctable-nobulletlist li:nth-of-type(2)').get_text().replace('Max surfing speed of ', '').replace(' Mbps', '').strip()
            upload_speed = table.select_one('ul.rs-ctable-nobulletlist li:nth-of-type(3)').get_text().replace('Upload speed ', '').replace('of ', '').replace(' Mbps', '').strip()
            price_per_month = table.find('span', class_='rs-unit').get_text().strip()

            internet_data.append({
                'product_name': subscription_name + '_internet_subscription',
                'competitor_name': 'scarlet',
                'product_category': 'internet_subscription',
                'product_url': url,
                'price': float(price_per_month),
                'date': time.strftime("%Y-%m-%d"),
                'data': float(internet_volume),
                "minutes": None,
                "sms": None,
                'upload_speed': float(upload_speed),
                'download_speed': float(max_surfing_speed),
            })

    except Exception as e:
        error_message = f"Error extracting internet table data: {str(e)}"
        print(error_message)
        logging.error(error_message)

    return internet_data


def extract_options_data(page_content, url):
    """function to extract options data for extra internet"""
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


'''def extract_packs(page_content, url):'''



  
def get_mobile_subscription_data(browser, url):
    """function to load content from the page and extract data"""
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()
    logging.info(f"Extracting mobile subscription data from URL: {url}")
    mobile_subscription_data = extract_mobile_subscription_data(page_content, url)

    page.close()
    return mobile_subscription_data


def get_internet_subscription_data(browser, url):
    """function to load content from the page and extract data"""
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()
    logging.info(f"Extracting internet subscription data from URL: {url}")
    internet_subscription_data = extract_internet_table_data(page_content, url)
    page.close()

    return internet_subscription_data

def get_options_data(browser, url):
    """function to load content from the page and extract data"""
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()
    logging.info(f"Extracting options data from URL: {url}")
    options_data = extract_options_data(page_content, url)

    page.close()
    return options_data

'''def packs_data(browser, url):
    """function to load content from the page and extract data"""
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()
    logging.info(f"Extracting options data from URL: {url}")
    options_data = extract_packs_data(page_content, url)

    page.close()
    return packs_data'''


def get_products(browser, url):
    start = time.time()
    mobile_subscription_data = get_mobile_subscription_data(browser, url['mobile_subscription'])
    options_data = get_options_data(browser, url['option_mobile_subscription'])
    internet_subscription_data = get_internet_subscription_data(browser, url['internet_subscription'])
    packs_data = scarlet_trio()
    
    product_list =[]
    options_list = []
    packs_list = []
    product_list.extend(mobile_subscription_data)
    product_list.extend(internet_subscription_data)
    options_list.extend(options_data)
    packs_list.extend(packs_data)

    product_dict = {'products': product_list}
    options_dict = {'options' : options_list}
    packs_dict = {'packs' : packs_list}
    end = time.time()
    print("Time taken to scrape products: {:.3f}s".format(end - start))
    return product_dict, options_dict, packs_dict

def scarlet_trio():
    packs =  {}
    packprices = {}
    packs_data = [packs, packprices]
    url = 'https://www.scarlet.be/en/packs/trio.html?#/OurTrio'

    session = requests.Session()
    response = session.get(url)
    time.sleep(5)
    soup = BeautifulSoup(response.content, "html.parser")
    internet_speed = soup.find_all("h3", class_="rs-mediabox-title")
    pack_name = soup.find("h1").get_text()
    pack_description = soup.find_all("p")
    price = soup.find(class_="rs-unit").get_text()
    
    for i in pack_description:
        if i.find_parent(class_="panel rs-emphasis-light rs-usp"):
            pack_desc = i.get_text()
        elif i.find("strong"):
            pack_desc1 = i.get_text()
            break
        else:
            continue

    for i in internet_speed:
        if 'download' in i.get_text():
            download_speed = ''.join(filter(str.isdigit, i.get_text()))
            download_speed = int(download_speed)

        elif 'upload' in i.get_text():
            upload_speed = ''.join(filter(str.isdigit, i.get_text()))
            upload_speed =  int(upload_speed)

        elif 'Unlimited' in i.get_text():
            internet_data = "unlimited"

        else:
            continue
    
    pack_description = (f"{pack_desc} {pack_desc1} Internet info: Upload speed:{upload_speed}, Download speed:{download_speed}, Data:{internet_data}")

    today = date.today()
    today = today.strftime("%d/%m/%Y")        

    packs['competitor_id'] = 'scarlet'
    packs['pack_name'] = pack_name
    packs['pack_url'] = url
    packs['pack_description'] = pack_description
    packs['download_speed'] = "N/A"
    packs['upload_speed'] = "N/A"
    packprices['date'] = today
    packprices['price'] = float(price)
   
    
    return packs_data


def scarlet_trio_mobile():
    packs =  {}
    packprices = {}
    products = {}
    link = [packs, packprices, products]

    url = 'https://www.scarlet.be/en/packs/trio-mobile.html?#/OurTrioMobile'

    session = requests.Session()
    response = session.get(url)
    time.sleep(5)
    soup = BeautifulSoup(response.content, "html.parser")
    internet_speed = soup.find_all("h3", class_="rs-mediabox-title")
    pack_name = soup.find("h1").get_text()
    pack_description = soup.find_all("p")
    price = soup.find(class_="rs-unit").get_text()
    
    for i in pack_description:
        if i.find_parent(class_="panel rs-emphasis-light rs-usp"):
            pack_desc = i.get_text()
        elif i.find("strong"):
            pack_desc1 = i.get_text()
            break
        else:
            continue
    
    for i in internet_speed:
        if 'download' in i.get_text():
            download_speed = ''.join(filter(str.isdigit, i.get_text()))
            download_speed = int(download_speed)

        elif 'upload' in i.get_text():
            upload_speed = ''.join(filter(str.isdigit, i.get_text()))
            upload_speed =  int(upload_speed)

        elif 'Unlimited' in i.get_text():
            internet_data = "unlimited"
        else:
            continue
    
    info = soup.find_all("h3", class_="rs-tit4 rs-txt-c2 rs-padding-bottom1")
    for i in info:
        if "GB" in i.get_text():
            mobile_data = ''.join(filter(str.isdigit, i.get_text()))
            mobile_data = int(mobile_data)
        elif "minutes" in i.get_text():
            minutes = ''.join(filter(str.isdigit, i.get_text()))
            minutes = int(minutes)
        elif "SMS" in i.get_text():
            sms = i.get_text()
        else:
            continue
    
    product_url = soup.find("a", text="Cherry").get_text()

    pack_description = (f"{pack_desc} {pack_desc1} Internet info: Upload speed:{upload_speed}, Download speed:{download_speed}, Data:{internet_data}")
    
    today = date.today()
    today = today.strftime("%d/%m/%Y")

    packs['competitor_id'] = 'scarlet'
    packs['pack_name'] = pack_name
    packs['pack_url'] = url
    packs['pack_description'] = pack_description
    packprices['date'] = today
    packprices['price'] = float(price)
    products['product_name'] = "Cherry"
    products['product_category'] = "Mobile Subscription"
    products['product_url'] = product_url
    products['download_speed'] = "N/A"
    products['upload_speed'] = "N/A"
    products['minutes'] = minutes
    products['sms'] = sms
    products['data'] = mobile_data
    
    return link


def main():    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        try: 
            product_dict, options_dict = get_products(browser, URL)
            products_json = json.dumps(product_dict, indent=4)
            options_json = json.dumps(options_dict, indent=4)
            print(products_json)
            print(options_json)
        except Exception as e:
            print(f"Error in main function:{str(e)}")
        finally:
            browser.close()


if __name__ == "__main__":
    main()



