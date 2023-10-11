from playwright.sync_api import sync_playwright
import re
import time
from bs4 import BeautifulSoup
from datetime import date
import logging
import traceback
import requests
from airflow import AirflowException


from utils import save_to_json, check_request, save_scraping_log


URL = {
    'mobile_subscription': 'https://www.scarlet.be/en/abonnement-gsm.html',
    'option_mobile_subscription': 'https://www.scarlet.be/en/abonnement-gsm.html',
    'internet_subscription': 'https://www.scarlet.be/en/abonnement-internet.html',
    'packs': 'https://www.scarlet.be/en/packs.html'
}


def goto_page(browser, url):
    """function to open a page with the given url using a browser"""

    check_request(url)

    page = browser.new_page()
    logging.info(f"Navigating to URL: {url}")
    page.goto(url, wait_until='domcontentloaded')
    time.sleep(5)
    try:
        page.wait_for_selector('#onetrust-accept-btn-handler')
        page.query_selector('#onetrust-accept-btn-handler').click(force=True)
    except Exception as e:
        error_message = f"Accept cookie button error: {str(e)}"
        logging.error(error_message)
        raise AirflowException(error_message)

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
            product_name = element.find('h3', class_="rs-ctable-panel-title").get_text().strip().lower()
            price_per_month = element.find('span', class_="rs-unit").get_text()
            elms = element.find_all('li', class_="jsrs-resizerPart")
            mobile_data = elms[0].get_text().replace('GB', '').strip()
            minutes = elms[2].get_text().split(' ')[0]
            sms = elms[1].get_text().replace('texting', '').strip()
            sms = unlimited_check_to_float(sms)
            minutes = unlimited_check_to_float(minutes)

            mobile_subscription_data.append({
                'product_name': f"mobile_subscription_{product_name}",
                'competitor_name': 'scarlet',
                'product_category': 'mobile_subscription',
                'product_url': url,
                'price': float(price_per_month),
                'scraped_at': date,
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
        raise AirflowException(error_message)


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
                'scraped_at': time.strftime("%Y-%m-%d"),
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
        raise AirflowException(error_message)

    return internet_data


def extract_options_data(page_content, url):
    """function to extract options data for extra internet"""
    options_data = []
    date = time.strftime("%Y-%m-%d")
    try:
        soup = BeautifulSoup(page_content, 'html.parser')
        options_data_element = soup.find('div', class_="rs-checkbox")

        option_info = options_data_element.get_text().replace('Option:', '').strip()
        price = re.findall(r'€(\d+)', option_info)
        option_details = option_info.encode('ascii', 'ignore').decode('ascii').lower().strip()

        options_data.append({
            'product_category': 'mobile_subscription',
            'option_name': "extra_internet",
            'option_details': option_details,
            'price': float(price[0]),
            'scraped_at': date,
            'pack_name': None

        })
        return options_data

    except Exception as e:
        error_message = f"Error extracting options data: {str(e)}"
        print(error_message)
        logging.error(error_message)
        traceback.print_exc()
        raise AirflowException(error_message)


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


def get_products(browser, url):
    mobile_subscription_data = get_mobile_subscription_data(browser, url['mobile_subscription'])
    options_data = get_options_data(browser, url['option_mobile_subscription'])
    internet_subscription_data = get_internet_subscription_data(browser, url['internet_subscription'])
    options_dict_trio = get_options_streaming(browser, "https://www.scarlet.be/en/homepage/packs/trio_packs/trio_pack")
    options_dict__trio_mobile = get_options_streaming(browser, "https://www.scarlet.be/en/homepage/packs/all_packs_arc_dof/trio_mobile")
    tv_options = get_options_tv(browser, "https://www.scarlet.be/en/tv-digitale.html")

    packs_data_1 = scarlet_trio()
    packs_data_2 = scarlet_trio_mobile()

    product_list = []
    options_list = []
    packs_list = []
    product_list.extend(mobile_subscription_data)
    product_list.extend(internet_subscription_data)
    options_list.extend(options_data)
    options_list.extend(options_dict_trio)
    options_list.extend(options_dict__trio_mobile)
    options_list.extend(tv_options)
    packs_list.append(packs_data_1)
    packs_list.append(packs_data_2)

    product_dict = {'products': product_list}
    options_dict = {'options': options_list}
    packs_dict = {'packs': packs_list}
    return product_dict, options_dict, packs_dict


def scarlet_trio():
    packs = {}
    url = 'https://www.scarlet.be/en/packs/trio.html?#/OurTrio'

    session = requests.Session()
    response = session.get(url)
    time.sleep(5)
    soup = BeautifulSoup(response.content, "html.parser")
    internet_speed = soup.find_all("h3", class_="rs-mediabox-title")
    pack_name = soup.find("h1").get_text().lower().replace(' ', '_')
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
            upload_speed = int(upload_speed)

        elif 'Unlimited' in i.get_text():
            internet_data = -1

        else:
            continue

    pack_description = (f"{pack_desc} {pack_desc1} Internet info: Upload speed:{upload_speed}, Download speed:{download_speed}, Data:{internet_data}")

    today = date.today()
    today = today.strftime("%Y-%m-%d")

    packs['competitor_name'] = 'scarlet'
    packs['pack_name'] = pack_name
    packs['pack_url'] = url
    packs['pack_description'] = pack_description.encode('ascii', 'ignore').decode('ascii')
    packs['price'] = float(price)
    packs['scraped_at'] = today
    packs["mobile_product_name"] = None
    packs["internet_product_name"] = None

    return packs


def scarlet_trio_mobile():
    packs = {}

    url = 'https://www.scarlet.be/en/packs/trio-mobile.html?#/OurTrioMobile'

    session = requests.Session()
    response = session.get(url)
    time.sleep(5)
    soup = BeautifulSoup(response.content, "html.parser")
    internet_speed = soup.find_all("h3", class_="rs-mediabox-title")
    pack_name = soup.find("h1").get_text().lower().replace(' ', '_')
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
            upload_speed = int(upload_speed)

        elif 'Unlimited' in i.get_text():
            internet_data = -1
        else:
            continue

    pack_description = (f"{pack_desc} {pack_desc1} Internet info: Upload speed:{upload_speed}, Download speed:{download_speed}, Data:{internet_data}")

    today = date.today()
    today = today.strftime("%Y-%m-%d")

    packs['competitor_name'] = 'scarlet'
    packs['pack_name'] = pack_name
    packs['pack_url'] = url
    packs['pack_description'] = pack_description.encode('ascii', 'ignore').decode('ascii')
    packs['price'] = float(price)
    packs['scraped_at'] = today
    packs["mobile_product_name"] = None
    packs["internet_product_name"] = None

    return packs


def get_options_streaming(browser, url):
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()

    options_data = extract_options_streaming(page_content, url)

    page.close()
    return options_data


def extract_options_streaming(page_content, url):
    options_data = []
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    try:
        soup = BeautifulSoup(page_content, 'html.parser')
        pack_list = soup.find_all("span")
        for i in pack_list:
            if i.find_parent('a', href="#"):
                pack_name = i.get_text().lower().replace(' ', '_')
        detail_list = soup.find_all("div", class_="rs-sbox rs-sbox-with-topimage rs-sbox-with-extracontent")
        for i in detail_list:
            titles = i.find("span", class_="rs-sbox-title")
            details = i.find("span", class_="rs-sbox-block")
            price_unit = i.find("span", class_="rs-unit").get_text()
            price_decimal = i.find("span", class_="rs-decimal").get_text()
            option_name = titles.get_text().lower().replace(' ', '_')
            option_details = details.get_text().encode('ascii', 'ignore').decode('ascii')
            price = (price_unit + price_decimal)

            options_data.append({'product_category': None, 'options_name': option_name, 'options_details': option_details, 'price': float(price), 'scraped_at': today, 'pack_name': pack_name})

        return options_data
    except Exception as e:
        error_message = f"Error extracting options streaming data: {str(e)}"
        logging.error(error_message)
        traceback.print_exc()
        raise AirflowException(error_message)

def get_options_tv(browser, url):
    time.sleep(5)
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()

    options_data_tv = extract_options_tv(page_content, url)

    page.close()
    return options_data_tv


def extract_options_tv(page_content, url):
    options_data = []
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    try:
        soup = BeautifulSoup(page_content, 'html.parser')
        details = soup.find_all("div", class_="rs-panel-flex-cell-big rs-bg-grey2")

        for i in details:
            price = i.find(class_="rs-unit").get_text()
            titles = i.find(class_="rs-mediabox-title").get_text()
            detail = i.find(class_="jsrs-resizerPart").get_text()
            option_name = titles.lower().replace(' ', '_')
            option_details = detail.encode('ascii', 'ignore').decode('ascii')

            options_data.append({'product_category': None, 'options_name': option_name, 'options_details': option_details, 'price': float(price), 'scraped_at': today, 'pack_name': "scarlet_packs"})

        return options_data
    except Exception as e:
        error_message = f"Error extracting options tv data: {str(e)}"
        logging.error(error_message)
        traceback.print_exc()
        raise AirflowException(error_message)

def scarlet_scraper():
    with sync_playwright() as p:
        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        start_time_seconds = time.time()
        error_details = 'no error'

        log_file_name = 'test.log'
        log_file_path = f"logs/{log_file_name}"

        log_format = '%(asctime)s [%(levelname)s] - %(message)s'
        logging.basicConfig(filename=log_file_path, level=logging.INFO, format=log_format)
        logging.info(f"=========== scarlet_scraper start: {start_time} ===========")

        browser = p.chromium.launch(headless=True)
        try:
            product_dict, options_dict, packs_dict = get_products(browser, URL)
            # list_product = [product_dict]
            # list_options = [options_dict]
            # list_packs = [packs_dict]
            save_to_json(product_dict, 'scarlet', 'products')
            save_to_json(options_dict, 'scarlet', 'options')
            save_to_json(packs_dict, 'scarlet', 'packs')
            # save_to_ndjson(list_product, 'scarlet', 'products')
            # save_to_ndjson(list_options, 'scarlet', 'options')
            # save_to_ndjson(list_packs, 'scarlet', 'packs')

        except Exception as e:
            error_message = f"Error in scarlet_scraper function: {str(e)}"
            traceback.print_exc()
            raise AirflowException(error_message)
        finally:
            browser.close()

            end_time_seconds = time.time()
            execution_time_message = "scarlet_scraper execution time: {:.3f}s".format(end_time_seconds - start_time_seconds)
            logging.info(execution_time_message)

            end_time = time.strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"=========== scarlet_scraper end: {end_time} ===========")

            save_scraping_log(error_details, 'scarlet')