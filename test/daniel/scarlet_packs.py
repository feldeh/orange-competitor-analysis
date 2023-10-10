from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import requests
import json
import ndjson
import re
import time
from datetime import date


def scarlet_trio():
    packs =  {}
    packprices = {}
    link = [packs, packprices]
    url = 'https://www.scarlet.be/en/packs/trio.html?#/OurTrio'

    session = requests.Session()
    response = session.get(url)
    time.sleep(5)
    soup = BeautifulSoup(response.content, "html.parser")
    internet_speed = soup.find_all("h3", class_="rs-mediabox-title")
    pack_name = soup.find("h1").get_text().lower().replace(' ','_')
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
            internet_data = -1

        else:
            continue
    
    pack_description = (f"{pack_desc} {pack_desc1} Internet info: Upload speed:{upload_speed}, Download speed:{download_speed}, Data:{internet_data}")

    today = date.today()
    today = today.strftime("%Y-%m-%d")        

    packs['competitor_name'] = 'scarlet'
    packs['pack_name'] = pack_name
    packs['pack_url'] = url
    packs['pack_description'] = pack_description
    packprices['date'] = today
    packprices['price'] = float(price)
   
    
    return link


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
    pack_name = soup.find("h1").get_text().lower().replace(' ','_')
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
            internet_data = -1
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
    

    pack_description = (f"{pack_desc} {pack_desc1} Internet info: Upload speed:{upload_speed}, Download speed:{download_speed}, Data:{internet_data}")
    
    today = date.today()
    today = today.strftime("%Y-%m-%d")

    packs['competitor_name'] = 'scarlet'
    packs['pack_name'] = pack_name
    packs['pack_url'] = url
    packs['pack_description'] = pack_description
    packprices['date'] = today
    packprices['price'] = float(price)
    
    return link

def get_options_data(browser, url):
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()
    
    options_data = extract_options_data(page_content, url)

    page.close()
    return options_data

def goto_page(browser, url):
    page = browser.new_page()
    page.goto(url, wait_until='domcontentloaded')
    time.sleep(5)
    page.wait_for_selector('#onetrust-accept-btn-handler')
    page.query_selector('#onetrust-accept-btn-handler').click(force=True)
    time.sleep(5)
    return page

def extract_options_data(page_content, url):
    options_data = []
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    try:
        soup = BeautifulSoup(page_content, 'html.parser')
        pack_list = soup.find_all("span")
        for i in pack_list:
            if i.find_parent('a', href="#"):
                pack_name = i.get_text().lower().replace(' ','_')
            else:
                pack_name = "scarlet_mobile_trio"
        detail_list = soup.find_all("div", class_="rs-sbox rs-sbox-with-topimage rs-sbox-with-extracontent")
        for i in detail_list:
            titles = i.find("span", class_="rs-sbox-title")
            details = i.find("span", class_="rs-sbox-block")
            price_unit = i.find("span", class_="rs-unit").get_text()
            price_decimal = i.find("span", class_="rs-decimal").get_text()
            option_name = titles.get_text().lower().replace(' ','_')
            option_details = details.get_text()
            price = (price_unit + price_decimal)

            options_data.append({'product_category': 'N/A', 'options_name': option_name, 'options_details': option_details, 'price': float(price), 'date': today, 'pack_name': pack_name})
        
        
        return options_data
    except Exception as e:
        print(f"Error extracting options data: {str(e)}")

def get_options_tv(browser, url):
    time.sleep(5)
    page = goto_page(browser, url)
    time.sleep(5)
    page_content = page.content()
    
    options_data = extract_options_tv(page_content, url)

    page.close()
    return options_data

def extract_options_tv(page_content, url):
    options_data = []
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    try:
        soup = BeautifulSoup(page_content, 'html.parser')
        details = soup.find_all("div", class_="rs-panel-flex-cell-big rs-bg-grey2")
        print(details)
        for i in details:
            price = i.find(class_="rs-unit").get_text()
            titles = i.find(class_="rs-mediabox-title").get_text()
            detail = i.find(class_="jsrs-resizerPart").get_text()
            option_name = titles.lower().replace(' ','_')
            option_details = detail
        
            options_data.append({'product_category': 'N/A', 'options_name': option_name, 'options_details': option_details, 'price': float(price), 'date': today, 'pack_name': "scarlet_packs"})
        
        
        return options_data
    except Exception as e:
        print(f"Error extracting options data: {str(e)}")

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        time.sleep(5)
        try: 
            #options_dict = get_options_data(browser, "https://www.scarlet.be/en/homepage/packs/trio_packs/trio_pack")
            #options_dict_mobile = get_options_data(browser, "https://www.scarlet.be/en/homepage/packs/all_packs_arc_dof/trio_mobile")
            tv_options = get_options_tv(browser, "https://www.scarlet.be/en/tv-digitale.html")
        except Exception as e:
            print(f"Error in main function:{str(e)}")
        finally:
            browser.close()
            #pack_one = scarlet_trio()
            #pack_two = scarlet_trio_mobile()
            print(tv_options)


if __name__ == "__main__":
    main()