from turtle import down
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import requests
import json
import ndjson
import re
import time
import datetime

URL = 'https://www.scarlet.be/en/packs.html'


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
    
    pack_description = (f"{pack_desc} + {pack_desc1}")

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
    
    packs['competitor_id'] = 'scarlet'
    packs['pack_name'] = pack_name
    packs['pack_url'] = url
    packs['pack_description'] = (f"{pack_description} Internet info: Upload speed:{upload_speed}, Download speed:{download_speed}, Data:{internet_data}")
    packs['download_speed'] = "N/A"
    packs['upload_speed'] = "N/A"
    packprices['date'] = datetime.datetime.now
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
    pack_description = (f"{pack_desc} + {pack_desc1}")
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
    

    packs['competitor_id'] = 'scarlet'
    packs['pack_name'] = pack_name
    packs['pack_url'] = url
    packs['pack_description'] = (f"{pack_description} Internet info: Upload speed:{upload_speed}, Download speed:{download_speed}, Data:{internet_data}")
    packprices['date'] = datetime.datetime.now
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
    pack_one = scarlet_trio()
    pack_two = scarlet_trio_mobile()
    print(pack_one, pack_two)


if __name__ == "__main__":
    main()