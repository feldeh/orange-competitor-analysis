from bs4 import BeautifulSoup
import requests

session = requests.Session()
URL = 'https://www.proximus.be/en/id_cr_msub/personal/mobile/mobile-subscriptions.html'
response = session.get(URL)
soup = BeautifulSoup(response.text, "html.parser")

table_essential = soup.find("div",{"data-tms-id":"TMS_MOBILE_ESSENTIAL_TABLE"})
table_easy = soup.find("div",{"data-tms-id":"TMS_MOBILE_EASY_TABLE"})
table_maxi = soup.find("div",{"data-tms-id":"TMS_MOBILE_MAXI_TABLE"})
table_unlimited = soup.find("div",{"data-tms-id":"TMS_MOBILE_UNLIMITED_TABLE"})

data = {
        "Mobile_Essential":{
                "Data Allowance": table_essential.find("div", class_="rs-ctable-feature-title-big").text.strip(),
                "Features": [item.text.strip() for item in table_essential.find_all("ul", class_="rs-ctable-bulletlist")[0].find_all("li")],
                "Prices": table_essential.find("span", class_="rs-price-sm").get_text(strip=True)},
        "Mobile_Easy":{
                "Data Allowance": table_easy.find("div", class_="rs-ctable-feature-title-big").text.strip(),
                "Features": [item.text.strip() for item in table_easy.find_all("ul", class_="rs-ctable-bulletlist")[0].find_all("li")],
                "Prices": table_easy.find("span", class_="rs-price-promo").get_text(strip=True)},
        "Mobile_Maxi":{
                "Data Allowance": table_maxi.find("div", class_="rs-ctable-feature-title-big").text.strip(),
                "Features": [item.text.strip() for item in table_maxi.find_all("ul", class_="rs-ctable-bulletlist")[0].find_all("li")],
                "Prices": table_maxi.find("span", class_="rs-price-promo").get_text(strip=True)},
        "Mobile_Unlimited":{
                "Data Allowance": table_unlimited.find("div", class_="rs-ctable-feature-title-big").text.strip(),
                "Features": [item.text.strip() for item in table_unlimited.find_all("ul", class_="rs-ctable-bulletlist")[0].find_all("li")],
                "Prices": table_unlimited.find("span", class_="rs-price-promo").get_text(strip=True)}
        }

