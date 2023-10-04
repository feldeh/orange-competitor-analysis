import re
import requests
from bs4 import BeautifulSoup



URL = 'https://mobilevikings.be/en/offer/combo/'


response = requests.get(URL)

if response.status_code == 200:
    page_content = response.text
else:
    print(f"Failed to retrieve the web page url: {URL}")

soup = BeautifulSoup(page_content, "html.parser")


combo_text = soup.select_one('.monthlyPrice__discountMessage').get_text()

match = re.search(r'\d+', combo_text)

# Convert the extracted digits to an integer
if combo_text:
    combo_advantage = int(match.group())
else:
    combo_advantage = None


print(combo_advantage)

