
from playwright.sync_api import sync_playwright

URL = '127.0.0.1:4000'

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto(URL)
