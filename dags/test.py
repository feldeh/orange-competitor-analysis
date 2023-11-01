from data_model import Products
from playwright.sync_api import sync_playwright, Playwright
from bs4 import BeautifulSoup
import requests
import re
import time
import logging
import traceback
from typing import List, Dict
from airflow import AirflowException
from pydantic import ValidationError
from utils import *


product_list = [{
            "product_name": "unlimited_fast_internet",
            "competitor_name": "mobileviking",
            "product_category": "internet_subscription",
            "product_url": "https://mobilevikings.be/en/offer/internet/",
            "price": 40.0,
            "scraped_at": "2023-10-29",
            "data": -1.0,
            "minutes": 1,
            "sms": 1,
            "upload_speed": "40mbps",
            "download_speed": "100mbps"
        },
        {
            "product_name": "unlimited_superfast_internet",
            "competitor_name": "mobileviking",
            "product_category": "internet_subscription",
            "product_url": "https://mobilevikings.be/en/offer/internet/",
            "price": 55.0,
            "scraped_at": "2023-10-29",
            "data": -1.0,
            "minutes": 1,
            "sms": 1,
            "upload_speed": "500mbps",
            "download_speed": "1gbps"
        }]


def validate_products(product_list) -> List[dict]:
    try:
        products_object = Products(products=product_list)
        products_dict = products_object.model_dump()
        return products_dict['products']

    except ValidationError as validation_error:
        error_message = f"Validation error: {validation_error}"
        logging.error(error_message)
        traceback.print_exc()
        raise AirflowException(error_message)

validate_products(product_list)