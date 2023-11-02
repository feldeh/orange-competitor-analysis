from typing import List, Optional
from pydantic import BaseModel
from pydantic import ValidationError
import logging
import traceback
from airflow import AirflowException


class Product(BaseModel):
    product_name: str
    competitor_name: str
    product_category: str
    product_url: str
    price: float
    scraped_at: str
    data: float
    minutes: Optional[float]
    sms: Optional[int]
    upload_speed: Optional[str]
    download_speed: Optional[str]


class Products(BaseModel):
    products: Optional[List[Product]]


def validate_products(product_list) -> List[dict]:
    try:
        products_object = Products(products=product_list)
        products_dict = products_object.model_dump()
        return products_dict

    except ValidationError as validation_error:
        error_message = f"Validation error: {validation_error}"
        logging.error(error_message)
        traceback.print_exc()
        raise AirflowException(error_message)
