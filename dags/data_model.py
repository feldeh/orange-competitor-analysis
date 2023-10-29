from typing import List, Optional
from pydantic import BaseModel


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