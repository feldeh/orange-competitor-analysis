import json

l = [
    {
        "product_name": "mobile_prepaid_data_1_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_prepaid",
        "product_url": "https://mobilevikings.be/en/offer/prepaid/",
        "price": "10",
        "data": "1",
        "network": "",
        "minutes": "25",
        "price_per_minute": "0.40",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_prepaid_data_4_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_prepaid",
        "product_url": "https://mobilevikings.be/en/offer/prepaid/",
        "price": "15",
        "data": "4",
        "network": "",
        "minutes": "50",
        "price_per_minute": "0.30",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_prepaid_data_6_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_prepaid",
        "product_url": "https://mobilevikings.be/en/offer/prepaid/",
        "price": "20",
        "data": "6",
        "network": "",
        "minutes": "67",
        "price_per_minute": "0.30",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_prepaid_data_8_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_prepaid",
        "product_url": "https://mobilevikings.be/en/offer/prepaid/",
        "price": "25",
        "data": "8",
        "network": "",
        "minutes": "100",
        "price_per_minute": "0.25",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_prepaid_voice_0.5_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_prepaid",
        "product_url": "https://mobilevikings.be/en/offer/prepaid/",
        "price": "10",
        "data": "0.5",
        "network": "",
        "minutes": "50",
        "price_per_minute": "0.20",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_prepaid_voice_2_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_prepaid",
        "product_url": "https://mobilevikings.be/en/offer/prepaid/",
        "price": "15",
        "data": "2",
        "network": "",
        "minutes": "100",
        "price_per_minute": "0.15",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_prepaid_voice_3_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_prepaid",
        "product_url": "https://mobilevikings.be/en/offer/prepaid/",
        "price": "20",
        "data": "3",
        "network": "",
        "minutes": "133",
        "price_per_minute": "0.15",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_prepaid_voice_4_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_prepaid",
        "product_url": "https://mobilevikings.be/en/offer/prepaid/",
        "price": "25",
        "data": "4",
        "network": "",
        "minutes": "200",
        "price_per_minute": "0.125",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_subscription_2_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_subscription",
        "product_url": "https://mobilevikings.be/en/offer/subscriptions/",
        "price": "10",
        "data": "2",
        "network": "4g",
        "minutes": "150",
        "price_per_minute": "",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_subscription_4_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_subscription",
        "product_url": "https://mobilevikings.be/en/offer/subscriptions/",
        "price": "12",
        "data": "4",
        "network": "4g",
        "minutes": "150",
        "price_per_minute": "",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_subscription_10_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_subscription",
        "product_url": "https://mobilevikings.be/en/offer/subscriptions/",
        "price": "15",
        "data": "10",
        "network": "5g",
        "minutes": "unlimited",
        "price_per_minute": "",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_subscription_20_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_subscription",
        "product_url": "https://mobilevikings.be/en/offer/subscriptions/",
        "price": "20",
        "data": "20",
        "network": "5g",
        "minutes": "unlimited",
        "price_per_minute": "",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "mobile_subscription_70_gb",
        "competitor_name": "mobile_viking",
        "product_category": "mobile_subscription",
        "product_url": "https://mobilevikings.be/en/offer/subscriptions/",
        "price": "29",
        "data": "70",
        "network": "5g",
        "minutes": "unlimited",
        "price_per_minute": "",
        "sms": "unlimited",
        "upload_speed": "",
        "download_speed": "",
        "line_type": ""
    },
    {
        "product_name": "unlimited_fast_internet",
        "competitor_name": "mobile_viking",
        "product_category": "internet_subscription",
        "product_url": "https://mobilevikings.be/en/offer/internet/",
        "price": "40",
        "data": "unlimited",
        "network": "",
        "minutes": "",
        "price_per_minute": "",
        "sms": "",
        "download_speed": "100mbps",
        "upload_speed": "40mbps",
        "line_type": "copper"
    },
    {
        "product_name": "unlimited_superfast_internet",
        "competitor_name": "mobile_viking",
        "product_category": "internet_subscription",
        "product_url": "https://mobilevikings.be/en/offer/internet/",
        "price": "40",
        "data": "unlimited",
        "network": "",
        "minutes": "",
        "price_per_minute": "",
        "sms": "",
        "download_speed": "100mbps",
        "upload_speed": "40mbps",
        "line_type": "copper"
    }
]

url = 'https://mobilevikings.be/en/offer/combo/'


def generate_packs(products_list, combo_advantage, url):
    try:
        packs_list = []

        mobile_products = [product for product in products_list if 'mobile' in product['product_name']]
        internet_products = [product for product in products_list if 'internet' in product['product_name']]

        for internet_product in internet_products:
            for mobile_product in mobile_products:
                
                mobile_price = mobile_product['price']
                internet_price = internet_product['price']
                price = float(mobile_price) + float(internet_price) - combo_advantage

                pack_name = f"{mobile_product['product_name']}_{internet_product['product_name']}"
                competitor_name = internet_product['competitor_name']

                packs_list.append(
                    {
                        'competitor_name': competitor_name,
                        'pack_name': pack_name,
                        'pack_url': url,
                        'price': price,
                        'mobile_price': mobile_price,
                        'internet_price': internet_price
                    })

        packs_dict = {'packs': packs_list}
        json_data = json.dumps(packs_dict, indent=4)

        print(json_data)
        print(len(packs_list))

        return packs_dict

    except Exception as e:
        error_message = f'Error generating packs: {str(e)}'
        print(error_message)


# Example usage:
number = 3  # Replace with your desired number
packs = generate_packs(l, number, url)
