import re
import json
import ndjson
from pathlib import Path


def convert_speed(speed):
    if speed is None:
        return None
    if isinstance(speed, (int, float)):
        return speed
    if isinstance(speed, str):
        match = re.match(r'(\d+)(mbps|gbps)', speed)
        if not match:
            return None
        value, unit = match.groups()
        value = int(value)
        if unit == "gbps":
            value *= 1000

        return value


def json_to_list_of_dicts(competitor, header):
    json_file_path = Path(f'data/raw_data/{competitor}_{header}.json')
    try:
        with open(json_file_path, 'r') as f:
            data_dict = json.load(f)
        return data_dict[header]
    except FileNotFoundError:
        print(f"File not found: {json_file_path}")


def clean_product_data(data_list):
    for data_dict in data_list:
        data_dict['upload_speed'] = convert_speed(data_dict.get('upload_speed'))
        data_dict['download_speed'] = convert_speed(data_dict.get('download_speed'))
        for key, value in data_dict.items():
            if value is None:
                data_dict[key] = None
    return data_list


def list_of_dicts_to_ndjson(data_list, competitor,  header):
    with open(Path(f'data/cleaned_data/{competitor}_{header}.ndjson'), 'w') as f:
        ndjson.dump(data_list, f)


def clean_data_task(competitors, headers):
    for competitor in competitors:
        for header in headers:
            data_list = json_to_list_of_dicts(competitor, header)
            if data_list is None:
                print(f"No data for {competitor} {header}")
                continue
            if header == 'products':
                cleaned_data = clean_product_data(data_list)
                list_of_dicts_to_ndjson(cleaned_data, competitor, header)
                continue
            # add cleanup for each header as needed
            list_of_dicts_to_ndjson(data_list, competitor, header)
