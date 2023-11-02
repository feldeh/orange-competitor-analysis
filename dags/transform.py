import re
import json
import ndjson
from pathlib import Path
from typing import Optional, Union, List, Dict, Any


def convert_speed(speed: Optional[Union[int, float, str]]) -> Optional[int]:
    """Converts an internet speed string to its numerical value in Mbps.

    Args:
        speed (int, float, str): The speed as a string, int or float.

    Returns:
        int: The speed converted to Mbps or None if the conversion is not possible.
    """
    if speed is None:
        return None
    if isinstance(speed, (int, float)):
        return int(speed)
    if isinstance(speed, str):
        match = re.match(r'(\d+)(mbps|gbps)', speed, re.IGNORECASE)
        if not match:
            return None
        value, unit = match.groups()
        value = int(value)
        if unit.lower() == "gbps":
            value *= 1000
        return value


def json_to_list_of_dicts(competitor: str, header: str) -> Optional[List[Dict[str, Any]]]:
    """Reads a JSON file and returns a list of dictionaries.

    Args:
        competitor (str): The competitor's name.
        header (str): The header or category of the data.

    Returns:
        Optional[List[Dict[str, Any]]]: A list of dictionaries containing the data or None if file not found.
    """
    json_file_path = Path(f'data/raw_data/{competitor}_{header}.json')
    try:
        with open(json_file_path, 'r') as f:
            data_dict = json.load(f)
        return data_dict[header]
    except FileNotFoundError:
        print(f"File not found: {json_file_path}")
        return None


def clean_product_data(data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Cleans the product data by converting speed values to a consistent format.

    Args:
        data_list (list): The list of product data dictionaries to clean.

    Returns:
        list: The cleaned list of product data dictionaries.
    """
    for data_dict in data_list:
        data_dict['upload_speed'] = convert_speed(data_dict.get('upload_speed'))
        data_dict['download_speed'] = convert_speed(data_dict.get('download_speed'))
        for key, value in data_dict.items():
            if value is None:
                data_dict[key] = None
    return data_list


def list_of_dicts_to_ndjson(data_list: List[Dict[str, Any]], competitor: str, header: str) -> None:
    """Writes a list of dictionaries to a newline delimited JSON (ndjson) file.

    Args:
        data_list (list): The list of dictionaries to write to file.
        competitor (str): The competitor's name.
        header (str): The header or category of the data.
    """
    ndjson_file_path = Path(f'data/cleaned_data/{competitor}_{header}.ndjson')
    with open(ndjson_file_path, 'w') as f:
        ndjson.dump(data_list, f)


def clean_data_task(competitors: List[str], headers: List[str]) -> None:
    """Performs the cleaning task for each competitor and header combination.

    Args:
        competitors (list): A list of competitors.
        headers (list): A list of data headers/categories to clean.
    """
    for competitor in competitors:
        for header in headers:
            data_list = json_to_list_of_dicts(competitor, header)
            if data_list is None:
                continue
            if header == 'products':
                cleaned_data = clean_product_data(data_list)
                if cleaned_data is not None:
                    list_of_dicts_to_ndjson(cleaned_data, competitor, header)
            else:
                list_of_dicts_to_ndjson(data_list, competitor, header)
