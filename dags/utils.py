from pathlib import Path
import ndjson
import json
import logging
import requests
from airflow import AirflowException
import time
import os
from typing import Dict, List, Union, Optional


def save_to_json(dict_data: Dict, competitor: str, filename: str) -> None:
    json_path = Path(f"data/raw_data/{competitor}_{filename}.json")
    json_data = json.dumps(dict_data, indent=4)
    with open(json_path, mode="w", encoding="utf-8") as f:
        f.write(json_data)

    file_saved_message = f"{filename} JSON file saved | {json_path}"
    logging.info(file_saved_message)


def save_to_ndjson(list_data: List[Dict], competitor: str, filename: str) -> None:
    ndjson_path = Path(f"data/raw_data/{competitor}_{filename}.ndjson")
    with open(ndjson_path, mode="w", encoding="utf-8") as f:
        ndjson.dump(list_data, f)

    file_saved_message = f"{filename} NDJSON file saved | {ndjson_path}"
    logging.info(file_saved_message)


def unlimited_check_to_float(string: str) -> float:
    return -1 if string.lower() == 'unlimited' else float(string)


def check_request(url: str) -> None:
    """
    Check url request for errors
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

    except requests.exceptions.ConnectionError as ec:
        logging.error(ec)
        raise AirflowException(ec)
    except requests.exceptions.Timeout as et:
        logging.error(et)
        raise AirflowException(et)
    except requests.exceptions.HTTPError as eh:
        logging.error(eh)
        raise AirflowException(eh)


def save_scraping_log(error_details: str, competitor: str) -> None:

    status = 'success' if error_details == 'no error' else 'failed'
    log_entry = {
        "logs":
            [
                {
                    'competitor_name': competitor,
                    'scraped_at': time.strftime("%Y-%m-%d"),
                    'error_details': error_details,
                    'status': status
                }
            ]
    }
    save_to_json(log_entry, competitor, "logs")


def read_config_from_json(filename: str) -> Dict:
    with open(filename, 'r') as json_file:
        return json.load(json_file)


def check_empty_el(el: Optional[Union[str, List]], selector_name: str) -> None:
    if not el:
        raise Exception(f"Selector '{selector_name}' not found")


def load_ndjson(competitor: str, table_name: str) -> List[Dict]:
    file_path = Path(f'data/cleaned_data/{competitor}_{table_name}.ndjson')
    with open(file_path, "rb") as file:
        return ndjson.load(file)


def check_file_exists(competitor: str, table_name: str) -> bool:
    file_path = f'data/cleaned_data/{competitor}_{table_name}.ndjson'
    return os.path.isfile(file_path)
