from pathlib import Path
import ndjson
import json
import logging
import requests
from airflow import AirflowException
import time


def save_to_json(dict_data, competitor, filename):
    json_path = Path(f"data/raw_data/{competitor}_{filename}.json")
    json_data = json.dumps(dict_data, indent=4)
    with open(json_path, mode="w", encoding="utf-8") as f:
        f.write(json_data)

    file_saved_message = f"{filename} JSON file saved | {json_path}"
    logging.info(file_saved_message)


def save_to_ndjson(list_data, competitor, filename):
    ndjson_path = Path(f"data/raw_data/{competitor}_{filename}.ndjson")
    with open(ndjson_path, mode="w", encoding="utf-8") as f:
        ndjson.dump(list_data, f)

    file_saved_message = f"{filename} NDJSON file saved | {ndjson_path}"
    logging.info(file_saved_message)



def check_request(url):
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


def save_scraping_log(error_details, competitor):

    status = 'success' if error_details == 'no error' else 'failed'
    log_entry = {
        "logs":
            [
                {
                    'competitor_name': competitor,
                    'scrape_date': time.strftime("%Y-%m-%d"),
                    'error_details': error_details,
                    'status': status
                }
            ]
    }
    save_to_json(log_entry, competitor, "log.json")