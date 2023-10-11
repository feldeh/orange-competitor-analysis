from pathlib import Path
import ndjson
import json
import logging


def save_to_json(dict_data, filename):
    json_path = f"data/{filename}.json"
    json_data = json.dumps(dict_data, indent=4)
    with open(json_path, mode="w", encoding="utf-8") as f:
        f.write(json_data)

    file_saved_message = f"{filename} JSON file saved | {json_path}"
    print(file_saved_message)
    logging.info(file_saved_message)


def save_to_ndjson(list_data, filename):
    ndjson_path = f"data/raw_data/ndjson/{filename}.ndjson"
    with open(ndjson_path, mode="w", encoding="utf-8") as f:
        ndjson.dump(list_data, f)

    file_saved_message = f"{filename} NDJSON file saved | {ndjson_path}"
    print(file_saved_message)
    logging.info(file_saved_message)
