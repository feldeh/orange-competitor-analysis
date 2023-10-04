from pathlib import Path
import ndjson
import json


def save_to_json(dict_data, filename):
    json_path = Path.cwd() / f"data/raw_data/json/{filename}.json"
    json_data = json.dumps(dict_data, indent=4)
    with json_path.open(mode="w", encoding="utf-8") as f:
        f.write(json_data)

    print(f"JSON data saved to {json_path}")


def save_to_ndjson(list_data, filename):
    ndjson_path = Path.cwd() / f"data/raw_data/ndjson/{filename}.ndjson"
    with ndjson_path.open(mode="w", encoding="utf-8") as f:
        ndjson.dump(list_data, f)
    print(f"NDJSON data saved to {ndjson_path}")
