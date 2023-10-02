from pathlib import Path


def save_to_json(json_data, filename):
    raw_data_path = f"data/raw_data/{filename}"
    with raw_data_path.open(mode="w", encoding="utf-8") as json_file:
        json_file.write(json_data)

    print(f"JSON data saved to {raw_data_path}")
