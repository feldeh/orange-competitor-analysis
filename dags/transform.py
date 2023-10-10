import pandas as pd
import re
import json
import ndjson


def convert_speed(speed):

    if pd.isnull(speed):
        return None
    match = re.match(r'(\d+)(mbps|gbps)', speed)

    if not match:
        return None
    value, unit = match.groups()
    value = int(value)

    if unit == "gbps":
        value *= 1000

    return value


def json_to_df(header):

    json_file_path = f'data/raw_data/json/{header}.json'
    with open(json_file_path, 'r') as f:
        data_dict = json.load(f)
    df = pd.DataFrame(data_dict[header])

    return df


def clean_product_data(df):

    df['upload_speed'] = df['upload_speed'].apply(convert_speed)
    df['download_speed'] = df['download_speed'].apply(convert_speed)

    return df



def df_to_ndjson(df, header):
    ndjson_data = df.to_dict(orient='records')
    with open(f'/tmp/cleaned_{header}.ndjson', 'w') as f:
    # with open(f'/tmp/cleaned_{header}.ndjson', 'w') as f:
        ndjson.dump(ndjson_data, f)


def clean_data_task(headers):

    for header in headers:
        df = json_to_df(header)
        if header == 'products':
            cleaned_df = clean_product_data(df)
            df_to_ndjson(cleaned_df, header)
            print(cleaned_df)
            continue
        # add cleanup for each header as needed

        df_to_ndjson(df, header)

        # df.to_csv(f'/tmp/cleaned_{header}.csv', index=False)
