import pandas as pd
from google.cloud.bigquery import SchemaField
import pandas as pd
import json


def generate_bigquery_schema(df):
    TYPE_MAPPING = {
        "i": "INTEGER",
        "u": "NUMERIC",
        "b": "BOOLEAN",
        "f": "FLOAT",
        "O": "STRING",
        "S": "STRING",
        "U": "STRING",
        "M": "TIMESTAMP",
    }
    schema = []
    for column, dtype in df.dtypes.items():
        val = df[column].iloc[0]
        mode = "REPEATED" if isinstance(val, list) else "NULLABLE"

        if isinstance(val, dict) or (mode == "REPEATED" and isinstance(val[0], dict)):
            fields = generate_bigquery_schema(pd.json_normalize(val))
        else:
            fields = ()

        type = "RECORD" if fields else TYPE_MAPPING.get(dtype.kind)
        schema.append(
            SchemaField(
                name=column,
                field_type=type,
                mode=mode,
                fields=fields,
            )
        )
    return schema


json_file_path = 'data/raw_data/json/products.json'

with open(json_file_path, 'r') as f:
    data_dict = json.load(f)

df = pd.DataFrame(data_dict['products'])

prod_price_df = df[['competitor_name', 'product_name', 'date', 'price']]

bq_schema = generate_bigquery_schema(prod_price_df)

print(bq_schema)
