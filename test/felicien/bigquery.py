from google.cloud import bigquery

import os


service_acc_key_path = os.environ.get("SERVICE_ACC_KEY_PATH")

project_id = 'arched-media-273319'
dataset_id = 'mobileviking'
table_id = 'test_table'
ndjson_file_path = 'data/raw_data/ndjson/products.ndjson'


client = bigquery.Client.from_service_account_json(service_acc_key_path)

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
)

with open(ndjson_file_path, "rb") as source_file:

    job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

job.result()

print(f"Loaded {job.output_rows} rows into {table_ref.path}")
