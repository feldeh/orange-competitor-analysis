from google.cloud import bigquery
import os

service_acc_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

DATASET_ID = 'mobileviking'
TABLE_NAMES = ['_products', '_packs']

client = bigquery.Client.from_service_account_json(service_acc_key_path)


# TODO: function to check if a table exist, creates it if it doesn't


def load_json_to_bigquery(client, dataset_id, table_names):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )
    dataset_ref = client.dataset(dataset_id)

    for table_name in table_names:
        table_id = f'{table_name}_table'
        table_ref = dataset_ref.table(table_id)
        ndjson_file_path = f'data/raw_data/ndjson/{table_name}.ndjson'
        with open(ndjson_file_path, "rb") as source_file:
            table_ref = dataset_ref.table(table_id)
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()
        print(f"Loaded {job.output_rows} rows into {table_ref.path}")



load_json_to_bigquery(client, DATASET_ID, TABLE_NAMES)
