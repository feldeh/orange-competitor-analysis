from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

service_acc_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

bigquery_client = bigquery.Client.from_service_account_json(service_acc_key_path)


BQ_TABLE_SCHEMAS = {
    "product_table": [
        # bigquery.SchemaField('product_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('product_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('product_category', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('product_url', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('price', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('date', 'DATETIME', mode='REQUIRED'),
        bigquery.SchemaField('data', 'FLOAT'),
        bigquery.SchemaField('minutes', 'FLOAT'),
        bigquery.SchemaField('sms', 'INTEGER'),
        bigquery.SchemaField('upload_speed', 'FLOAT'),
        bigquery.SchemaField('download_speed', 'FLOAT'),
        bigquery.SchemaField('updated_at', 'TIMESTAMP', mode='REQUIRED', description='Date and time when the record was updated')
    ],

}

def bq_create_dataset(dataset_ref):
    try:
        bigquery_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))

def bq_create_table(dataset_id, table_id, schema):
    dataset_ref = bigquery_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))



def load_json_to_bigquery(dataset_id, table_names):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )
    dataset_ref = bigquery_client.dataset(dataset_id)

    bq_create_dataset(dataset_ref)

    for table_name in table_names:
        table_id = f'{table_name}_table'
        table_ref = dataset_ref.table(table_id)



        # delete table content if exist
        # bigquery_client.delete_table(table_ref, not_found_ok=True)

        ndjson_file_path = f'data/raw_data/ndjson/{table_name}.ndjson'
        with open(ndjson_file_path, "rb") as source_file:
            table_ref = dataset_ref.table(table_id)
            job = bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()
        print(f"Loaded {job.output_rows} rows into {table_ref.path}")


def exist_record():

    query = ('SELECT * FROM `{}.{}.{}` WHERE product_category="{}" LIMIT 1'
            .format('arched-media-273319', 'mobileviking', 'products_table', 'mobile_prepaid'))

    try:
        query_job = bigquery_client.query(query)
        is_exist = len(list(query_job.result())) >= 1
        print('Exist id: {}'.format('my_selected_id') if is_exist else 'Not exist id: {}'.format('my_selected_id'))
        return is_exist
    except Exception as e:
        print("Error")
        print(e)

    return False
