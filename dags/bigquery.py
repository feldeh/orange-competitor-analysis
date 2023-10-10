from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
import ndjson
import time

service_acc_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

bigquery_client = bigquery.Client.from_service_account_json(service_acc_key_path)


# TODO: create table with schema if table not exist
# TODO: compare source data with existing table data and load conditionally
# TODO: add logs table
# TODO: add price table
# TODO: add uuid?

BQ_TABLE_SCHEMAS = {
    "product": [
        # bigquery.SchemaField('product_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('product_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('product_category', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('product_url', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('price', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('date', 'DATETIME', mode='REQUIRED'),
        bigquery.SchemaField('data', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('minutes', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('sms', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('upload_speed', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('download_speed', 'FLOAT', mode='NULLABLE'),
        # bigquery.SchemaField('updated_at', 'TIMESTAMP', mode='REQUIRED', description='Date and time when the record was updated')
    ],

}


def bq_create_dataset(dataset_ref):
    try:
        bigquery_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
        time.sleep(3)


def bq_create_table(dataset_id, table_id, schema):
    dataset_ref = bigquery_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))
        time.sleep(3)


def is_different_record(existing_record, new_record):
    """
    Compare two records. If they are different in any field (except 'date'), return True. Otherwise, return False.
    """
    for field in existing_record.keys():
        print('field:', field)
        if field != 'date' and existing_record[field] != new_record[field]:
            return True
    return False

def get_existing_record(dataset_id, table_id, product_name, competitor_name):
    """
    Query an existing record from BQ by product_name and competitor_name. If it exists, return the record, otherwise, return None.
    """
    query = (f'SELECT * FROM `{dataset_id}.{table_id}` WHERE product_name="{product_name}" AND competitor_name="{competitor_name}" LIMIT 1')
    try:
        query_job = bigquery_client.query(query)
        print('dict row items: ', [dict(row.items()) for row in query_job.result()][0] )
        # comparison will be missed if feature changes on existing product_name, competitor_name
        # => first record will appear as different
        # => need to compare using uuid
        return [dict(row.items()) for row in query_job.result()][0]  # Return the first record as a dict
    except Exception as e:
        print("Error")
        print(e)
        return None

def load_json_to_bigquery(dataset_id, table_id, schema):

    dataset_ref = bigquery_client.dataset(dataset_id)

    bq_create_dataset(dataset_ref)
    bq_create_table(dataset_id, table_id, schema)

    ndjson_file_path = f'data/raw_data/ndjson/{table_id}.ndjson'

    # Load and check each record from ndjson
    with open(ndjson_file_path, "rb") as source_file:
        new_data = ndjson.load(source_file)
        print('new data:', new_data)
        data_to_load = []

        for record in new_data:
            print('record: ', record)
            # Get existing record in BQ if any
            existing_record = get_existing_record(dataset_id, table_id, record['product_name'], record['competitor_name'])
            print('existing_record:', existing_record)
            if existing_record:
                # If a difference exists (except date), append the new record to data_to_load
                print('is_different_record: ', is_different_record(existing_record, record))
                if is_different_record(existing_record, record):
                    data_to_load.append(record)
            else:
                # If no existing record, append the new record to data_to_load
                data_to_load.append(record)

        # If there are records to load, load them to BQ
        print('data_to_load: ', data_to_load)
        if data_to_load:
            # errors = bigquery_client.insert_rows_json(dataset_id + '.' + table_id, data_to_load, row_ids=[None] * len(data_to_load))
            try:
                table_ref = dataset_ref.table(table_id)
                errors = bigquery_client.insert_rows_json(table_ref, data_to_load, row_ids=[None] * len(data_to_load))
            except Exception as e:
                print(f"Error inserting data: {str(e)}")

            print('errors data load: ', errors)

            if errors != []:
                print("Errors:")
                print(errors)
            else:
                print(f"Inserted {len(data_to_load)} rows into {table_id}.")
        else:
            print("No new records to load.")

load_json_to_bigquery('test', 'test', BQ_TABLE_SCHEMAS['product'])



