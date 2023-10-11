import google.cloud.bigquery as bq
from google.cloud.exceptions import NotFound
import os
import ndjson
import time
import uuid


service_acc_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

bq_client = bq.Client.from_service_account_json(service_acc_key_path)


# TODO: create table with schema if table not exist
# TODO: compare source data with existing table data and load conditionally
# TODO: add logs table
# TODO: add price table
# TODO: add uuid?

BQ_TABLE_SCHEMAS = {
    # this table should contain immutable data
    "competitor": [
        bq.SchemaField('competitor_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('created_at', 'DATETIME', mode='REQUIRED'),

    ],
    "product": [
        bq.SchemaField('product_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('competitor_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('created_at', 'DATETIME', mode='REQUIRED'),


    ],
    "feature": [
        bq.SchemaField('feature_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_name', 'STRING', mode='REQUIRED'),
        # bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_category', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_url', 'STRING', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),
        bq.SchemaField('data', 'FLOAT', mode='NULLABLE'),
        bq.SchemaField('minutes', 'FLOAT', mode='NULLABLE'),
        bq.SchemaField('sms', 'INTEGER', mode='NULLABLE'),
        bq.SchemaField('upload_speed', 'FLOAT', mode='NULLABLE'),
        bq.SchemaField('download_speed', 'FLOAT', mode='NULLABLE'),

    ],
    "price": [
        bq.SchemaField('price_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('feature_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('price', 'FLOAT', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),
        bq.SchemaField('data', 'FLOAT', mode='NULLABLE'),
    ],

}


def bq_create_dataset(dataset_id):
    dataset_ref = bq.DatasetReference(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bq.Dataset(dataset_ref)
        dataset = bq_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
        time.sleep(3)


# def bq_create_table(dataset_id, table_id, schema):
#     dataset_ref = bq.DatasetReference(dataset_id)

#     table_ref = dataset_ref.table(table_id)

#     try:
#         bq_client.get_table(table_ref)
#     except NotFound:
#         table = bq.Table(table_ref, schema=schema)
#         table = bq_client.create_table(table)
#         print('table {} created.'.format(table.table_id))
#         time.sleep(3)

def bq_create_table(dataset_id, tables, schemas):
    dataset_ref = bq.DatasetReference(dataset_id)

    for table_id in tables:
        table_ref = dataset_ref.table(table_id)

        try:
            bq_client.get_table(table_ref)
        except NotFound:
            table = bq.Table(table_ref, schema=schemas[table_id])
            table = bq_client.create_table(table)
            print('table {} created.'.format(table.table_id))


def is_different_record(existing_record, new_record):
    """
    Compare two records. If they are different in any field (except 'date'), return True. Otherwise, return False.
    """
    for field in existing_record.keys():
        print('field:', field)
        if field != 'date' and existing_record[field] != new_record[field]:
            return True
    return False

# def get_existing_record(dataset_id, table_id, record_uuid):
#     """
#     Query an existing record from BQ by product_name and competitor_name. If it exists, return the record, otherwise, return None.
#     """
#     query = (f'SELECT * FROM `{dataset_id}.{table_id}` WHERE product_uuid="{record_uuid}" LIMIT 1')
#     try:
#         query_job = bq_client.query(query)
#         print('dict row items: ', [dict(row.items()) for row in query_job.result()][0] )
#         # comparison will be missed if feature changes on existing product_name, competitor_name
#         # => first record will appear as different
#         # => need to add product feature table with reference to product table
#         return [dict(row.items()) for row in query_job.result()][0]  # Return the first record as a dict
#     except Exception as e:
#         print("Error")
#         print(e)
#         return None

def get_existing_record(query):
    try:
        query_job = bq_client.query(query)

        print('query job result: ', query_job.result())

        print('existing record: ', [dict(row.items()) for row in query_job.result()][0])

        return [dict(row.items()) for row in query_job.result()][0]
    except Exception as e:
        print("Error")
        print(e)
        return None

def load_json_to_bigquery(dataset_id, table_id, schema):

    dataset_ref = bq.DatasetReference(dataset_id)

    bq_create_dataset(dataset_id)
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
            existing_record = get_existing_record(dataset_id, table_id, record['product_uuid'])
            print('existing_record:', existing_record)
            if existing_record:
                # If a difference exists (except date), append the new record to data_to_load
                print('is_different_record: ', is_different_record(existing_record, record))
                if is_different_record(existing_record, record):
                    data_to_load.append(record)
            else:
                # If no existing record, append the new record to data_to_load
                record['uuid'] = str(uuid.uuid4())
                data_to_load.append(record)

        # If there are records to load, load them to BQ
        print('data_to_load: ', data_to_load)
        if data_to_load:
            try:
                table_ref = dataset_ref.table(table_id)
                errors = bq_client.insert_rows_json(table_ref, data_to_load, row_ids=[None] * len(data_to_load))
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

#load_json_to_bigquery('new_test', 'new_test', BQ_TABLE_SCHEMAS['product'])





DATASET_ID = 'dataset'

TABLES_ID = ['competitor', 'product', 'feature', 'price']

bq_create_dataset(DATASET_ID)
bq_create_table(DATASET_ID, TABLES_ID, BQ_TABLE_SCHEMAS)


ndjson_file_path = f'data/raw_data/ndjson/test.ndjson'
with open(ndjson_file_path, "rb") as source_file:
    new_data = ndjson.load(source_file)
    print(new_data)
    first_record = new_data[0]
    product_table = []
    feature_table = []
    price_table = []

    dataset_ref = bq.DatasetReference(DATASET_ID)


    get_competitor_query = (f'SELECT * FROM `{DATASET_ID}.competitor` WHERE competitor_name="{first_record["competitor"]}" LIMIT 1')
    existing_competitor_record = get_existing_record(get_competitor_query)
    if not existing_competitor_record:
        competitor_uuid = str(uuid.uuid4())
        new_competitor = [
            {
            "competitor_uuid": competitor_uuid,
            "competitor_name": first_record["competitor_name"],
            "created_at": first_record["date"],
            }
        ]
        try:
            table_ref = dataset_ref.table('competitor')
            errors = bq_client.insert_rows_json(table_ref, new_competitor)
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
        if errors != []:
            print("Errors:")
            for error in errors:
                print(error.reason)
        else:
            print(f"Inserted {first_record['competitor_name']} into competitor table.")

        







        for record in new_data:
            get_product_query = (f'SELECT * FROM `{DATASET_ID}.product` WHERE product_name="{record["product"]}" LIMIT 1')
            existing_product_record = get_existing_record(get_product_query)

            if existing_product_record:
                pass


            product = {
                # # "product_uuid": product_uuid,
                "product_name": record["product_name"],
                "competitor_name": record["competitor_name"],
                # # "competitor_uuid": competitor_uuid,
                "created_at": record["created_at"],
            }
            product_table.append(product)

            feature = {
                # # "feature_uuid": feature_uuid,
                # # "product_uuid": product_uuid,
                "product_name": record["product_name"],
                "product_category": record["product_category"],
                "product_url": record["product_url"],
                "scraped_at": record["date"],  # Assuming date corresponds to scraped_at
                "data": record["data"],
                "minutes": record["minutes"],
                "sms": record["sms"],
                "upload_speed": record["upload_speed"],
                "download_speed": record["download_speed"],
            }
            feature_table.append(feature)

            price = {
                # # "price_uuid": price_uuid,
                # # "feature_uuid": feature_uuid,
                "price": record["price"],
                "scraped_at": record["date"],  # Assuming date corresponds to scraped_at
                "data": record.get("data"),  # Using get to handle nullable fields
            }
            price_table.append(price)