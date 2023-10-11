import google.cloud.bigquery as bq
from google.cloud.exceptions import NotFound
import ndjson
import time
import uuid


def create_dataset_if_not_exist(client, project_id, dataset_id):
    dataset_ref = bq.DatasetReference(project_id, dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except NotFound:
        dataset = bq.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
        time.sleep(3)


def create_table_if_not_exist(client, project_id, dataset_id, tables, schemas):
    dataset_ref = bq.DatasetReference(project_id, dataset_id)

    for table_id in tables:
        table_ref = dataset_ref.table(table_id)

        try:
            client.get_table(table_ref)
            print(f"Table {table_id} already exists.")
        except NotFound:
            table = bq.Table(table_ref, schema=schemas[table_id])
            table = client.create_table(table)
            print('table {} created.'.format(table.table_id))


def is_different_record(existing_record, new_record, ignored_keys):
    """
    Compare two records. If they are different in any key except in ignored_keys, return True. Else return False.
    """
    for key in existing_record.keys():
        if key not in ignored_keys and existing_record[key] != new_record.get(key):
            return True

    return False


def get_existing_record(client, query):
    try:
        query_job = client.query(query)
        results = [dict(row.items()) for row in query_job.result()][0]
        print("Query executed successfully")
        return results

    except IndexError:
        print("No records found.")
        return None
    except Exception as e:
        print(f"Error fetching existing record: {str(e)}")
        return None


def insert_rows(client, project_id, dataset_id, table_id, data_to_load):
    errors = []
    try:
        table_ref = bq.DatasetReference(project_id, dataset_id).table(table_id)
        errors = client.insert_rows_json(table_ref, data_to_load, row_ids=[None] * len(data_to_load))
    except Exception as e:
        print(f"Error inserting data: {str(e)}")
    if errors != []:
        print("Errors in insert_rows:")
        print(errors)
    else:
        print(f"Inserted {len(data_to_load)} rows into {table_id}.")


def load_packs_to_bq(client, project_id, dataset_id):

    ndjson_file_path = f'data/cleaned_data/packs.ndjson'

    with open(ndjson_file_path, "rb") as source_file:
        packs_data = ndjson.load(source_file)

        packs_to_load = []

        for record in packs_data:
            pack_data = {
                "competitor_name": record["competitor_name"],
                "pack_name": record["pack_name"],
                "pack_url": record["pack_url"],
                "price": record["price"],
                "scraped_at": record["scraped_at"],
                "mobile_product_name": record["mobile_product_name"],
                "internet_product_name": record["internet_product_name"],
            }

            packs_to_load.append(pack_data)

        insert_rows(client, project_id, dataset_id, 'packs', packs_to_load)


def load_to_bq(client, project_id, dataset_id, table_names, table_schemas):

    create_dataset_if_not_exist(client, project_id, dataset_id)
    create_table_if_not_exist(client, project_id, dataset_id, table_names, table_schemas)

    ndjson_file_path = f'data/cleaned_data/products.ndjson'
    with open(ndjson_file_path, "rb") as source_file:
        new_data = ndjson.load(source_file)

        first_record = new_data[0]
        products_to_load = []
        features_to_load = []
        prices_to_load = []

        competitor_uuid = str(uuid.uuid4())

        get_competitor_query = (f'SELECT * FROM `{dataset_id}.competitors` WHERE competitor_name="{first_record["competitor_name"]}" LIMIT 1')
        existing_competitor_record = get_existing_record(client, get_competitor_query)
        if not existing_competitor_record:
            new_competitor = [
                {
                    "competitor_uuid": competitor_uuid,
                    "competitor_name": first_record["competitor_name"],
                    "created_at": first_record["scraped_at"],
                }
            ]
            insert_rows(client, project_id, dataset_id, 'competitors', new_competitor)

            for record in new_data:
                product_uuid = str(uuid.uuid4())
                feature_uuid = str(uuid.uuid4())
                price_uuid = str(uuid.uuid4())

                product_data = {
                    "product_uuid": product_uuid,
                    "product_name": record["product_name"],
                    "competitor_uuid": competitor_uuid,
                    "competitor_name": record["competitor_name"],
                    "scraped_at": record["scraped_at"],
                }
                products_to_load.append(product_data)

                feature_data = {
                    "feature_uuid": feature_uuid,
                    "product_uuid": product_uuid,
                    "product_name": record["product_name"],
                    "product_category": record["product_category"],
                    "product_url": record["product_url"],
                    "scraped_at": record["scraped_at"],
                    "data": record["data"],
                    "minutes": record["minutes"],
                    "sms": record["sms"],
                    "upload_speed": record["upload_speed"],
                    "download_speed": record["download_speed"]
                }
                features_to_load.append(feature_data)

                price_data = {
                    "price_uuid": price_uuid,
                    "feature_uuid": feature_uuid,
                    "price": record["price"],
                    "scraped_at": record["scraped_at"],
                }
                prices_to_load.append(price_data)

            insert_rows(client, project_id, dataset_id, 'products', products_to_load)
            insert_rows(client, project_id, dataset_id, 'features', features_to_load)
            insert_rows(client, project_id, dataset_id, 'prices', prices_to_load)

            return

        competitor_uuid = existing_competitor_record['competitor_uuid']

        for record in new_data:
            product_uuid = str(uuid.uuid4())
            feature_uuid = str(uuid.uuid4())
            price_uuid = str(uuid.uuid4())

            product_data = {
                "product_uuid": product_uuid,
                "product_name": record["product_name"],
                "competitor_uuid": competitor_uuid,
                "competitor_name": record["competitor_name"],
                "scraped_at": record["scraped_at"],
            }

            feature_data = {
                "feature_uuid": feature_uuid,
                "product_uuid": product_uuid,
                "product_name": record["product_name"],
                "product_category": record["product_category"],
                "product_url": record["product_url"],
                "scraped_at": record["scraped_at"],
                "data": record["data"],
                "minutes": record["minutes"],
                "sms": record["sms"],
                "upload_speed": record["upload_speed"],
                "download_speed": record["download_speed"]
            }

            price_data = {
                "price_uuid": price_uuid,
                "feature_uuid": feature_uuid,
                "price": record["price"],
                "scraped_at": record["scraped_at"],
            }

            # Check if product exist
            get_product_query = (f'SELECT * FROM `{dataset_id}.products` WHERE competitor_uuid="{competitor_uuid}" AND product_name="{product_data["product_name"]}" LIMIT 1')
            existing_product_record = get_existing_record(client, get_product_query)
            # To be loaded if product not exist
            if not existing_product_record:
                products_to_load.append(product_data)
                features_to_load.append(feature_data)
                prices_to_load.append(price_data)

            else:
                product_uuid = existing_product_record['product_uuid']

                # Keep the same product_uuid
                feature_data["product_uuid"] = product_uuid

                # Fetch last record from product feature
                get_feature_query = (f'SELECT * FROM `{dataset_id}.features` WHERE product_uuid="{product_uuid}" ORDER BY scraped_at LIMIT 1')
                existing_feature_record = get_existing_record(client, get_feature_query)

                if not existing_feature_record:
                    features_to_load.append(feature_data)
                    prices_to_load.append(price_data)

                else:
                    ignored_keys = ['scraped_at', 'product_uuid', 'feature_uuid']
                    # To be loaded if feature changed
                    if is_different_record(existing_feature_record, feature_data, ignored_keys):

                        features_to_load.append(feature_data)
                        prices_to_load.append(price_data)


                # Keep the same feature_uuid
                price_data["feature_uuid"] = feature_uuid

                # Fetch last record from product price
                get_price_query = (f'SELECT * FROM `{dataset_id}.prices` WHERE feature_uuid="{feature_uuid}" ORDER BY scraped_at LIMIT 1')
                existing_price_record = get_existing_record(client, get_price_query)

                if not existing_price_record:
                    prices_to_load.append(price_data)

                else:
                    ignored_keys = ['scraped_at', 'feature_uuid', 'price_uuid']
                    # To be loaded if price changed
                    if is_different_record(existing_price_record, price_data, ignored_keys):
                        prices_to_load.append(price_data)


        if products_to_load:
            insert_rows(client, project_id, dataset_id, 'products', products_to_load)
        if features_to_load:
            insert_rows(client, project_id, dataset_id, 'features', features_to_load)
        if prices_to_load:
            insert_rows(client, project_id, dataset_id, 'prices', prices_to_load)

        load_packs_to_bq(client, project_id, dataset_id)