import google.cloud.bigquery as bq
from google.cloud.exceptions import NotFound
import time
import uuid
from utils import load_ndjson
from typing import Any, Dict, List, Tuple, Optional


def create_dataset_if_not_exist(client: bq.Client, project_id: str, dataset_id: str) -> None:
    """Create a BigQuery dataset if it does not exist.

    Args:
        client (bq.Client): A BigQuery client.
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the dataset to create.

    """
    dataset_ref = bq.DatasetReference(project_id, dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except NotFound:
        dataset = bq.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f'Dataset {dataset.dataset_id} created.')
        time.sleep(3)


def create_table_if_not_exist(client: bq.Client, project_id: str, dataset_id: str, tables: List[str], schemas: Dict[str, List[bq.SchemaField]]) -> None:
    """Create BigQuery tables if they do not exist based on provided schemas.

    Args:
        client (bq.Client): A BigQuery client.
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the dataset where tables will be created.
        tables (list): A list of table IDs to create.
        schemas (dict): A dictionary mapping table IDs to their corresponding schemas.

    """
    dataset_ref = bq.DatasetReference(project_id, dataset_id)

    for table_id in tables:
        table_ref = dataset_ref.table(table_id)
        try:
            client.get_table(table_ref)
            print(f"Table {table_id} already exists.")
        except NotFound:
            table = bq.Table(table_ref, schema=schemas[table_id])
            table = client.create_table(table)
            print(f'Table {table.table_id} created.')


def is_different_record(existing_record: Dict[str, Any], new_record: Dict[str, Any], ignored_keys: List[str]) -> bool:
    """Check if two records are different, ignoring specified keys.

    Args:
        existing_record (dict): The existing record.
        new_record (dict): The new record to compare.
        ignored_keys (list): Keys to ignore during the comparison.

    Returns:
        bool: True if records differ in keys not ignored, False otherwise.

    """
    for key in existing_record.keys():
        if key not in ignored_keys and existing_record[key] != new_record.get(key):
            return True
    return False


def get_existing_record(client: bq.Client, query: str) -> Optional[Dict[str, Any]]:
    """Retrieve the first record that matches a given SQL query.

    Args:
        client (bq.Client): A BigQuery client.
        query (str): A SQL query string to retrieve the record.

    Returns:
        dict: The first record found or None if no records are found.

    """
    try:
        query_job = client.query(query)
        results = next(iter(query_job.result()), None)
        if results:
            print("Query executed successfully")
            print('Query results: ', results)
            return dict(results.items())
        else:
            print("No records found.")
            return None
    except Exception as e:
        print(f"Error fetching existing record: {str(e)}")
        return None


def insert_rows(client: bq.Client, project_id: str, dataset_id: str, table_id: str, data_to_load: List[Dict[str, Any]]) -> None:
    """Insert rows into a BigQuery table.

    Args:
        client (bq.Client): A BigQuery client.
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table to insert data into.
        data_to_load (list): The list of data to load into the table.

    """
    errors = []
    try:
        table_ref = bq.DatasetReference(project_id, dataset_id).table(table_id)
        # Insert rows into BigQuery table without specifying row IDs, BigQuery will generate unique IDs
        errors = client.insert_rows_json(table_ref, data_to_load, row_ids=[None] * len(data_to_load))
    except Exception as e:
        print(f"Error inserting data: {str(e)}")
    if errors:
        print("Errors in insert_rows:")
        print(errors)
    else:
        print(f"Inserted {len(data_to_load)} rows into {table_id}.")


def load_packs_to_bq(client: bq.Client, project_id: str, dataset_id: str, competitor: str) -> None:
    """
    Load packs data to the BigQuery 'packs' table for a specified competitor.

    Args:
        client: A BigQuery client object.
        project_id: The ID of the Google Cloud project.
        dataset_id: The ID of the BigQuery dataset.
        competitor: The name of the competitor to load packs data for.
    """
    packs_data = load_ndjson(competitor, 'packs')

    packs_to_load = []

    for record in packs_data:
        new_data = {
            "competitor_name": record["competitor_name"],
            "pack_name": record["pack_name"],
            "pack_url": record["pack_url"],
            "pack_description": record["pack_description"],
            "price": record["price"],
            "scraped_at": record["scraped_at"],
            # "mobile_product_name": record["mobile_product_name"],
            # "internet_product_name": record["internet_product_name"],
        }

        get_packs_query = (f'SELECT * FROM `{dataset_id}.packs` WHERE competitor_name="{new_data["competitor_name"]}" AND pack_name="{new_data["pack_name"]}" LIMIT 1')
        existing_packs_record = get_existing_record(client, get_packs_query)
        if not existing_packs_record:
            packs_to_load.append(new_data)

    if packs_to_load != []:
        insert_rows(client, project_id, dataset_id, 'packs', packs_to_load)


def load_logs_to_bq(client: bq.Client, project_id: str, dataset_id: str, competitor: str) -> None:
    """
    Load logs data to the BigQuery 'logs' table for a specified competitor.

    Args:
        client: A BigQuery client object.
        project_id: The ID of the Google Cloud project.
        dataset_id: The ID of the BigQuery dataset.
        competitor: The name of the competitor to load logs data for.
    """
    logs_data = load_ndjson(competitor, 'logs')

    insert_rows(client, project_id, dataset_id, 'logs', logs_data)


def prepare_data_for_insertion(record: Dict[str, Any], competitor_uuid: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Prepare product, feature and price data for insertion into BigQuery from a given record.

    Args:
        record: A dictionary containing the product data.
        competitor_uuid: The UUID of the competitor.

    Returns:
        A tuple of three dictionaries: product_data, feature_data and price_data.
    """
    product_uuid = str(uuid.uuid4())
    feature_uuid = str(uuid.uuid4())
    price_uuid = str(uuid.uuid4())

    product_data = {
        "product_uuid": product_uuid,
        "product_name": record["product_name"],
        "competitor_uuid": competitor_uuid,
        "feature_uuid": feature_uuid,
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

    return product_data, feature_data, price_data


def load_products_to_bq(client: bq.Client, project_id: str, dataset_id: str, competitor: str) -> None:
    """
    Load products, features and prices data to the BigQuery tables for a specified competitor.
    Ensures that existing records are not duplicated.

    Args:
        client: A BigQuery client object.
        project_id: The ID of the Google Cloud project.
        dataset_id: The ID of the BigQuery dataset.
        competitor: The name of the competitor to load products data for.
    """
    new_data = load_ndjson(competitor, 'products')

    first_record = new_data[0]
    products_to_load = []
    features_to_load = []
    prices_to_load = []

    competitor_uuid = str(uuid.uuid4())

    get_competitor_query = (f'SELECT * FROM `{dataset_id}.competitors` WHERE competitor_name="{first_record["competitor_name"]}" LIMIT 1')
    existing_competitor_record = get_existing_record(client, get_competitor_query)

    # If the competitor doesn't exist, insert new competitor data with associated product, features and price data.
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

            product_data, feature_data, price_data = prepare_data_for_insertion(record, competitor_uuid)

            products_to_load.append(product_data)
            features_to_load.append(feature_data)
            prices_to_load.append(price_data)

        insert_rows(client, project_id, dataset_id, 'products', products_to_load)
        insert_rows(client, project_id, dataset_id, 'features', features_to_load)
        insert_rows(client, project_id, dataset_id, 'product_prices', prices_to_load)

        load_packs_to_bq(client, project_id, dataset_id, competitor)
        load_logs_to_bq(client, project_id, dataset_id, competitor)

        return

    competitor_uuid = existing_competitor_record['competitor_uuid']

    for record in new_data:

        product_data, feature_data, price_data = prepare_data_for_insertion(record, competitor_uuid)

        # Check if a product exist
        get_product_query = (f'SELECT * FROM `{dataset_id}.products` WHERE competitor_uuid="{competitor_uuid}" AND product_name="{product_data["product_name"]}" LIMIT 1')
        existing_product_record = get_existing_record(client, get_product_query)
        # If product doesn't exist, load product, feature and price
        if not existing_product_record:
            products_to_load.append(product_data)
            features_to_load.append(feature_data)
            prices_to_load.append(price_data)

        else:
            # Store the fetched product_uuid
            product_uuid = existing_product_record['product_uuid']
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

            # Store the fetched feature_uuid
            feature_uuid = existing_product_record['feature_uuid']
            price_data["feature_uuid"] = feature_uuid

            # Fetch the last record matching that feature_uuid and product price
            get_price_query = (f'SELECT * FROM `{dataset_id}.product_prices` WHERE feature_uuid="{feature_uuid}" ORDER BY scraped_at LIMIT 1')
            existing_price_record = get_existing_record(client, get_price_query)

            # If no price found insert price data with newly generated price_uuid
            if not existing_price_record:
                prices_to_load.append(price_data)

            else:
                ignored_keys = ['scraped_at', 'feature_uuid', 'price_uuid']
                # If new price, load the new price
                if is_different_record(existing_price_record, price_data, ignored_keys):
                    prices_to_load.append(price_data)

    if products_to_load:
        insert_rows(client, project_id, dataset_id, 'products', products_to_load)
    if features_to_load:
        insert_rows(client, project_id, dataset_id, 'features', features_to_load)
    if prices_to_load:
        insert_rows(client, project_id, dataset_id, 'product_prices', prices_to_load)
