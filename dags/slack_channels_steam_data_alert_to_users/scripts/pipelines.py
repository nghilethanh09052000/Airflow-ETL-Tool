import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
from airflow.hooks.base import BaseHook

GCP_CONN_ID = "sipher_gcp"

class BigQueryIngestionPipeline:
    def __init__(self):
        self.client = None
        self.project_id = 'sipher-data-platform'
        self.dataset_id = 'raw_steam_games'
        self.table_id_prefix = 'steam_game_lists_'

    def open_spider(self, spider):
        # Initialize BigQuery client
        key_path = BaseHook.get_connection(GCP_CONN_ID).extra_dejson["key_path"]
        self.client = bigquery.Client.from_service_account_json(key_path)

        current_date = datetime.now().strftime('%Y%m%d')
        self.table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id_prefix}{current_date}"

        dataset_ref = self.client.dataset(self.dataset_id, project=self.project_id)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception as e:
            spider.logger.info(f'Dataset not found, creating new one: {e}')
            self.client.create_dataset(dataset_ref)
        try:
            self.client.get_table(self.table_id)
        except Exception as e:
            spider.logger.info(f'Table not found, creating new one: {e}')
            table = bigquery.Table(self.table_id, schema=self.schema)
            self.client.create_table(table)

    def process_item(self, item, spider):
        row = {
            'steamId': item.get('steamId'),
            'id': item.get('id'),
            'name': item.get('name'),
            'price': item.get('price'),
            'reviews': item.get('reviews'),
            'followers': item.get('followers'),
            'reviewScore': item.get('reviewScore'),
            'copiesSold': item.get('copiesSold'),
            'revenue': item.get('revenue'),
            'avgPlaytime': item.get('avgPlaytime'),
            'tags': item.get('tags'),
            'genres': item.get('genres'),
            'features': item.get('features'),
            'developers': item.get('developers'),
            'publishers': item.get('publishers'),
            'unreleased': item.get('unreleased'),
            'earlyAccess': item.get('earlyAccess'),
            'releaseDate': item.get('releaseDate'),
            'EAReleaseDate': item.get('EAReleaseDate'),
            'publisherClass': item.get('publisherClass')
        }

        errors = self.client.insert_rows_json(self.table_id, [row])
        if errors:
            spider.logger.error(f'Failed to insert rows: {errors}')

        return item

    @property
    def schema(self):
        return [
            bigquery.SchemaField('steamId', 'INTEGER'),
            bigquery.SchemaField('id', 'INTEGER'),
            bigquery.SchemaField('name', 'STRING'),
            bigquery.SchemaField('price', 'FLOAT'),
            bigquery.SchemaField('reviews', 'INTEGER'),
            bigquery.SchemaField('followers', 'INTEGER'),
            bigquery.SchemaField('reviewScore', 'INTEGER'),
            bigquery.SchemaField('copiesSold', 'INTEGER'),
            bigquery.SchemaField('revenue', 'FLOAT'),
            bigquery.SchemaField('avgPlaytime', 'FLOAT'),
            bigquery.SchemaField('tags', 'STRING', mode='REPEATED'),
            bigquery.SchemaField('genres', 'STRING', mode='REPEATED'),
            bigquery.SchemaField('features', 'STRING', mode='REPEATED'),
            bigquery.SchemaField('developers', 'STRING', mode='REPEATED'),
            bigquery.SchemaField('publishers', 'STRING', mode='REPEATED'),
            bigquery.SchemaField('unreleased', 'BOOLEAN'),
            bigquery.SchemaField('earlyAccess', 'BOOLEAN'),
            bigquery.SchemaField('releaseDate', 'STRING'),
            bigquery.SchemaField('EAReleaseDate', 'STRING'),
            bigquery.SchemaField('publisherClass', 'STRING')
        ]
