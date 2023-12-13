import functions_framework
import pandas as pd
from google.cloud import bigquery

schema_costs = [
            bigquery.SchemaField('costs', 'FLOAT'),
            bigquery.SchemaField('date', 'DATE')
        ]

schema_costs_detail = [
            bigquery.SchemaField('keyword', 'STRING'),
            bigquery.SchemaField('landing_page', 'STRING'),
            bigquery.SchemaField('channel', 'STRING'),
            bigquery.SchemaField('ad_group', 'STRING'),
            bigquery.SchemaField('medium', 'STRING'),
            bigquery.SchemaField('ad_content', 'STRING'),
            bigquery.SchemaField('location', 'STRING'),
            bigquery.SchemaField('campaign', 'STRING'),
            bigquery.SchemaField('cost', 'FLOAT'),
            bigquery.SchemaField('date', 'DATE')
        ]

schema_orders = [
            bigquery.SchemaField('orders', 'FLOAT'),
            bigquery.SchemaField('date', 'DATE')
        ]


schema_installs = [
            bigquery.SchemaField('channel', 'STRING'),
            bigquery.SchemaField('medium', 'STRING'),
            bigquery.SchemaField('campaign', 'STRING'),
            bigquery.SchemaField('keyword', 'STRING'),
            bigquery.SchemaField('ad_content', 'STRING'),
            bigquery.SchemaField('ad_group', 'STRING'),
            bigquery.SchemaField('landing_page', 'STRING'),
            bigquery.SchemaField('location', 'STRING'),
            bigquery.SchemaField('date', 'DATE')
        ]


def process_csv_and_upload_to_bigquery(csv_path, table_id, schem: list):
    df = pd.read_csv(csv_path)
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schem,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=','
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


@functions_framework.http
def add_data_bigquery(request):
    process_csv_and_upload_to_bigquery('gs://ata-set-market/costs.csv', 'testwork-407514.data_set_mark.costs', schema_costs)
    process_csv_and_upload_to_bigquery('gs://ata-set-market/costs_detail.csv', 'testwork-407514.data_set_mark.costs_detail', schema_costs_detail)
    process_csv_and_upload_to_bigquery('gs://ata-set-market/installs.csv', 'testwork-407514.data_set_mark.installs', schema_installs)
    process_csv_and_upload_to_bigquery('gs://ata-set-market/orders.csv', 'testwork-407514.data_set_mark.orders', schema_orders)
    return 'Successful'
