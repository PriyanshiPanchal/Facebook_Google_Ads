import requests
import json
import psycopg2
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

# Replace these with your actual credentials and details
FB_ACCESS_TOKEN = 'your_facebook_access_token'
FB_AD_ACCOUNT_ID = 'act_your_facebook_ad_account_id'
DEVELOPER_TOKEN = 'your_developer_token'
CLIENT_ID = 'your_client_id'
CLIENT_SECRET = 'your_client_secret'
REFRESH_TOKEN = 'your_refresh_token'
LOGIN_CUSTOMER_ID = 'your_login_customer_id'
CUSTOMER_ID = 'your_customer_id'
RDS_HOST = 'your_rds_host'
RDS_PORT = 5432
RDS_DB_NAME = 'your_db_name'
RDS_USER = 'your_db_user'
RDS_PASSWORD = 'your_db_password'

def fetch_facebook_ads_data(access_token, ad_account_id):
    url = f"https://graph.facebook.com/v14.0/{ad_account_id}/ads"
    params = {
        'access_token': access_token,
        'fields': 'id,name,adset_id,campaign_id,clicks,impressions,spend,created_time,updated_time',
        'limit': 100
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Error fetching data: {response.text}")
    data = response.json()
    ads_data = data['data']
    while 'paging' in data and 'next' in data['paging']:
        response = requests.get(data['paging']['next'])
        if response.status_code != 200:
            raise Exception(f"Error fetching data: {response.text}")
        data = response.json()
        ads_data.extend(data['data'])
    return ads_data

def fetch_google_ads_data():
    google_ads_client = GoogleAdsClient.load_from_storage()
    query = '''
    SELECT
        campaign.id,
        ad_group.id,
        ad_group_ad.ad.id,
        ad_group_ad.ad.name,
        metrics.clicks,
        metrics.impressions,
        metrics.cost_micros
    FROM
        ad_group_ad
    WHERE
        segments.date DURING LAST_30_DAYS
    '''
    try:
        response = google_ads_client.service.google_ads.search(
            customer_id=CUSTOMER_ID, query=query)
        ads_data = []
        for row in response:
            ad_data = {
                'campaign_id': row.campaign.id.value,
                'ad_group_id': row.ad_group.id.value,
                'ad_id': row.ad_group_ad.ad.id.value,
                'ad_name': row.ad_group_ad.ad.name.value,
                'clicks': row.metrics.clicks.value,
                'impressions': row.metrics.impressions.value,
                'cost_micros': row.metrics.cost_micros.value
            }
            ads_data.append(ad_data)
        return ads_data
    except GoogleAdsException as ex:
        print(f'Request with ID "{ex.request_id}" failed with status "{ex.error.code().name}" and includes the following errors:')
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f'\t\tOn field: {field_path_element.field_name}')
        raise

def load_data_to_rds(data, table_name):
    conn = psycopg2.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        dbname=RDS_DB_NAME,
        user=RDS_USER,
        password=RDS_PASSWORD
    )
    cursor = conn.cursor()
    for record in data:
        columns = ', '.join(record.keys())
        values = ', '.join([f"'{v}'" for v in record.values()])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        cursor.execute(insert_query)
    conn.commit()
    cursor.close()
    conn.close()

def etl_facebook_ads():
    fb_ads_data = fetch_facebook_ads_data(FB_ACCESS_TOKEN, FB_AD_ACCOUNT_ID)
    load_data_to_rds(fb_ads_data, 'facebook_ads_table')

def etl_google_ads():
    google_ads_data = fetch_google_ads_data()
    load_data_to_rds(google_ads_data, 'google_ads_table')

def task_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
    subject = f"Airflow Task Failure: {dag_id}.{task_id}"
    html_content = f"""
    <p>Task failed in DAG: {dag_id}</p>
    <p>Task: {task_id}</p>
    <p>Execution Date: {execution_date}</p>
    <p>Log URL: <a href="{log_url}">{log_url}</a></p>
    """
    send_email('your_email@example.com', subject, html_content)

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'facebook_google_ads_etl',
    default_args=default_args,
    description='ETL pipeline for Facebook and Google Ads data',
    schedule_interval=timedelta(days=1),
)

# Task definitions
facebook_ads_etl_task = PythonOperator(
    task_id='etl_facebook_ads',
    python_callable=etl_facebook_ads,
    on_failure_callback=task_failure_callback,
    dag=dag,
)

google_ads_etl_task = PythonOperator(
    task_id='etl_google_ads',
    python_callable=etl_google_ads,
    on_failure_callback=task_failure_callback,
    dag=dag,
)

# Set task dependencies
facebook_ads_etl_task >> google_ads_etl_task
