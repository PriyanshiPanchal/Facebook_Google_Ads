# Facebook and Google Ads ETL Pipeline

## Overview
This project is an ETL (Extract, Transform, Load) pipeline designed to fetch advertising data from Facebook Ads and Google Ads, process the data, and load it into an Amazon RDS PostgreSQL database. The ETL process is orchestrated using Apache Airflow.

## Requirements
To run this project, you will need:

- Python 3.7+
- Apache Airflow
- psycopg2
- Google Ads API Client Library for Python
- Requests Library

## Installation

1. **Clone the Repository**
    ```bash
    git clone https://github.com/PriyanshiPanchal/Facebook_Google_Ads.git
    cd facebook-google-ads-etl
    ```

2. **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

3. **Set Up Airflow**
    ```bash
    export AIRFLOW_HOME=~/airflow
    airflow db init
    airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@example.com
    ```

## Configuration
Replace the placeholder values in the script with your actual credentials and details.

```python
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
