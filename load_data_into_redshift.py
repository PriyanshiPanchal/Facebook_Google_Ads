import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

REDSHIFT_USER = 'your_redshift_user'
REDSHIFT_PASSWORD = 'your_redshift_password'
REDSHIFT_HOST = 'your_redshift_host'
REDSHIFT_PORT = '5439'
REDSHIFT_DB = 'your_redshift_db'
REDSHIFT_TABLE = 'your_redshift_table'

engine = create_engine(f'redshift+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}')


def transform_facebook_ads_data(json_data):
    ads_data = json.loads(json_data)

    transformed_data = []

    for ad in ads_data:
        transformed_entry = {
            "id": ad["id"],
            "name": ad["name"],
            "adset_id": ad["adset_id"],
            "campaign_id": ad["campaign_id"],
            "clicks": int(ad["clicks"]),
            "impressions": int(ad["impressions"]),
            "spend": float(ad["spend"]),
            "created_time": datetime.strptime(ad["created_time"], "%Y-%m-%dT%H:%M:%S%z"),
            "updated_time": datetime.strptime(ad["updated_time"], "%Y-%m-%dT%H:%M:%S%z")
        }
        transformed_data.append(transformed_entry)

    df = pd.DataFrame(transformed_data)
    return df

json_data = '''
[
    {
        "id": "123",
        "name": "Ad 1",
        "adset_id": "456",
        "campaign_id": "789",
        "clicks": "100",
        "impressions": "1000",
        "spend": "50.00",
        "created_time": "2023-01-01T00:00:00+0000",
        "updated_time": "2023-01-02T00:00:00+0000"
    },
    {
        "id": "124",
        "name": "Ad 2",
        "adset_id": "457",
        "campaign_id": "790",
        "clicks": "150",
        "impressions": "1500",
        "spend": "75.00",
        "created_time": "2023-01-01T00:00:00+0000",
        "updated_time": "2023-01-02T00:00:00+0000"
    }
]
'''

df = transform_facebook_ads_data(json_data)
print(df)

df.to_sql(REDSHIFT_TABLE, engine, index=False, if_exists='replace')

