## docker exec -it spark python3 /home/hadoop/workspace/bronze/insert_s3.py

import boto3
import json
from fakestore_api import FakeStoreAPI
from datetime import datetime, timedelta, timezone, date, time
import os
import io
import pandas as pd

today = datetime.now(timezone.utc).date()
month = f"{today.month:02d}"
year = f"{today.year}"
day = f"{today.day:02d}"

api = FakeStoreAPI()

def products_to_s3():
    products = api.get_products()
    return products

def categories_to_s3():
    categories = api.get_categories()
    
    return categories

def carts_to_s3():
    carts = api.get_carts()
    
    return carts

def get_minio_endpoint():
    if os.path.exists('/.dockerenv'):
        return "http://minio:9000"  # Dentro do container
    return "http://localhost:9000"  # Localmente

def insert_MinIO(s3, bucket, key, body):
    s3 = boto3.client(
        "s3",
        endpoint_url=get_minio_endpoint(),
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
    )
    # inserir parquet
    try:
        # Verifica se o arquivo tem dados antes de enviar
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
        )
        print(f"Inserido no MinIO com sucesso: {key}")
        return True
    except Exception as e:
        print(f"Erro ao inserir no MinIO: {e}")
        return False


def main():

    products = products_to_s3()
    categories = categories_to_s3()
    carts = carts_to_s3()

    insert_MinIO(products, "datalake", f"bronze/products/year={year}/month={month}/day={day}/products.json", json.dumps(products))
    insert_MinIO(categories, "datalake", f"bronze/categories/year={year}/month={month}/day={day}/categories.json", json.dumps(categories))
    insert_MinIO(carts, "datalake", f"bronze/carts/year={year}/month={month}/day={day}/carts.json", json.dumps(carts))

if __name__ == "__main__":
    main()