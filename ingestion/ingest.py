import os
import io
import logging
from datetime import datetime

import requests
import pandas as pd
import boto3
from botocore.client import Config
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO configuration (endpoint is localhost because we run the script from the host machine)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "bronze"

# Frankfurter API (Free, no API key required)
API_URL = "https://api.frankfurter.app/latest?from=USD"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def fetch_data(url: str) -> dict:
    """
    Fetch data from API with retries (exponential backoff).
    """
    logger.info(f"Fetching data from {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def upload_to_minio(df: pd.DataFrame, bucket: str, key: str):
    """
    Upload Pandas DataFrame as Parquet to MinIO. Creates bucket if it doesn't exist.
    """
    logger.info(f"Preparing to upload data to MinIO: {bucket}/{key}")

    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # Check if bucket exists, create if not
    try:
        s3_client.head_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' already exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.info(f"Bucket '{bucket}' not found. Creating it...")
            s3_client.create_bucket(Bucket=bucket)
        else:
            logger.error(f"Error checking bucket: {e}")
            raise

    # Save DataFrame to an in-memory buffer
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)

    # Upload the buffer
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue()
    )
    logger.info("Upload successful")


def main():
    try:
        # 1. Fetch raw data
        data = fetch_data(API_URL)

        # 2. Simple transformation (flatten JSON to tabular format)
        # API returns: {"amount": 1.0, "base": "USD", "date": "2024-05-20", "rates": {"EUR": 0.92, "GBP": 0.78}}
        date = data.get("date")
        base_currency = data.get("base")
        rates = data.get("rates", {})

        records = []
        for target_currency, rate in rates.items():
            records.append({
                "date": date,
                "base_currency": base_currency,
                "target_currency": target_currency,
                "rate": rate,
                "ingested_at": datetime.utcnow().isoformat()
            })

        df = pd.DataFrame(records)
        logger.info(f"Successfully fetched {len(df)} records.")

        # 3. Load to MinIO (Bronze layer)
        # Partitioning path: rates/date=YYYY-MM-DD/rates_HH-MM-SS.parquet
        execution_time = datetime.utcnow().strftime("%H-%M-%S")
        object_key = f"rates/date={date}/rates_{execution_time}.parquet"

        upload_to_minio(df, BUCKET_NAME, object_key)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()