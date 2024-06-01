import boto3
import requests
import os



# AWS 配置
ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'my-storm-oracle-data'

# 初始化S3客戶端
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# Correct URL for the CSV file
url = "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/ibtracs.NA.list.v04r00.csv"

# Attempt to download the file
try:
    response = requests.get(url)
    response.raise_for_status()  # Will raise an HTTPError for bad responses
    data = response.content

    # Specify your S3 bucket and the desired S3 path
    bucket_name = 'my-storm-oracle-data'
    s3_path = 'path-to-your-folder/ibtracs.NA.list.v04r00.csv'

    # Upload to S3
    s3.put_object(Bucket=bucket_name, Key=s3_path, Body=data)
    print("File uploaded successfully.")

except requests.exceptions.HTTPError as err:
    print(f"HTTP Error: {err}")
except requests.exceptions.RequestException as err:
    print(f"Request Error: {err}")