# utils.py
import boto3
import os
import time

# AWS configuration
ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'my-storm-oracle-data'

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def clear_log_file(log_file):
    open(log_file, 'w').close()
    print("Log file cleared")

def clear_s3(bucket_name, s3_path_prefix, attribute, timeout=10):
    start_time = time.time()
 
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_path_prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        print(f"S3 {attribute} directory cleared")
    
            