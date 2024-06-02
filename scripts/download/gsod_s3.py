import os
import requests
from bs4 import BeautifulSoup
import boto3
import logging
from utils import s3_client, BUCKET_NAME, clear_log_file, clear_s3

def download_gsod_data(year, bucket_name, s3_path_prefix):
    base_url = f'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year}/'
    
    response = requests.get(base_url)
    if response.status_code != 200:
        logging.error(f"Failed to access URL: {base_url}")
        return
    
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a')
    
    for link in links:
        href = link.get('href')
        if href.endswith('.csv'):
            file_url = f'{base_url}{href}'
            download_and_upload_file(file_url, year, href, bucket_name, s3_path_prefix)

def download_and_upload_file(url, year, file_name, bucket_name, s3_path_prefix):
    response = requests.get(url)
    if response.status_code == 200:
        # Temporarily download to local storage
        temp_file_path = f'/tmp/{file_name}'
        with open(temp_file_path, 'wb') as file:
            file.write(response.content)
        logging.info(f"Downloaded: {temp_file_path}")
        
        # Upload to S3
        s3_key = f'{s3_path_prefix}/{year}/{file_name}'
        s3_client.upload_file(temp_file_path, bucket_name, s3_key)
        logging.info(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")
        
        # Remove the temporary file
        os.remove(temp_file_path)
    else:
        logging.error(f"Failed to download file: {url}")

if __name__ == "__main__":
    years = range(2013, 2024)  # From 2013 to 2023
    s3_path_prefix = 'noaa/raw/gsod'  # S3 path prefix
    attribute = 'gsod'

    # Configure logging
    log_file = '/home/azureuser/Storm-Oracle/logs/download_gsod.log'
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Clear log file and S3 directory
    clear_log_file(log_file)
    clear_s3(BUCKET_NAME, s3_path_prefix, attribute)

    # Download and upload GSOD data
    for year in years:
        download_gsod_data(year, BUCKET_NAME, s3_path_prefix)
