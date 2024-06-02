# sst.py
import os
import requests
from bs4 import BeautifulSoup
import logging
from utils import s3_client, BUCKET_NAME, clear_log_file, clear_s3

def download_noaa_sst(year, month, bucket_name, s3_path_prefix):
    base_url = 'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/'
    month_str = f'{year}{month:02d}'
    url = f'{base_url}{month_str}/'
    
    response = requests.get(url)
    if response.status_code != 200 or 'No objects found' in response.text:
        logging.error(f"No data available for {year}-{month:02d}.")
        return False
    
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a')
    
    if not links:
        logging.error(f"No data available for {year}-{month:02d}.")
        return False
    
    for link in links:
        href = link.get('href')
        if href.endswith('.nc'):
            file_url = f'{url}{href}'
            download_and_upload_file(file_url, year, month, href, bucket_name, s3_path_prefix)
    
    return True

def download_and_upload_file(url, year, month, file_name, bucket_name, s3_path_prefix):
    response = requests.get(url)
    if response.status_code == 200:
        # Temporarily download to local storage
        temp_file_path = f'/tmp/{file_name}'
        with open(temp_file_path, 'wb') as file:
            file.write(response.content)
        logging.info(f"Downloaded: {temp_file_path}")
        
        # Upload to S3
        s3_key = f'{s3_path_prefix}/{year}/{month:02d}/{file_name}'
        s3_client.upload_file(temp_file_path, bucket_name, s3_key)
        logging.info(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")
        
        # Remove the temporary file
        os.remove(temp_file_path)
    else:
        logging.error(f"Failed to download file: {url}")

if __name__ == "__main__":
    years = range(2013, 2024)  # From 2013 to 2023
    months = range(1, 13)  # Example months, adjust range as needed
    s3_path_prefix = 'noaa/raw/sst'  # S3 path prefix
    attribute = 'sst'

    # Configure logging
    log_file = '/home/azureuser/Storm-Oracle/logs/download_sst.log'
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Clear log file and S3 directory
    clear_log_file(log_file)
    clear_s3(BUCKET_NAME, s3_path_prefix, attribute)

    # Download and upload SST data
    for year in years:
        for month in months:
            if not download_noaa_sst(year, month, BUCKET_NAME, s3_path_prefix):
                break
