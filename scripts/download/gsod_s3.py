# download_gsod_data.py
import os
import requests
from bs4 import BeautifulSoup
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
            if is_us_file(file_url, href):
                download_and_upload_file(file_url, year, href, bucket_name, s3_path_prefix)

def is_us_file(file_url, file_name):
    response = requests.get(file_url)
    if response.status_code == 200:
        temp_file_path = f'/tmp/temp.csv'
        with open(temp_file_path, 'wb') as file:
            file.write(response.content)
        with open(temp_file_path, 'r') as file:
            # Skip the header line
            file.readline()
            second_line = file.readline()
            # Split the line by commas and check if the last element is 'US'
            if second_line:
                fields = second_line.split(',')
                if len(fields) > 6:
                    # Joining the last two fields to handle cases where the NAME field has a comma
                    name_field = fields[5].strip() + ', ' + fields[6].strip()
                    logging.info(f"Checking file {file_name}, NAME field: {name_field}")
                    if fields[6].endswith("US\""):
                        return True
                    else:
                        logging.info(f"Skipped {file_name}: Not a US station")
    else:
        logging.info(f"Could not access file: {file_url}")
    return False

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
        logging.info(f'Uploaded to S3: s3://{bucket_name}/{s3_key}')
        
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