import os
import requests
from bs4 import BeautifulSoup

def download_noaa_sst(year, month):
    base_url = 'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/'
    month_str = f'{year}{month:02d}'
    url = f'{base_url}{month_str}/'
    
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to access URL: {url}")
        return
    
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a')
    
    for link in links:
        href = link.get('href')
        if href.endswith('.nc'):
            file_url = f'{url}{href}'
            download_file(file_url, year, month, href)

def download_file(url, year, month, file_name):
    response = requests.get(url)
    if response.status_code == 200:
        output_dir = f'../../data/raw/noaa/sst/{year}/{month:02d}/'
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {file_path}")
    else:
        print(f"Failed to download file: {url}")

if __name__ == "__main__":
    years = [2023]  # 示例年份，可以根据需要添加更多年份
    months = range(1, 3)  # 示例月份，可以根据需要调整范围

    for year in years:
        for month in months:
            download_noaa_sst(year, month)
