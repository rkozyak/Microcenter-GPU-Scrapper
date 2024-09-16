import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

print('Program started')

# Configurations
start_id = 676370
end_id = 679000
base_url = 'https://www.microcenter.com/product/{}/gpu'
file_path = 'discovered_gpus.csv'
non_gpu_file_path = 'discovered_non_gpus.csv'
error_404_file_path = '404_errors.csv'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

prebuilt_keywords = ['Laptop', 'Prebuilt']

# Initialize or load existing files
def load_existing_skus(file_path):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        return set(df['ID'].astype(str))
    return set()

existing_skus = load_existing_skus(file_path)
non_gpu_skus = load_existing_skus(non_gpu_file_path)
error_404_skus = load_existing_skus(error_404_file_path)

# Function to get price, brand, and vendor from site
def extract_price_and_brand(soup, website_id):
    class_name = f'ProductLink_{website_id}'
    price_element = soup.find('span', class_=class_name)
    if price_element:
        price = price_element.get('data-price', 'Not Available').strip()
        brand = price_element.get('data-brand', 'Unknown').strip()
        return price, brand
    return 'Not Available', 'Unknown'

# Determine the vendor based on the item title
def determine_vendor(title):
    title = title.lower()
    if 'nvidia' in title or 'geforce' in title:
        return 'Nvidia'
    elif 'amd' in title or 'radeon' in title:
        return 'AMD'
    elif 'intel' in title:
        return 'Intel'
    return 'Unknown'

# Function to get GPU model from title
def extract_gpu_model(title):
    title = title.upper()
    models = [
        # nvidia
        '4060', '4070', '4080', '4090',
        '3050', '3060', '3070', '3080', '3090',
        '2060', '2070', '2080',
        '1630', '1650', '1660',
        '1010', '1030', '1050', '1060', '1070', '1080',

        #intel
        'A310', 'A380', 'A580', 'A750', 'A770',

        # amd
        '7600', '7700', '7800', '7900',
        '6300', '6400', '6500', '6600', '6650', '6700', '6750', '6800', '6900', '6950',
        '5300', '5500', '5600', '5700',
        'VEGA 56', 'VEGA 64',
        '520', '530', '540', '550', '560', '570', '580', '590',
        '430', '435', '450', '455', '460', '470', '480',
        '330', '340', '350', '360', '370', '370', '390',
        '220', '230', '235', '240', '250', '260', '270', '280', '290', '295',

        # other
        'QUADRO', 'TITAN', 'VEGA'
    ]
    for model in models:
        if model in title:
            return model
    return 'Unknown'

# Loop through SKUs
for website_id in range(start_id, end_id):
    if str(website_id) in existing_skus or str(website_id) in non_gpu_skus or str(website_id) in error_404_skus:
        # Skip already processed SKUs
        print(f'{website_id}: Already processed, skipping.')
        continue

    url = base_url.format(website_id)

    try:
        response = requests.get(url, headers=headers)
    
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            tab_title = soup.title.get_text(strip=True) if soup.title else ''
            price, brand = extract_price_and_brand(soup, website_id)
            vendor = determine_vendor(tab_title)
            gpu_model = extract_gpu_model(tab_title)
            
            if any(keyword in tab_title for keyword in prebuilt_keywords):
                print(f'{website_id}: Skipping prebuilt or laptop')
                non_gpu_entry = pd.DataFrame([{'ID': website_id, 'Tab Title': tab_title}])
                non_gpu_entry.to_csv(non_gpu_file_path, mode='a', header=not os.path.exists(non_gpu_file_path), index=False)
                non_gpu_skus.add(str(website_id))
                continue
            
            if 'Graphics Card' in tab_title:
                if str(website_id) not in existing_skus:
                    new_entry = {'ID': website_id, 'Price': price, 'Vendor': vendor, 'Brand': brand,  'Model': gpu_model, 'Tab Title': tab_title}
                    new_df = pd.DataFrame([new_entry])
                    new_df.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)
                    existing_skus.add(str(website_id))
                    print(f'GPU Found and saved: {tab_title}')
                else:
                    print(f'{website_id}: Already in the file')
            else:
                non_gpu_entry = pd.DataFrame([{'ID': website_id, 'Tab Title': tab_title}])
                non_gpu_entry.to_csv(non_gpu_file_path, mode='a', header=not os.path.exists(non_gpu_file_path), index=False)
                non_gpu_skus.add(str(website_id))
                print(f'{website_id}: Not a GPU')
        
        elif response.status_code == 403:
            print(f'{website_id}: Error 403 Forbidden - waiting 10 minutes before retrying.')
            time.sleep(600)
        elif response.status_code == 404:
            print(f'{website_id}: Error 404')
            error_404_entry = pd.DataFrame([{'ID': website_id}])
            error_404_entry.to_csv(error_404_file_path, mode='a', header=not os.path.exists(error_404_file_path), index=False)
            error_404_skus.add(str(website_id))
        else:
            print(f'Error: Received status code {response.status_code} for SKU {website_id}.')
    
    except Exception as e:
        print(f'An error occurred while processing SKU {website_id}: {e}')

    # Sleep to avoid timeout/bans
    if str(website_id) not in existing_skus and str(website_id) not in non_gpu_skus and str(website_id) not in error_404_skus:
        time.sleep(0.5)

print('Scraping completed.')

# Sort GPU CSV
def sort_csv(file_path):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        
        if 'Vendor' in df.columns and 'Model' in df.columns and 'Price' in df.columns:
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
            df = df.sort_values(by=['Vendor', 'Model', 'Price'])
            df.to_csv(file_path, index=False)
            print(f'Sorted and saved {file_path}')
        else:
            print(f'Required columns not found in {file_path}.')
    else:
        print(f'File {file_path} does not exist.')

sort_csv(file_path)