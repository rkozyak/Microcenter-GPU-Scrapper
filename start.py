import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

print('Program started')

# Configurations
start_id = 654055
end_id = 672120
base_url = 'https://www.microcenter.com/product/{}/gpu'
file_path = 'discovered_gpus.csv'
non_gpu_file_path = 'discovered_non_gpus.csv'
error_404_file_path = '404_errors.csv'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

prebuilt_keywords = ['Laptop', 'Prebuilt']

# Initialize or load existing files
if os.path.exists(file_path):
    existing_df = pd.read_csv(file_path)
else:
    existing_df = pd.DataFrame(columns=['ID', 'Brand', 'Tab Title', 'Price'])

if os.path.exists(non_gpu_file_path):
    non_gpu_df = pd.read_csv(non_gpu_file_path)
else:
    non_gpu_df = pd.DataFrame(columns=['ID', 'Tab Title'])

if os.path.exists(error_404_file_path):
    error_404_df = pd.read_csv(error_404_file_path)
else:
    error_404_df = pd.DataFrame(columns=['ID'])

existing_skus = set(existing_df['ID'].astype(str))
non_gpu_skus = set(non_gpu_df['ID'].astype(str))
error_404_skus = set(error_404_df['ID'].astype(str))

# Function to get price and brand from site
def extract_price_and_brand(soup, website_id):
    class_name = f'ProductLink_{website_id}'
    price_element = soup.find('span', class_=class_name)
    if price_element:
        price = price_element.get('data-price', 'Not Available').strip()
        brand = price_element.get('data-brand', 'Unknown').strip()
        return price, brand
    return 'Not Available', 'Unknown'

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
            tab_brand = tab_title.split()[0] if tab_title else 'Unknown'
            
            if any(keyword in tab_title for keyword in prebuilt_keywords):
                print(f'{website_id}: Skipping prebuilt or laptop')
                non_gpu_entry = pd.DataFrame([{'ID': website_id, 'Tab Title': tab_title}])
                non_gpu_df = pd.concat([non_gpu_df, non_gpu_entry], ignore_index=True)
                non_gpu_df.to_csv(non_gpu_file_path, mode='a', header=not os.path.exists(non_gpu_file_path), index=False)
                continue
            
            if 'GPU' in tab_title or 'Graphics Card' in tab_title or 'NVIDIA' in tab_title or 'RTX' in tab_title:
                if str(website_id) not in existing_skus:
                    new_entry = {'ID': website_id, 'Brand': brand, 'Tab Title': tab_title, 'Price': price}
                    new_df = pd.DataFrame([new_entry])
                    new_df.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)
                    existing_skus.add(str(website_id))
                    print(f'GPU Found and saved: {tab_title}')
                else:
                    print(f'{website_id}: Already in the file')
            else:
                non_gpu_entry = pd.DataFrame([{'ID': website_id, 'Tab Title': tab_title}])
                non_gpu_df = pd.concat([non_gpu_df, non_gpu_entry], ignore_index=True)
                non_gpu_df.to_csv(non_gpu_file_path, mode='a', header=not os.path.exists(non_gpu_file_path), index=False)
                print(f'{website_id}: Not a GPU')
        
        elif response.status_code == 403:
            print(f'{website_id}: Error 403 Forbidden - waiting 10 minutes before retrying.')
            time.sleep(600)
        elif response.status_code == 404:
            print(f'{website_id}: Error 404')
            error_404_entry = pd.DataFrame([{'ID': website_id}])
            error_404_df = pd.concat([error_404_df, error_404_entry], ignore_index=True)
            error_404_df.to_csv(error_404_file_path, mode='a', header=not os.path.exists(error_404_file_path), index=False)
            error_404_skus.add(str(website_id))
        else:
            print(f'Error: Received status code {response.status_code} for SKU {website_id}.')
    
    except Exception as e:
        print(f'An error occurred while processing SKU {website_id}: {e}')

    # Sleep to avoid overwhelming the server, but skip if already in file
    if str(website_id) not in existing_skus and str(website_id) not in non_gpu_skus and str(website_id) not in error_404_skus:
        time.sleep(3)

print('Scraping completed.')
