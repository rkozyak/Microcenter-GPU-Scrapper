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

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

prebuilt_keywords = ['Laptop', 'Prebuilt']

# Initialize or load existing file
if os.path.exists(file_path):
    existing_df = pd.read_csv(file_path)
else:
    existing_df = pd.DataFrame(columns=['ID', 'Brand', 'Tab Title', 'Price'])

existing_skus = set(existing_df['ID'].astype(str))

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
            elif 'GPU' in tab_title or 'Graphics Card' in tab_title or 'NVIDIA' in tab_title or 'RTX' in tab_title:
                if str(website_id) not in existing_skus:
                    new_entry = {'ID': website_id, 'Brand': brand, 'Tab Title': tab_title, 'Price': price}
                    # Append to the file immediately
                    new_df = pd.DataFrame([new_entry])
                    new_df.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)
                    existing_skus.add(str(website_id))
                    print(f'GPU Found and saved: {tab_title}')
                else:
                    print(f'{website_id}: Already in the file')
            else:
                print(f'{website_id}: Not a GPU')
        elif response.status_code == 404:
            print(f'{website_id}: Error 404')
        else:
            print(f'Error: Received status code {response.status_code} for SKU {website_id}. URL: {url}')
    
    except Exception as e:
        print(f'An error occurred while processing SKU {website_id}: {e}')

    # Sleep to avoid overwhelming the server
    time.sleep(3)

print('Scraping completed.')
