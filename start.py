import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

print('Program started')

# config stuff
start_id = 678427 - 1
end_id = 678427 + 2

base_url = 'https://www.microcenter.com/product/{}/gpu'

file_path = 'discovered_gpus.csv'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# do not consider these items
prebuilt_keywords = ['Laptop', 'Prebuilt']

# reads existing file and does not save existing SKUs
if os.path.exists(file_path):
    existing_df = pd.read_csv(file_path)
else:
    existing_df = pd.DataFrame(columns=['SKU', 'Website ID', 'Tab Title', 'Price', 'URL'])

existing_skus = set(existing_df['SKU'].astype(str))

discovered_gpus = []

# get price and brand from site
def extract_price_and_brand(soup, website_id):
    class_name = f'ProductLink_{website_id}'
    price_element = soup.find('span', class_=class_name)
    if price_element:
        price = price_element.get('data-price', 'Not Available').strip()
        brand = price_element.get('data-brand', 'Unknown').strip()
        return price, brand
    return 'Not Available', 'Unknown'

# loop though SKUs
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
                print(f'Skipping prebuilt or laptop: {tab_title}')
            elif 'GPU' in tab_title or 'Graphics Card' in tab_title or 'NVIDIA' in tab_title or 'RTX' in tab_title:
                if str(website_id) not in existing_skus:
                    discovered_gpus.append({'SKU': website_id, 'Brand': brand, 'Tab Title': tab_title, 'Price': price, 'URL': url})
                    existing_skus.add(str(website_id))
                    print(f'GPU Found: {tab_title}')
                else:
                    print(f'{website_id} is already in the file')
            else:
                print(f'{website_id} is not a GPU')
        elif response.status_code == 404:
            print(f'Error 404: Page not found for SKU {website_id}. URL: {url}')
        else:
            print(f'Error: Received status code {response.status_code} for SKU {website_id}. URL: {url}')
    
    except Exception as e:
        print(f'An error occurred while processing SKU {website_id}: {e}')

    # wait between searches
    time.sleep(0.5)

# save results
if discovered_gpus:
    new_df = pd.DataFrame(discovered_gpus)
    combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=['SKU'], keep='first')
    combined_df.to_csv(file_path, index=False)
    print('Results appended to discovered_gpus.csv')
else:
    print('No new GPUs found.')

print('Scraping completed.')