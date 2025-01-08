import asyncio
from pyppeteer import launch
from bs4 import BeautifulSoup
import pandas as pd
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# BASE_FOLDER = 'scraped_data'
# BASE_CSV_FILE = os.path.join(BASE_FOLDER, 'file_data.csv')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the base folder path
BASE_FOLDER = os.path.join(SCRIPT_DIR, 'scraped_data')
BASE_CSV_FILE = os.path.join(BASE_FOLDER, 'file_data.csv')

async def scrape_tbody():
    try:
        browser = await launch(headless=True, args=['--no-sandbox'])
        page = await browser.newPage()
        url = "https://s3.amazonaws.com/tripdata/index.html"
        await page.goto(url)
        await page.waitForSelector('#tbody-content')
        await asyncio.sleep(5)
        tbody_html = await page.querySelectorEval('#tbody-content', '(element) => element.outerHTML')
        await browser.close()
    except Exception as e:
        logger.error(f"Error scraping tbody: {e}")
        return []
    
    soup = BeautifulSoup(tbody_html, 'html.parser')
    rows = soup.find_all('tr')
    data = []
    for row in rows:
        link = row.find('a')
        if link and 'JC' in link.get_text(strip=True):
            data.append({'Filename': link.get_text(strip=True), 'URL': link.get('href')})
    return data

def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logger.info(f"Folder created: {folder_path}")

def create_csv(data):
    if data:
        df = pd.DataFrame(data)
        df.to_csv(BASE_CSV_FILE, index=False)
        logger.info(f"CSV created at {BASE_CSV_FILE}")
    else:
        logger.info("No data to save to CSV.")

async def main():
    create_folder(BASE_FOLDER)
    data = await scrape_tbody()
    if data:
        create_csv(data)
    else:
        logger.warning("No data scraped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
