import asyncio
from pyppeteer import launch
from bs4 import BeautifulSoup
import pandas as pd
import os

# Define the base folder and file name
BASE_FOLDER = 'scraped_data'
BASE_CSV_FILE = os.path.join(BASE_FOLDER, 'file_data.csv')

async def scrape_tbody():
    # Launch a browser (headless for normal usage)
    browser = await launch(headless=True, args=['--no-sandbox'])
    page = await browser.newPage()

    # Navigate to the URL
    url = "https://s3.amazonaws.com/tripdata/index.html"
    await page.goto(url)

    # Wait for the page to load completely
    await page.waitForSelector('#tbody-content')
    await asyncio.sleep(5)  # Wait for JavaScript execution

    # Capture the tbody content
    try:
        tbody_html = await page.querySelectorEval('#tbody-content', '(element) => element.outerHTML')
    except Exception as e:
        print("Error extracting tbody content:", e)
        tbody_html = None

    # Close the browser
    await browser.close()

    data = []
    if tbody_html:
        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(tbody_html, 'html.parser')
        rows = soup.find_all('tr')

        if not rows:
            print("No rows found!")
        else:
            # Extract data into a list
            for row in rows:
                link = row.find('a')
                if link:
                    href = link.get('href')
                    text = link.get_text(strip=True)
                    if 'JC' in text:
                        data.append({'Filename': text, 'URL': href})
    return data

def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder created: {folder_path}")

def create_csv(data, version=0):
    # Create the file name with versioning if required
    file_name = BASE_CSV_FILE if version == 0 else os.path.join(BASE_FOLDER, f"file_data_v{version}.csv")

    # Create a new DataFrame from the scraped data
    df = pd.DataFrame(data)

    # Save to CSV
    df.to_csv(file_name, index=False)
    print(f"CSV file created: {file_name}")

def update_csv(new_data):
    # Read the existing CSV file
    existing_df = pd.read_csv(BASE_CSV_FILE)

    # Convert new data into a DataFrame
    new_df = pd.DataFrame(new_data)

    # Identify new rows
    combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=['Filename', 'URL'], keep='first')

    # Check if there are new files
    if len(combined_df) > len(existing_df):
        # Find the next version number
        version = 1
        while os.path.exists(os.path.join(BASE_FOLDER, f"file_data_v{version}.csv")):
            version += 1

        # Save the updated DataFrame as a new version
        create_csv(combined_df.to_dict('records'), version=version)
        print(f"CSV updated! {len(combined_df) - len(existing_df)} new files added as version {version}.")
    else:
        print("No new files detected. No updates made.")

async def main():
    # Ensure the base folder exists
    create_folder(BASE_FOLDER)

    if not os.path.exists(BASE_CSV_FILE):
        # No CSV file found; perform initial scrape and save
        print("No existing CSV file found. Scraping data for the first time...")
        data = await scrape_tbody()
        if data:
            create_csv(data)
        else:
            print("No data scraped from the website.")
    else:
        print("Existing CSV file found. Checking for updates...")
        data = await scrape_tbody()
        if data:
            update_csv(data)
        else:
            print("No data scraped from the website.")


asyncio.get_event_loop().run_until_complete(main())
