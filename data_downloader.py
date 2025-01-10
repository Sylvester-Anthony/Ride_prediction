import os
import pandas as pd
import requests
from zipfile import ZipFile
from io import BytesIO

# Function to download and extract files
def download_and_extract(url, output_folder):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        
        # Read the ZIP file from the response content
        with ZipFile(BytesIO(response.content)) as zf:
            zf.extractall(output_folder)  # Extract files to the output folder
        print(f"Downloaded and extracted: {url}")
    except Exception as e:
        print(f"Failed to process {url}: {e}")

# Path to the CSV file
csv_file = '/Users/sylvesteranthony/Documents/Ride_prediction/scraped_data/file_data.csv'  # Update with your file path

# Output folder to store the extracted files
output_folder = 'extracted_files'
os.makedirs(output_folder, exist_ok=True)

# Read the CSV file
df = pd.read_csv(csv_file)

# Loop through each URL in the CSV and process it
for url in df['URL']:
    download_and_extract(url, output_folder)

print("All files have been processed.")