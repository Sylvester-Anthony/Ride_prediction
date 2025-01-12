# import os
# import pandas as pd
# import requests
# from zipfile import ZipFile
# from io import BytesIO

# # Function to download and extract files
# def download_and_extract(url, output_folder):
#     try:
#         # Send a GET request to the URL
#         response = requests.get(url)
#         response.raise_for_status()  # Raise an error for bad status codes
        
#         # Read the ZIP file from the response content
#         with ZipFile(BytesIO(response.content)) as zf:
#             zf.extractall(output_folder)  # Extract files to the output folder
#         print(f"Downloaded and extracted: {url}")
#     except Exception as e:
#         print(f"Failed to process {url}: {e}")

# # Path to the CSV file
# csv_file = '/Users/sylvesteranthony/Documents/Ride_prediction/scraped_data/file_data.csv'  # Update with your file path

# # Output folder to store the extracted files
# output_folder = 'extracted_files'
# os.makedirs(output_folder, exist_ok=True)

# # Read the CSV file
# df = pd.read_csv(csv_file)

# # Loop through each URL in the CSV and process it
# for url in df['URL']:
#     download_and_extract(url, output_folder)

# print("All files have been processed.")


import os
import pandas as pd
import requests
from zipfile import ZipFile
from io import BytesIO
from pyspark.sql import SparkSession

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

print("All files have been downloaded and extracted.")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Combine Extracted Files") \
    .getOrCreate()

# Define the file path pattern to read all extracted files (assuming CSV format)
extracted_files_pattern = os.path.join(output_folder, "*.csv")

# Read all extracted CSV files into a single Spark DataFrame
combined_df = spark.read.csv(extracted_files_pattern, header=True, inferSchema=True)

# Show combined data
combined_df.show(truncate=False)


combined_df.write.parquet("combined_data.parquet", mode="overwrite")

print("All extracted data has been combined into a single DataFrame.")