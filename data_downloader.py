import os
import pandas as pd
import requests
from zipfile import ZipFile
from io import BytesIO
import psycopg2
from pyspark.sql import SparkSession

# Function to download and extract files
def download_and_extract(url, output_folder):
    try:
        response = requests.get(url)
        response.raise_for_status()
        with ZipFile(BytesIO(response.content)) as zf:
            zf.extractall(output_folder)
        print(f"Downloaded and extracted: {url}")
    except Exception as e:
        print(f"Failed to process {url}: {e}")

# PostgreSQL connection details (no password)
db_host = "localhost"
db_port = "5432"
db_user = "sylvesteranthony"
db_name = "ride_prediction"  # New database name
db_table = "combined_data"
jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

# Create PostgreSQL database and table dynamically
def create_db_and_table(schema, db_host, db_port, db_user, db_name, db_table):
    try:
        conn = psycopg2.connect(
            dbname="postgres", user=db_user, host=db_host, port=db_port
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Database '{db_name}' created.")
        else:
            print(f"Database '{db_name}' already exists.")
        cursor.close()
        conn.close()
        conn = psycopg2.connect(dbname=db_name, user=db_user, host=db_host, port=db_port)
        cursor = conn.cursor()
        columns = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {db_table} ({columns})"
        cursor.execute(create_table_query)
        print(f"Table '{db_table}' created with schema: {schema}")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database or table: {e}")

# Path to the CSV file
csv_file = '/Users/sylvesteranthony/Documents/Ride_prediction/scraped_data/file_data.csv'
output_folder = 'extracted_files'
os.makedirs(output_folder, exist_ok=True)
df = pd.read_csv(csv_file)
for url in df['URL']:
    download_and_extract(url, output_folder)
print("All files have been downloaded and extracted.")

spark = SparkSession.builder \
    .appName("Combine and Write to PostgreSQL") \
    .config("spark.jars", "/Users/sylvesteranthony/Documents/Ride_prediction/postgresql-42.7.5.jar") \
    .getOrCreate()

extracted_files_pattern = os.path.join(output_folder, "*.csv")
combined_df = spark.read.csv(extracted_files_pattern, header=True, inferSchema=True)
schema = {field.name: "TEXT" if field.dataType.simpleString() == "string" else "FLOAT" for field in combined_df.schema.fields}
create_db_and_table(schema, db_host, db_port, db_user, db_name, db_table)
combined_df.write.jdbc(url=jdbc_url, table=db_table, mode="overwrite", properties={
    "user": db_user,
    "driver": "org.postgresql.Driver"
})
print("Data has been successfully written to the PostgreSQL database.")
