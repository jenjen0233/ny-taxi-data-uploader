import pandas as pd
import requests
import os
import sys
import logging
from google.cloud import storage

# Set up logging for better visibility during the Docker run
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Base URL for the NYC TLC data from DataTalksClub repository
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"

 # Get environment variables
bucket_name = os.getenv("BUCKET")
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")

# URL for the static lookup table file
LOOKUP_URL = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
LOOKUP_FILENAME = 'taxi_zone_lookup.csv'
LOOKUP_PARQUET_FILENAME = 'taxi_zone_lookup.parquet'

# upload to gcs
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    try:

        # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
        # (Ref: https://github.com/googleapis/python-storage/issues/74)
        # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
        # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        logging.info(f"Uploading {source_file_name} to gs://{bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(source_file_name)
        logging.info(f"Successfully uploaded {destination_blob_name}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload {destination_blob_name}: {e}")
        return False

# download from github and convert to parquet
def web_to_gcs(year,service):

    for i in range(1,13):
        month = i
        month_str = str(month).zfill(2)
        filename = f"{service}_tripdata_{year}-{month_str}.csv.gz"
        release_tag = service

        # Construct the full download URL
        url = f"{BASE_URL}{release_tag}/{filename}"

        try:
            # Use a temporary file path for the downloaded content
            local_csv_path = f"/tmp/{filename}"

            r = requests.get(url, stream=True)
            r.raise_for_status() # Raise an exception for bad status codes

            with open(local_csv_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

            logging.info(f"Downloaded: {filename}")

            # Read the GZIP compressed CSV directly into a Pandas DataFrame
            df = pd.read_csv(local_csv_path, compression='gzip')

            # Clean up the monthly CSV file immediately after reading
            os.remove(local_csv_path)

            # Define output paths and filenames
            output_filename = f"{service}_tripdata_{year}-{month_str}.parquet"
            local_parquet_path = f"/tmp/{output_filename}"
            gcs_destination_path = f"{service}/{year}/{output_filename}"

            # Save the DataFrame as a Parquet file locally
            logging.info(f"Converting to Parquet: {local_parquet_path}")
            # Use pyarrow engine for better compatibility and efficiency
            df.to_parquet(local_parquet_path, engine='pyarrow', index=False, compression='gzip')

            # Upload the Parquet file to GCS
            if upload_to_gcs(bucket_name, local_parquet_path, gcs_destination_path):
                #Clean up the local Parquet file
                os.remove(local_parquet_path)

        except requests.exceptions.HTTPError as http_err:
            # The data source sometimes misses a few months, log a warning instead of erroring out completely
            logging.warning(f"Could not find data for {service}/{year}-{month_str} (HTTP Error: {http_err}). Skipping this month.")
        except Exception as e:
            logging.error(f"An unexpected error occurred during download/read for {filename}: {e}")
    """Downloads a single month's data and returns it as a DataFrame."""

def download_lookup_table():
    """Downloads the taxi zone lookup table and uploads it to GCS."""
    try:
        local_csv_path = f"/tmp/{LOOKUP_FILENAME}"
        local_parquet_path = f"/tmp/{LOOKUP_PARQUET_FILENAME}"
        gcs_destination_path = LOOKUP_PARQUET_FILENAME

        r = requests.get(LOOKUP_URL)
        r.raise_for_status()  # Raise an exception for bad status codes

        with open(local_csv_path, 'wb') as f:
            f.write(r.content)

        logging.info(f"Downloaded lookup table: {LOOKUP_FILENAME}")

        # Read the CSV into a DataFrame
        df = pd.read_csv(local_csv_path)

        # Save as Parquet
        df.to_parquet(local_parquet_path, engine='pyarrow', index=False, compression='gzip')

        # Upload to GCS
        if upload_to_gcs(bucket_name, local_parquet_path, gcs_destination_path):
            # Clean up local files
            os.remove(local_csv_path)
            os.remove(local_parquet_path)

    except Exception as e:
        logging.error(f"An error occurred while processing the lookup table: {e}")

def main():
    """Main function to start the process."""
    logging.info("Starting NYC Taxi data upload process")

    if not bucket_name:
        logging.error("BUCKET environment variable is not set. Exiting.")
        sys.exit(1)
    
    if not project_id:
        logging.error("GOOGLE_CLOUD_PROJECT environment variable is not set. Exiting.")
        sys.exit(1)

    years = ['2019', '2020']
    # services = ['green', 'yellow']
    services = ['green']
    
    for year in years:
        for service in services:
            # Call the web_to_gcs function for each year and service
            web_to_gcs(year, service)

    download_lookup_table()

if __name__ == "__main__":
    main()