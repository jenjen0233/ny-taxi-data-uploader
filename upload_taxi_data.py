import io
import os
import requests
import pandas as pd
from google.cloud import storage
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/'
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")
TEMP_DIR = "/app/temp"

def web_to_gcs(year, service):
   
    logger.info(f"Starting upload for {service} taxi data for year {year}")
    
    for i in range(12):
        # Format month with leading zero
        month = f"{i+1:02d}"
        
        # File names
        csv_file = f"{service}_tripdata_{year}-{month}.csv.gz"
        
        # Full paths
        csv_path = os.path.join(TEMP_DIR, csv_file)
        
        try:
            # Download CSV file
            request_url = f"{init_url}{service}/{csv_file}"
            logger.info(f"Downloading {request_url}")
            
            response = requests.get(request_url, stream=True)
            response.raise_for_status()
            
            with open(csv_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded: {csv_file}")
            
            # Convert to parquet
            logger.info(f"Converting {csv_file} to parquet...")
            df = pd.read_csv(csv_path, compression='gzip')
            df.to_parquet(parquet_path, engine='pyarrow')
            logger.info(f"Converted to: {parquet_file}")
            
            # Upload to GCS
            gcs_path = f"{service}/{parquet_file}"
            upload_to_gcs(BUCKET, gcs_path, parquet_path)
            
            # Clean up local files to save space
            os.remove(csv_path)
            os.remove(parquet_path)
            logger.info(f"Cleaned up local files for {month}")
            
        except Exception as e:
            logger.error(f"Failed to process {service} data for {year}-{month}: {e}")
            # Clean up any remaining files
            for file_path in [csv_path, parquet_path]:
                if os.path.exists(file_path):
                    os.remove(file_path)
            continue

def main():
    """
    Main function to orchestrate the upload process
    """
    logger.info("Starting NYC Taxi data upload process")
    logger.info(f"Using bucket: {BUCKET}")
    
    # Verify credentials are available
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        logger.info(f"Using credentials from: {creds_path}")
    else:
        logger.error("Google Cloud credentials not found!")
        raise ValueError("Missing Google Cloud credentials")
    
    # Create temp directory if it doesn't exist
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    # Define the data to upload
    upload_tasks = [
        ('2020', 'green'),
        ('2020', 'yellow')
    ]
    
    try:
        total_tasks = len(upload_tasks)
        logger.info(f"Will upload {total_tasks} datasets: {upload_tasks}")
        
        for i, (year, service) in enumerate(upload_tasks, 1):
            logger.info(f"Starting task {i}/{total_tasks}: {service} taxi data for {year}")
            web_to_gcs(year, service)
            logger.info(f"Completed task {i}/{total_tasks}: {service} {year}")
        
        logger.info("All upload tasks completed successfully!")
        
    except Exception as e:
        logger.error(f"Upload process failed: {e}")
        raise

if __name__ == "__main__":
    main()