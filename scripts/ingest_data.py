import kaggle
import duckdb
import os
import shutil
from pathlib import Path
from google.cloud import storage

DATA_DIR = Path("./data")
DB_PATH = DATA_DIR / "pipeline.duckdb"
DATASET = "olistbr/brazilian-ecommerce"
GCS_BUCKET   = "return-analysis-data"
CREDENTIALS  = Path("./credentials/return-analysis-490800-9f8a54f16dfd.json")

TABLES = {
    "raw_orders":      "olist_orders_dataset.csv",
    "raw_order_items": "olist_order_items_dataset.csv",
    "raw_sellers":     "olist_sellers_dataset.csv",
    "raw_products":    "olist_products_dataset.csv",
    "raw_customers":   "olist_customers_dataset.csv",  
}

def download_data():
    
    all_exist = all(
        (DATA_DIR / filename).exists()
        for filename in TABLES.values()
    )

    if all_exist:
        print("Data already downloaded, skipping...\n")
        return

    
    print("Downloading Olist dataset from Kaggle...")
    DATA_DIR.mkdir(exist_ok=True)
    kaggle.api.dataset_download_files(
        DATASET,
        path=DATA_DIR,
        unzip=True
    )
    print("Download complete\n")

def verify_files():
    print("Verifying CSV files...")
    all_good = True
    for table, filename in TABLES.items():
        path = DATA_DIR / filename
        if path.exists():
            print(f" {filename}")
        else:
            print(f" MISSING: {filename}")
            all_good = False
    print()
    return all_good


def create_bucket_if_not_exists():
    print("Checking GCS bucket")
    client = storage.Client.from_service_account_json(str(CREDENTIALS))
    
    bucket = client.lookup_bucket(GCS_BUCKET)
    if bucket is None:
        bucket = client.create_bucket(GCS_BUCKET, location="asia-south1")
        print(f"Created bucket: gs://{GCS_BUCKET}\n")
    else:
        print(f"Bucket already exists: gs://{GCS_BUCKET}\n") 

def upload_to_gcs():
    print("Uploading CSVs to GCS...")

    # authenticate using service account key
    client = storage.Client.from_service_account_json(str(CREDENTIALS))
    bucket = client.bucket(GCS_BUCKET)

    for table, filename in TABLES.items():
        source_path = DATA_DIR / filename
        destination = f"raw/{filename}"

        blob = bucket.blob(destination)
        blob.upload_from_filename(str(source_path))
        print(f" {filename} → gs://{GCS_BUCKET}/{destination}")

    print()

def cleanup_local():
    print("Cleaning up local data folder...")
    for file in DATA_DIR.iterdir():
        if file.is_file():
            file.unlink()
            print(f"Deleted {file.name}")
    print()

if __name__ == "__main__":
    print("Return Analysis — Data Ingestion\n")
    download_data()
    if verify_files():
        create_bucket_if_not_exists() 
        upload_to_gcs()
        cleanup_local()            
        print("Done! CSVs in GCS, local files cleaned up.")
    else:
        print("Some files missing. Check your Kaggle credentials.")

