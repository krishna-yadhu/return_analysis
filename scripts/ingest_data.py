from pathlib import Path
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

GCS_BUCKET  = "return-analysis-data"
PROJECT_ID  = "return-analysis-490800"
CREDENTIALS = Path("./credentials/return-analysis-490800-9f8a54f16dfd.json")

THELOOK_TABLES = {
    "order_items":"bigquery-public-data.thelook_ecommerce.order_items",
    "orders": "bigquery-public-data.thelook_ecommerce.orders",
    "products": "bigquery-public-data.thelook_ecommerce.products",
    "users": "bigquery-public-data.thelook_ecommerce.users" 
}

def export_to_gcs():
    print("Exporting TheLook tables to GCS...")
    client = bigquery.Client.from_service_account_json(
        str(CREDENTIALS),
        project=PROJECT_ID
    )
    gcs_client = storage.Client.from_service_account_json(str(CREDENTIALS))
    bucket = gcs_client.bucket(GCS_BUCKET)

    for table_name, full_table_id in THELOOK_TABLES.items():
        destination = f"gs://{GCS_BUCKET}/raw/thelook_{table_name}/*.csv"
        prefix      = f"raw/thelook_{table_name}/"
        blobs       = list(bucket.list_blobs(prefix=prefix))

        '''if blobs:
            print(f"Skipping {table_name} — already in GCS")
            continue'''

        print(f"Exporting {table_name}...")
        extract_job = client.extract_table(
            full_table_id,
            destination,
            job_config=bigquery.ExtractJobConfig(
                destination_format="CSV",
                print_header=True
            )
        )
        extract_job.result()
        print(f"{table_name} → {destination}")
    print()

def create_bruin_assets():
    print("Creating Bruin raw assets...")
    assets_dir = Path("./assets/raw")
    assets_dir.mkdir(parents=True, exist_ok=True)

    for table_name in THELOOK_TABLES.keys():
        asset_content = f"""name: raw.{table_name}
type: ingestr
connection: bigquery_main

parameters:
  source_connection: gcs_main
  source_table: gs://{GCS_BUCKET}/raw/thelook_{table_name}/*.csv
  destination: bigquery_main
  destination_table: raw.{table_name} 
"""
        asset_path = assets_dir / f"{table_name}.asset.yml"
        if asset_path.exists():
            print(f" Skipping {table_name}.asset.yml — already exists")
        else:
            asset_path.write_text(asset_content)
            print(f" Created assets/raw/{table_name}.asset.yml")
    print()

def create_bq_datasets():
    print("Creating BigQuery datasets...")
    client = bigquery.Client.from_service_account_json(
        str(CREDENTIALS),
        project=PROJECT_ID
    )

    datasets = ["staging", "mart"]

    for dataset_name in datasets:
        dataset_id = f"{PROJECT_ID}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-south1"

        try:
            client.create_dataset(dataset, exists_ok=True)
            print(f" Dataset {dataset_name} ready")
        except Exception as e:
            print(f"Error creating {dataset_name}: {e}")
    print()

def remove_raw_dataset():
    print("Removing raw dataset...")
    
    client = bigquery.Client.from_service_account_json(
        str(CREDENTIALS),
        project=PROJECT_ID
    )

    dataset_name = "raw"
    dataset_id = f"{PROJECT_ID}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)  # check if exists
        
        client.delete_dataset(
            dataset_id,
            delete_contents=True,
            not_found_ok=True
        )
        print(f"Dataset {dataset_name} removed")

    except NotFound:
        print(f"Dataset {dataset_name} does not exist, skipping removal")

    except Exception as e:
        print(f"Error removing {dataset_name}: {e}")

    print()

if __name__ == "__main__":
    print("Return Analysis — Data Ingestion\n")
    create_bq_datasets()   
    export_to_gcs()
    remove_raw_dataset()
    create_bruin_assets()
    print("Done! TheLook data in GCS, Bruin assets created.")