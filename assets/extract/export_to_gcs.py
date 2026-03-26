"""@bruin
name: extract.export_to_gcs
type: python
@bruin"""

from google.cloud import bigquery, storage
from pathlib import Path

PROJECT_ID  = "return-analysis-490800"
GCS_BUCKET  = "return-analysis-data"
CREDENTIALS = Path("./credentials/gcp_key.json")

THELOOK_TABLES = {
    "order_items":          "bigquery-public-data.thelook_ecommerce.order_items",
    "orders":               "bigquery-public-data.thelook_ecommerce.orders",
    "products":             "bigquery-public-data.thelook_ecommerce.products",
    "users":                "bigquery-public-data.thelook_ecommerce.users",
}

def export_to_gcs():
    print("Exporting TheLook tables to GCS...")
    client     = bigquery.Client.from_service_account_json(str(CREDENTIALS), project=PROJECT_ID)
    gcs_client = storage.Client.from_service_account_json(str(CREDENTIALS))
    bucket     = gcs_client.bucket(GCS_BUCKET)

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

export_to_gcs()
