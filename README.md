# Return Analysis Pipeline

This project analyzes product return behavior in the `TheLook Ecommerce` dataset and builds a batch analytics pipeline on GCP using Bruin, BigQuery, GCS, and Looker Studio.

## Problem Statement

Returns are expensive for ecommerce businesses because they reduce revenue, increase operational costs, and can reveal product or customer behavior issues. The goal of this project is to build an end-to-end data pipeline that answers questions such as:

- How do return rates change over time?
- Which product categories have the highest return rates?
- Which SKUs drive the most revenue loss from returns?
- Which customers return the most items?

The output is a set of analytics tables in BigQuery and a dashboard in Looker Studio for reporting.

## Dataset

Source dataset:
- `bigquery-public-data.thelook_ecommerce`

Tables used:
- `order_items`
- `orders`
- `products`
- `users`

## Architecture

Data flow:

1. Public BigQuery tables are exported to GCS using a Python Bruin asset.
2. Raw CSV files in GCS are loaded into BigQuery raw tables using Bruin `ingestr` assets.
3. A staging table enriches order items with order, product, and user attributes.
4. Mart tables are built for reporting and dashboarding.
5. Looker Studio reads from the mart layer.

Core services:
- Google BigQuery
- Google Cloud Storage
- Bruin
- Looker Studio

## Project Structure

```text
assets/
  extract/
    export_to_gcs.py
  raw/
    order_items.asset.yml
    orders.asset.yml
    products.asset.yml
    users.asset.yml
  staging/
    staging.stg_order_items.sql
  mart/
    return_trends.sql
    returns_by_category.sql
    high_return_skus.sql
    top_returning_customers.sql
pipeline.yml
.bruin.yml.example
```

## Pipeline Layers

### Extract

`extract.export_to_gcs`

- Reads TheLook tables from the BigQuery public dataset
- Exports them as CSV files to GCS

### Raw

Raw assets load GCS CSVs into BigQuery:

- `raw.order_items`
- `raw.orders`
- `raw.products`
- `raw.users`

These assets depend on the extract step so the export runs before the raw loads.

### Staging

`staging.stg_order_items`

This table joins raw order items with orders, products, and users to create an analytics-ready fact table.

Optimization choices:
- Partitioned by `order_month`
- Clustered by `product_category`
- Clustered by `customer_country`

Why:
- Most analysis is performed over time, so partitioning by month reduces scan cost.
- Category and country are common grouping dimensions in the reporting layer.

### Mart

The mart layer contains business-facing aggregates:

- `mart.return_trends`
  Monthly return rate trend
- `mart.returns_by_category`
  Return behavior by product category
- `mart.high_return_skus`
  Products with high return rates and revenue loss
- `mart.top_returning_customers`
  Customers with high return activity

## Dashboard

Looker Studio dashboard:

https://lookerstudio.google.com/s/rkTZJjZMx40

The dashboard is built on top of the mart tables in BigQuery and answers:

- How return rates change over time
- Which categories have the worst return performance
- Which products drive the highest lost revenue
- Which customers have unusually high return behavior

## Tech Stack

- Orchestration: Bruin
- Cloud storage: Google Cloud Storage
- Data warehouse: BigQuery
- Transformation: SQL in Bruin assets
- Programming language: Python
- Visualization: Looker Studio

## Reproducibility

### Prerequisites

- A GCP project with BigQuery and Cloud Storage enabled
- A service account JSON key placed at `credentials/gcp_key.json`
- Bruin CLI installed - [Install Bruin
](https://getbruin.com/docs/bruin/getting-started/introduction/installation.html)
- Terraform installed - [Install Terraform](https://developer.hashicorp.com/terraform/install)
- Python environment and project dependencies installed via `uv`


### Provision Cloud Resources With Terraform

Terraform provisions:

- GCS bucket: `return-analysis-data`
- BigQuery datasets: `raw`, `staging`, `mart`

Run:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

Then update `terraform.tfvars` with your own GCP project, region, bucket, and credentials path.

This creates the storage bucket and BigQuery datasets used by the pipeline.

### Connections

Bruin connections should be created from [`.bruin.yml.example`]
Create your local Bruin config:

```bash
cp .bruin.yml.example .bruin.yml
```

Then update `.bruin.yml` with your own project ID, location, and local credentials path.

Current connections:
- `bigquery_main`
- `gcs_main`

### Install Dependencies

```bash
uv sync
```

### Run The Full Pipeline

```bash
bruin run . --full-refresh
```

This performs a full reload of the pipeline for the run.

### Validate The Pipeline

```bash
bruin validate
```

## Key Outputs

BigQuery datasets/tables:

- `raw.order_items`
- `raw.orders`
- `raw.products`
- `raw.users`
- `staging.stg_order_items`
- `mart.return_trends`
- `mart.returns_by_category`
- `mart.high_return_skus`
- `mart.top_returning_customers`

## What This Project Demonstrates

- Batch data ingestion from BigQuery public data to GCS and BigQuery
- Multi-layer warehouse design: raw, staging, mart
- Partitioning and clustering in BigQuery
- Automated orchestration with Bruin
- BI reporting with Looker Studio

## Future Improvements

- Add data quality checks for nulls, duplicates, and schema expectations
- Make the export step idempotent with explicit overwrite behavior in GCS
- Add parameterized environments for dev and prod
