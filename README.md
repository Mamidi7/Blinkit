# ETL Pipeline Project

This project demonstrates an ETL pipeline using Google Cloud services such as Google Cloud Storage (GCS) and BigQuery.

## Instructions

1.  Download the dataset from Kaggle.
2.  Create a Google Cloud Storage bucket: [https://cloud.google.com/storage/docs/creating-buckets](https://cloud.google.com/storage/docs/creating-buckets)
3.  Upload the dataset files to your GCS bucket.
4.  Create a BigQuery dataset: [https://cloud.google.com/bigquery/docs/datasets](https://cloud.google.com/bigquery/docs/datasets)
5.  Create a BigQuery table: [https://cloud.google.com/bigquery/docs/tables](https://cloud.google.com/bigquery/docs/tables)
6.  Update the variables in the `extract_load_gcs.py`, `load_bq.py`, `transform_bq.py`, and `create_reporting_view.py` scripts with your Google Cloud project ID, BigQuery dataset ID, table ID, GCS bucket name, and dataset file paths.
7.  Authenticate to Google Cloud: [https://cloud.google.com/docs/authentication/getting-started](https://cloud.google.com/docs/authentication/getting-started)
8.  Run the scripts in the following order:
    *   `extract_load_gcs.py`
    *   `load_bq.py`
    *   `transform_bq.py`
    *   `create_reporting_view.py`

## Scripts

*   `extract_load_gcs.py`: Extracts data from the dataset and loads it into GCS.
*   `load_bq.py`: Loads data from GCS into BigQuery.
*   `transform_bq.py`: Transforms data in BigQuery.
*   `create_reporting_view.py`: Creates a reporting view in BigQuery.
