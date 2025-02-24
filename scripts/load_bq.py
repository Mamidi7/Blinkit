import os
from google.cloud import bigquery
from google.cloud import storage

# Google Cloud project ID
PROJECT_ID = "e2e-project-446614"
# BigQuery dataset ID
DATASET_ID = "blinkit_dataset"
# GCS bucket name
GCS_BUCKET = "blinkit-insights"

def load_csv_to_bq(bucket_name, file_name, project_id, dataset_id, table_id):
    """Loads a CSV file from GCS to BigQuery."""
    try:
        client = bigquery.Client(project=project_id)
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=True,  # Autodetect schema
            quote_character='"',  # Specify quote character
            field_delimiter=',',
            allow_quoted_newlines=True
        )

        gcs_uri = f"gs://{bucket_name}/{file_name}"

        load_job = client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )  # API request
        load_job.result()  # Waits for the job to complete.

        print(f"Loaded {file_name} to {dataset_id}.{table_id}")
        return True
    except Exception as e:
        print(f"Error loading {file_name} to {dataset_id}.{table_id}: {e}")
        return False

def main():
    """Loads data from GCS to BigQuery."""
    try:
        # Get the list of files in the GCS bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        blobs = bucket.list_blobs()

        # Create the BigQuery dataset if it doesn't exist
        client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = client.dataset(DATASET_ID)
        try:
            client.get_dataset(dataset_ref)  # Make an API request.
            print(f"Dataset {DATASET_ID} already exists")
        except:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Specify a location for the dataset
            client.create_dataset(dataset, timeout=30)  # Make an API request.
            print(f"Created dataset {DATASET_ID}")

        # Load each CSV file into a separate BigQuery table
        for blob in blobs:
            if blob.name.endswith(".csv"):
                file_name = blob.name
                table_id = file_name.replace(".csv", "").replace(" ", "_").lower()  # Create table ID from file name
                load_csv_to_bq(GCS_BUCKET, file_name, PROJECT_ID, DATASET_ID, table_id)

        print("Data load to BigQuery complete!")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
