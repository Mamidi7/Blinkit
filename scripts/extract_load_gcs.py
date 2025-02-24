import os
import subprocess
from google.cloud import storage

# GCS bucket name
GCS_BUCKET = "blinkit-insights"

# Path to the dataset directory
DATASET_PATH = "/Users/krishnavardhan/blinkit"

# Get the list of files in the dataset directory
DATASET_FILES = os.listdir(DATASET_PATH)
print("Dataset files:", DATASET_FILES)

def create_bucket(bucket_name):
    """Creates a new GCS bucket if it doesn't exist."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        bucket.location = "US"  # Specify a location for the bucket

        try:
            bucket = storage_client.create_bucket(bucket, location=bucket.location)
            print(f"Bucket {bucket.name} created")
        except Exception as e:
            if "You already own this bucket" in str(e):
                print(f"Bucket {bucket_name} already exists")
            else:
                raise e

    except Exception as e:
        print(f"Error creating or accessing bucket: {e}")
        return False
    return True

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        print(f"File {source_file_name} uploaded to {destination_blob_name}")
        return True
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

# Create the GCS bucket
if create_bucket(GCS_BUCKET):
    # Upload each file in the dataset to GCS
    for file_name in DATASET_FILES:
        source_file_path = os.path.join(DATASET_PATH, file_name)
        destination_blob_name = file_name  # Use the same file name in GCS
        upload_to_gcs(GCS_BUCKET, source_file_path, destination_blob_name)

    print("Data extraction and load to GCS complete!")
else:
    print("GCS bucket creation failed. Please check the error message.")
