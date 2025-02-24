# Blinkit ETL Pipeline 

This repository contains an ETL (Extract, Transform, Load) pipeline for Blinkit data. The pipeline is designed to extract data from various sources, transform it into a usable format, and load it into a data warehouse for analysis and reporting.

## Overview 

The pipeline consists of the following steps:

1.  **Extraction**: Data is extracted from various sources, including CSV files and potentially other data sources.
2.  **Transformation**: The extracted data is transformed to clean, normalize, and enrich it. This step may involve data cleaning, data type conversion, data aggregation, and data joining.
3.  **Loading**: The transformed data is loaded into a data warehouse, such as Google BigQuery, for analysis and reporting.

## File Structure 

The repository is organized as follows:

*   `data/`: This directory contains the raw data files used in the pipeline.
    *   `blinkit_customer_feedback.csv`: Customer feedback data.
    *   `blinkit_customers.csv`: Customer data.
    *   `blinkit_delivery_performance.csv`: Delivery performance data.
    *   `blinkit_inventory.csv`: Inventory data.
    *   `blinkit_inventoryNew.csv`: New inventory data.
    *   `blinkit_marketing_performance.csv`: Marketing performance data.
    *   `blinkit_order_items.csv`: Order items data.
    *   `blinkit_orders.csv`: Order data.
    *   `blinkit_products.csv`: Product data.
    *   `Category_Icons.xlsx`: Category icons data.
    *   `Rating_Icon.xlsx`: Rating icons data.
*   `scripts/`: This directory contains the scripts used in the pipeline.
    *   `create_reporting_view.py`: Script to create reporting views.
    *   `download_dataset.py`: Script to download the dataset.
    *   `extract_load_gcs.py`: Script to extract and load data into Google Cloud Storage.
    *   `load_bq.py`: Script to load data into Google BigQuery.
    *   `transform_bq.py`: Script to transform data in Google BigQuery.
*   `config/`: This directory contains the configuration files used in the pipeline.
*   `LICENSE`: The license for the repository.
*   `README.md`: This file, providing an overview of the repository.

## Scripts 

The following scripts are used in the pipeline:

*   `create_reporting_view.py`: This script creates reporting views in the data warehouse.
*   `download_dataset.py`: This script downloads the dataset from the source.
*   `extract_load_gcs.py`: This script extracts data from the source and loads it into Google Cloud Storage.
*   `load_bq.py`: This script loads data from Google Cloud Storage into Google BigQuery.
*   `transform_bq.py`: This script transforms data in Google BigQuery.

## License 

This repository is licensed under the MIT License. See the `LICENSE` file for details.

## Contact 
Feel free to reach out!

If you have any questions or suggestions, please feel free to contact me at krishnavardhan07@gmail.com.

## Getting Started

### Prerequisites
1. **Google Cloud Account:** You'll need a Google Cloud account with billing enabled.
2. **GCloud CLI:** Install and configure the Google Cloud CLI. [GCloud CLI Installation Guide](https://cloud.google.com/sdk/docs/install)
3. **Python 3.6+:** Make sure you have Python 3.6 or higher installed.
4. **Virtual Environment (Recommended):** Create a virtual environment to manage dependencies.

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Linux/macOS
    # venv\Scripts\activate  # On Windows
    ```
5. **Install Dependencies:**

    ```bash
    pip install -r requirements.txt # Create a requirements.txt file with necessary packages
    ```

### Configuration
1. **GCP Credentials:** Set up your Google Cloud credentials. The easiest way is to use `gcloud auth application-default login`.
2. **Configuration Files:** Update the configuration files in the `config/` directory with your project ID, dataset ID, bucket name, and any other relevant settings.

### Running the Pipeline
1. **Upload Data to GCS:** Upload your Blinkit dataset files to your GCS bucket.

    ```bash
    gsutil cp data/* gs://your-bucket-name/raw/
    ```
2. **Execute ETL Scripts:** Run the ETL scripts in the correct order.

    ```bash
    python scripts/download_dataset.py
    python scripts/extract_load_gcs.py
    python scripts/transform_bq.py
    python scripts/load_bq.py
    ```

## Detailed Steps (Example for BigQuery Loading)
1. **Create a BigQuery Dataset:**

    ```bash
    bq mk --location=US your-project-id:your_dataset_name
    ```
2. **Define a BigQuery Table Schema:** You can define your table schema directly in your `load.py` script or upload a JSON schema file.
3. **Load Data into BigQuery:** Use the BigQuery API or the `bq load` command to load your transformed data.

    ```bash
    bq load --source_format=CSV your-project-id:your_dataset_name.your_table_name gs://your-bucket-name/transformed/data.csv your_table_schema.json
    ```

## Testing
Add details on how to test your scripts. Example:
1. You can use sample datasets to test the script.
2. Use `pytest` to run the test cases.

## Optimization Tips
- **Partitioning and Clustering:** Optimize your BigQuery tables using partitioning and clustering for faster query performance. 
- **Data Types:** Choose the most appropriate data types for your BigQuery columns to reduce storage costs and improve query efficiency.
- **Error Handling:** Implement robust error handling in your ETL scripts to catch and handle potential issues.

## Contributing
Feel free to contribute to this project! Submit pull requests with improvements, bug fixes, or new features.

## License 
This repository is licensed under the MIT License. See the `LICENSE` file for details.

## Contact 
krishna vardhan - krishnavardhan07@gmail.com

---

***Note:** Replace the bracketed placeholders (e.g., `your-project-id`, `your-bucket-name`) with your actual values.*
