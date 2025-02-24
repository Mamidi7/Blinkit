from google.cloud import bigquery

# Google Cloud project ID
PROJECT_ID = "e2e-project-446614"
# BigQuery dataset ID
DATASET_ID = "blinkit_dataset"
# View ID
VIEW_ID = "product_sales_view"

def create_reporting_view(project_id, dataset_id, view_id):
    """Creates a reporting view in BigQuery."""
    try:
        client = bigquery.Client(project=project_id)
        view_id = f"{project_id}.{dataset_id}.{view_id}"

        # Define the SQL query for the reporting view
        query = f"""
        CREATE OR REPLACE VIEW `{view_id}` AS
        SELECT
            p.product_name,
            p.category,
            p.brand,
            p.price,
            p.price_category,
            o.order_id,
            o.customer_id,
            o.order_date,
            o.delivery_status,
            o.order_total,
            oi.quantity,
            oi.unit_price
        FROM
            `{project_id}.{dataset_id}.blinkit_products` p
        JOIN
            `{project_id}.{dataset_id}.blinkit_order_items` oi ON p.product_id = oi.product_id
        JOIN
            `{project_id}.{dataset_id}.blinkit_orders` o ON oi.order_id = o.order_id
        """

        client.query(query).result()
        print(f"Created reporting view {view_id}")

    except Exception as e:
        print(f"An error occurred while creating the reporting view: {e}")

def main():
    """Creates a reporting view in BigQuery."""
    try:
        create_reporting_view(PROJECT_ID, DATASET_ID, VIEW_ID)
        print("Reporting view creation complete!")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
