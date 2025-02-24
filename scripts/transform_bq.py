from google.cloud import bigquery

# Google Cloud project ID
PROJECT_ID = "e2e-project-446614"
# BigQuery dataset ID
DATASET_ID = "blinkit_dataset"

def transform_products(project_id, dataset_id):
    """Transforms the blinkit_products table."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.blinkit_products"

        # Define the SQL query to add the price_category column
        query = f"""
        ALTER TABLE `{table_id}`
        ADD COLUMN IF NOT EXISTS price_category STRING
        """
        client.query(query).result()
        print("Added price_category column to blinkit_products")

        # Define the SQL query to update the price_category column
        query = f"""
        UPDATE `{table_id}`
        SET price_category =
            CASE
                WHEN price < 100 THEN 'low'
                WHEN price >= 100 AND price < 500 THEN 'medium'
                ELSE 'high'
            END
        WHERE TRUE
        """
        client.query(query).result()
        print("Updated price_category column in blinkit_products")

        print("blinkit_products transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_products transformation: {e}")

def transform_customers(project_id, dataset_id):
    """Transforms the blinkit_customers table."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.blinkit_customers"

        # Define the SQL query to add the cltv column
        query = f"""
        ALTER TABLE `{table_id}`
        ADD COLUMN IF NOT EXISTS cltv NUMERIC
        """
        client.query(query).result()
        print("Added cltv column to blinkit_customers")

        # Define the SQL query to update the cltv column
        query = f"""
        UPDATE `{table_id}`
        SET cltv = SAFE_CAST((
            SELECT
                AVG(o.order_total) * COUNT(o.order_id) * (TIMESTAMP_DIFF(MAX(o.order_date), MIN(o.order_date), DAY) / 365.25)
            FROM
                `{project_id}.{dataset_id}.blinkit_orders` o
            WHERE o.customer_id = `{table_id}`.customer_id
        ) AS NUMERIC)
        WHERE TRUE
        """
        client.query(query).result()
        print("Updated cltv column in blinkit_customers")

        # Define the SQL query to add the order_frequency column
        query = f"""
        ALTER TABLE `{table_id}`
        ADD COLUMN IF NOT EXISTS order_frequency NUMERIC
        """
        client.query(query).result()
        print("Added order_frequency column to blinkit_customers")

        # Define the SQL query to update the order_frequency column
        query = f"""
        UPDATE `{table_id}`
        SET order_frequency = SAFE_CAST((
            SELECT
                CASE
                    WHEN TIMESTAMP_DIFF(MAX(o.order_date), MIN(o.order_date), DAY) = 0 THEN 0
                    ELSE COUNT(o.order_id) / (TIMESTAMP_DIFF(MAX(o.order_date), MIN(o.order_date), DAY) / 365.25)
                END
            FROM
                `{project_id}.{dataset_id}.blinkit_orders` o
            WHERE o.customer_id = `{table_id}`.customer_id
        ) AS NUMERIC)
        WHERE TRUE
        """
        client.query(query).result()
        print("Updated order_frequency column in blinkit_customers")

        print("blinkit_customers transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_customers transformation: {e}")

def transform_delivery_performance(project_id, dataset_id):
    """Transforms the blinkit_delivery_performance table."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.delivery_performance_summary"

        # Define the SQL query to create the delivery_performance_summary table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT
            delivery_partner_id,
            AVG(TIMESTAMP_DIFF(actual_delivery_time, promised_delivery_time, MINUTE)) AS average_delivery_time_minutes
        FROM
            `{project_id}.{dataset_id}.blinkit_orders`
        GROUP BY
            delivery_partner_id
        """
        client.query(query).result()
        print("Created delivery_performance_summary table")

        print("blinkit_delivery_performance transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_delivery_performance transformation: {e}")

def transform_marketing_performance(project_id, dataset_id):
    """Transforms the blinkit_marketing_performance table."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.marketing_performance_summary"

        # Define the SQL query to create the marketing_performance_summary table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT
            campaign_id,
            campaign_name,
            channel,
            SUM(impressions) AS total_impressions,
            SUM(clicks) AS total_clicks,
            SUM(conversions) AS total_conversions,
            SUM(spend) AS total_spend,
            SUM(revenue_generated) AS total_revenue,
            SAFE_DIVIDE(SUM(spend), SUM(conversions)) AS cost_per_conversion,
            SAFE_DIVIDE(SUM(revenue_generated), SUM(spend)) AS return_on_ad_spend
        FROM
            `{project_id}.{dataset_id}.blinkit_marketing_performance`
        GROUP BY
            campaign_id,
            campaign_name,
            channel
        """
        client.query(query).result()
        print("Created marketing_performance_summary table")

        print("blinkit_marketing_performance transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_marketing_performance transformation: {e}")

def transform_product_category(project_id, dataset_id):
    """Transforms the blinkit_products table."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.product_category_summary"

        # Define the SQL query to create the product_category_summary table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT
            p.category,
            SUM(oi.quantity) AS total_quantity,
            SUM(oi.unit_price * oi.quantity) AS total_revenue
        FROM
            `{project_id}.{dataset_id}.blinkit_products` p
        JOIN
            `{project_id}.{dataset_id}.blinkit_order_items` oi ON p.product_id = oi.product_id
        GROUP BY
            p.category
        """
        client.query(query).result()
        print("Created product_category_summary table")

        print("blinkit_product_category transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_product_category transformation: {e}")

def transform_sales_by_time_of_day(project_id, dataset_id):
    """Transforms the blinkit_orders table."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.sales_by_time_of_day"

        # Define the SQL query to create the sales_by_time_of_day table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT
            EXTRACT(HOUR FROM order_date) AS hour_of_day,
            SUM(order_total) AS total_sales,
            COUNT(order_id) AS total_orders
        FROM
            `{project_id}.{dataset_id}.blinkit_orders`
        GROUP BY
            EXTRACT(HOUR FROM order_date)
        ORDER BY
            EXTRACT(HOUR FROM order_date)
        """
        client.query(query).result()
        print("Created sales_by_time_of_day table")

        print("blinkit_sales_by_time_of_day transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_sales_by_time_of_day transformation: {e}")

def transform_sales_by_day_of_week(project_id, dataset_id):
    """Transforms the blinkit_orders table."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.sales_by_day_of_week"

        # Define the SQL query to create the sales_by_day_of_week table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT
            EXTRACT(DAYOFWEEK FROM order_date) AS day_of_week,
            SUM(order_total) AS total_sales,
            COUNT(order_id) AS total_orders
        FROM
            `{project_id}.{dataset_id}.blinkit_orders`
        GROUP BY
            EXTRACT(DAYOFWEEK FROM order_date)
        ORDER BY
            day_of_week
        """
        client.query(query).result()
        print("Created sales_by_day_of_week table")

        print("blinkit_sales_by_day_of_week transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_sales_by_day_of_week transformation: {e}")

def transform_customer_segments(project_id, dataset_id):
    """Transforms the blinkit_customers table."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.customer_segments"

        # Define the SQL query to create the customer_segments table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT
            customer_id,
            CASE
                WHEN cltv < 1000 AND order_frequency < 1 THEN 'Low Value'
                WHEN cltv < 1000 AND order_frequency >= 1 THEN 'Potential Growth'
                WHEN cltv >= 1000 AND order_frequency < 1 THEN 'High Value - Low Frequency'
                ELSE 'High Value'
            END AS customer_segment
        FROM
            `{project_id}.{dataset_id}.blinkit_customers`
        """
        client.query(query).result()
        print("Created customer_segments table")

        print("blinkit_customer_segments transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_customer_segments transformation: {e}")

def transform_product_affinity(project_id, dataset_id):
    """Transforms the blinkit_order_items table."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.product_affinity"

        # Define the SQL query to create the product_affinity table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT
            oi1.product_id AS product_id_1,
            oi2.product_id AS product_id_2,
            COUNT(*) AS co_occurrence_count
        FROM
            `{project_id}.{dataset_id}.blinkit_order_items` oi1
        JOIN
            `{project_id}.{dataset_id}.blinkit_order_items` oi2 ON oi1.order_id = oi2.order_id AND oi1.product_id < oi2.product_id
        GROUP BY
            oi1.product_id,
            oi2.product_id
        ORDER BY
            co_occurrence_count DESC
        """
        client.query(query).result()
        print("Created product_affinity table")

        print("blinkit_product_affinity transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_product_affinity transformation: {e}")

def transform_sales_by_region(project_id, dataset_id):
    """Transforms the blinkit_customers and blinkit_orders tables."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.sales_by_region"

        # Define the SQL query to create the sales_by_region table
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT
            SPLIT(c.address, ' ')[SAFE_OFFSET(0)] AS region,
            SUM(o.order_total) AS total_sales,
            COUNT(DISTINCT c.customer_id) AS total_customers
        FROM
            `{project_id}.{dataset_id}.blinkit_customers` c
        JOIN
            `{project_id}.{dataset_id}.blinkit_orders` o ON c.customer_id = o.customer_id
        GROUP BY
            region
        ORDER BY
            total_sales DESC
        """
        client.query(query).result()
        print("Created sales_by_region table")

        print("blinkit_sales_by_region transformation complete!")

    except Exception as e:
        print(f"An error occurred during blinkit_sales_by_region transformation: {e}")


def main():
    """Transforms data in BigQuery."""
    try:
        transform_products(PROJECT_ID, DATASET_ID)
        transform_customers(PROJECT_ID, DATASET_ID)
        transform_delivery_performance(PROJECT_ID, DATASET_ID)
        transform_marketing_performance(PROJECT_ID, DATASET_ID)
        transform_product_category(PROJECT_ID, DATASET_ID)
        transform_sales_by_time_of_day(PROJECT_ID, DATASET_ID)
        transform_sales_by_day_of_week(PROJECT_ID, DATASET_ID)
        transform_customer_segments(PROJECT_ID, DATASET_ID)
        transform_product_affinity(PROJECT_ID, DATASET_ID)
        transform_sales_by_region(PROJECT_ID, DATASET_ID)
        print("Data transformation complete!")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
