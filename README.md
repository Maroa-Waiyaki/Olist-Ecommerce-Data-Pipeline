**Olist E-commerce Data Pipeline using Dagster**

This project implements an ETL (Extract, Transform, Load) data pipeline using Dagster, a data orchestrator tool. The pipeline integrates various datasets from Olist, an e-commerce platform, processes them to create a clean dataset, and stores it for further analysis and reporting.

**Project Structure**

olist_etl.py: Defines the Dagster job that orchestrates the data pipeline.
requirements.txt: Lists all Python dependencies required for the project.
input datasets/: Directory containing the input CSV datasets from Olist.
output dataset/: Directory where the cleaned dataset (Olist_clean.csv) is stored after processing.

**Components**

**Extract**

The `extract_csv` function reads multiple CSV files from the input datasets/ directory:

olist_orders_dataset.csv
olist_order_items_dataset.csv
olist_customers_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_geolocation_dataset.csv
product_category_name_translation.csv

**Transform**

The `data_integration, drop_id_columns, treat_missing_values`, and `transform_date_columns` functions perform data transformations:

Data Integration: Merges various datasets based on common keys (e.g., order_id, product_id).
Drop ID Columns: Removes unnecessary identifier columns from the integrated dataset.
Treat Missing Values: Handles missing values by dropping columns with high missing percentages and rows with any NaN values.
Transform Date Columns: Converts date columns to datetime format and creates new columns based on date differences and categories.

**Load**

The `load_csv` function writes the cleaned and transformed dataset (Olist_clean.csv) to the output dataset/ directory.

**Running the Pipeline**

**_Setup_**: Ensure Python and necessary packages are installed (pip install -r requirements.txt).

**_Execution_**: Run the Dagster job using the following command:

`dagster pipeline execute -f main_job.py`

Output: After successful execution, check the output dataset/ directory for Olist_clean.csv.

**Requirements**

All Python dependencies required to run the project are listed in requirements.txt. Install them using:


`pip install -r requirements.txt`
