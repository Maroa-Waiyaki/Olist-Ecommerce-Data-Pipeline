import dagster
from dagster import asset, AssetExecutionContext, AssetIn, Definitions, define_asset_job, AssetSelection, ScheduleDefinition
import pandas as pd
import os

DATA_FOLDER = 'input datasets'


@asset(group_name ='Olist_Ecommerce_pipeline')
def extract_csv(context: AssetExecutionContext):
    """
    This is function is used for extracting the olist order and payments csv datasets
    """
    try:
        order_df = pd.read_csv(f'{DATA_FOLDER}/olist_orders_dataset.csv')
        order_item_df = pd.read_csv(f'{DATA_FOLDER}/olist_order_items_dataset.csv')
        customer_df = pd.read_csv(f'{DATA_FOLDER}/olist_customers_dataset.csv')
        product_df = pd.read_csv(f'{DATA_FOLDER}/olist_products_dataset.csv')
        seller_df = pd.read_csv(f'{DATA_FOLDER}/olist_sellers_dataset.csv')
        payment_df = pd.read_csv(f'{DATA_FOLDER}/olist_order_payments_dataset.csv')
        review_df = pd.read_csv(f'{DATA_FOLDER}/olist_order_reviews_dataset.csv')
        location_df = pd.read_csv(f'{DATA_FOLDER}/olist_geolocation_dataset.csv')
        product_names_df = pd.read_csv(f'{DATA_FOLDER}/product_category_name_translation.csv')

        context.log.info("Data extracted successfully.")
        
        dataframes = {
            "order_df": order_df,
            "order_item_df": order_item_df,
            "customer_df": customer_df,
            "product_df": product_df,
            "seller_df": seller_df,
            "payment_df": payment_df,
            "review_df": review_df,
            "location_df": location_df,
            "product_names_df": product_names_df
        }
        return dataframes
    except Exception as e:
        context.log.error(f"Error extracting data: {str(e)}")
        raise



@asset(ins={'upstream': AssetIn(key="extract_csv")}, group_name='Olist_Ecommerce_pipeline')
def data_integration(context: AssetExecutionContext, upstream: dict):
    """
    This function integrates all the datasets from Olist E-commerce platform
    """

    dataframes = upstream
    try:
        # Merge orders, order_items, order_payments, and order_reviews using order_id
        merged_df = dataframes["order_df"]\
            .merge(dataframes["order_item_df"], on="order_id", how="left")\
            .merge(dataframes["payment_df"], on="order_id", how="left")\
            .merge(dataframes["review_df"], on="order_id", how="left")

        # Merge the resulting DataFrame with products using product_id
        merged_1_df = merged_df.merge(dataframes["product_df"], on="product_id", how="left")

        # Merge the resulting DataFrame with customers using customer_id
        merged_2_df = merged_1_df.merge(dataframes["customer_df"], on="customer_id", how="left")

        # Merge the resulting DataFrame with sellers using seller_id
        merged_3_df = merged_2_df.merge(dataframes["seller_df"], on="seller_id", how="left")

        # Merge the resulting DataFrame with product_names_df using product_category_name
        merged_4_df = merged_3_df.merge(dataframes["product_names_df"], on='product_category_name', how='left')

        context.log.info("The merged dataset has {} rows and {} columns.".format(merged_4_df.shape[0], merged_4_df.shape[1]))
        context.log.info("The merged dataset has {} missing values.".format(merged_4_df.isnull().sum().sum()))
        
        return merged_4_df
    except Exception as e:
        context.log.error(f"Error merging datasets: {str(e)}")
        raise

@asset(ins={'upstream_1': AssetIn(key="data_integration")}, group_name='Olist_Ecommerce_pipeline')
def drop_id_columns(context:AssetExecutionContext, upstream_1: pd.DataFrame):
    """
    This function drops ID columns that are not important in our models or reports 
    """
    df = upstream_1
    try:
        columns_to_drop = ['customer_id',
                           'order_item_id',
                           'product_id',
                           'seller_id',
                           'review_id',
                           'product_width_cm',
                           'customer_unique_id']

        df_cleaned = df.drop(columns=columns_to_drop, errors='ignore')
        context.log.info("{} ID columns dropped successfully.".format(df.shape[1]-df_cleaned.shape[1]))
        
        return df_cleaned
    except Exception as e:
        context.log.error(f"Error dropping ID columns: {str(e)}")
        raise

@asset(ins={'upstream_2': AssetIn(key="drop_id_columns")}, group_name='Olist_Ecommerce_pipeline')
def treat_missing_values(context:AssetExecutionContext, upstream_2: pd.DataFrame):
    """
    This function is made to treat missing values in the data

    """
    df = upstream_2
    threshold=50
    try:
        # Drop columns with more than threshold percentage of missing values
        missing_percentages = df.isnull().mean() * 100
        columns_to_drop = missing_percentages[missing_percentages > threshold].index
        df_cleaned = df.drop(columns=columns_to_drop)

        context.log.info(f"Dropped columns with more than {threshold} % Missing values:{columns_to_drop}")
        context.log.info(f"Remaining Missing values: {df_cleaned.isnull().sum().sum()}.")
        # Drop rows with any NaN values
        df_cleaned = df_cleaned.dropna()

        context.log.info("Renaining missing values treated successfully.")
        
        return df_cleaned
    except Exception as e:
        context.log.error(f"Error treating missing values: {str(e)}")
        raise

@asset(ins={'upstream_3': AssetIn(key="treat_missing_values")}, group_name='Olist_Ecommerce_pipeline')
def transform_date_columns(context:AssetExecutionContext, upstream_3: pd.DataFrame):
    """
    This function is used to transform the date columns and also add new columns based on the date columns
    """
    df = upstream_3
    try:
        date_cols = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "shipping_limit_date",
            "review_creation_date",
            "review_answer_timestamp"
        ]

        for col in date_cols:
            df[col] = pd.to_datetime(df[col])

        context.log.info("Date columns transformed successfully.")

        df['purchase_year'] = df['order_purchase_timestamp'].dt.year
        df['purchase_month'] = df['order_purchase_timestamp'].dt.month_name()
        df['approval_duration_hours'] = (df['order_approved_at']-df['order_purchase_timestamp']).dt.total_seconds() / 3600
        df['delivery_duration_days'] = (df['order_delivered_customer_date']-df['order_approved_at']).dt.total_seconds() / (24*3600)
        df['estimated_duration_days'] = (df['order_estimated_delivery_date']-df['order_approved_at']).dt.total_seconds() / (24*3600)

        context.log.info("New columns created successfully.")

        return df
        
    except Exception as e:
        context.log.error(f"Transformation not successfully completed: {str(e)}")
        raise

@asset(ins={'upstream_4': AssetIn(key="transform_date_columns")}, group_name='Olist_Ecommerce_pipeline')
def load_csv(context: AssetExecutionContext, upstream_4: pd.DataFrame):
    """
    This function loads the transformed dataset to storage for modeling and report development
    """
    try:
        if upstream_4 is None:
            raise ValueError("Input 'upstream_4' is None. Expected a DataFrame.")

        df = upstream_4
        output_folder = 'output dataset'  # Specify your desired output folder
        output_file = 'Olist_clean.csv'  # Specify your desired output file name
        
        # Construct the full path
        output_path = os.path.join(output_folder, output_file)
    
        # Write DataFrame to CSV
        df.to_csv(output_path, index=False)
        context.log.info(f"Data successfully stored in {output_path}")
        
        # Return the transformed DataFrame for downstream assets (if needed)
        return df
    except Exception as e:
        context.log.error(f"Data loading to storage failed: {str(e)}")
        raise

dfs = Definitions(
    assets = [extract_csv, data_integration, drop_id_columns, treat_missing_values, transform_date_columns, load_csv],
    jobs = [
        define_asset_job(
            name='olist_e_commerce_etl_pipeline',
            selection=AssetSelection.groups("Olist_Ecommerce_pipeline")
        )
    ],
    schedules = [
        ScheduleDefinition(
            name="olist_etl_schedule",
            job_name = "olist_e_commerce_etl_pipeline",
            cron_schedule = "* * * * *"
        )
    ]
)