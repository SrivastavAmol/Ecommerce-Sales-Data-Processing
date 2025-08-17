import pytest
import pandas as pd
from pyspark.sql import SparkSession

@pytest.fixture
def sample_dataframes():
    spark = (
        SparkSession.builder
        .appName("pytest-spark")
        .getOrCreate()
    )
    df_customers_clean = pd.DataFrame([
        {"Customer_ID": 1, "Customer_Name": "Alice", "email": "alice@example.com", "phone": "123", "address": "Addr1", "Segment": "A", "Country": "US", "City": "NY", "State": "NY", "Postal_Code": "10001", "Region": "East"},
        {"Customer_ID": 2, "Customer_Name": None, "email": "bob@example.com", "phone": "456", "address": "Addr2", "Segment": "B", "Country": "US", "City": "LA", "State": "CA", "Postal_Code": "90001", "Region": "West"}
    ])
    df_products_clean = pd.DataFrame([
        {"Product_ID": 101, "Category": "Electronics", "Sub-Category": "Mobile", "Product_Name": "iPhone", "State": "NY", "Price_per_product": 699.99},
        {"Product_ID": 102, "Category": None, "Sub-Category": None, "Product_Name": None, "State": "CA", "Price_per_product": None}
    ])
    df_orders_clean = pd.DataFrame([
        {"Order_ID": 1001, "Customer_ID": 1, "Product_ID": 101, "Quantity": 2, "Price": 699.99, "Order_Date": "1/1/2024"},
        {"Order_ID": 1002, "Customer_ID": 2, "Product_ID": 102, "Quantity": 1, "Price": 0, "Order_Date": "2/1/2024"}
    ])
    config = {
        'enriched_customers': 'test_enriched_customers',
        'enriched_products': 'test_enriched_products',
        'enriched_orders': 'test_enriched_orders',
        'profit_agg_by_year_category_subcat_customer': 'test_profit_agg'
    }
    # Convert pandas DataFrames to Spark DataFrames
    df_customers_clean_spark = spark.createDataFrame(df_customers_clean)
    df_products_clean_spark = spark.createDataFrame(df_products_clean)
    df_orders_clean_spark = spark.createDataFrame(df_orders_clean)
    yield df_customers_clean_spark, df_products_clean_spark, df_orders_clean_spark, config
    spark.stop()

def test_create_enriched_tables(sample_dataframes, tmp_path, monkeypatch):
    df_customers_clean, df_products_clean, df_orders_clean, config = sample_dataframes

    from src.create_enriched_tables import enrich_and_save_tables

    df_enriched_customers, df_enriched_products, df_enriched_orders, df_profit_agg = enrich_and_save_tables(
        df_customers_clean, df_products_clean, df_orders_clean, config
    )

    # Check enriched customers
    assert any(row["Customer_Name"] == "Alice" for row in df_enriched_customers.collect())
    assert any(row["Customer_Name"] == "Unknown" or row["Customer_Name"] is None for row in df_enriched_customers.collect())

    # Check enriched products
    assert any(row["Product_Name"] == "iPhone" for row in df_enriched_products.collect())
    assert any(row["Category"] == "Unknown" or row["Category"] is None for row in df_enriched_products.collect())
    assert any(row["Sub-Category"] == "Unknown" or row["Sub-Category"] is None for row in df_enriched_products.collect())
    assert any(row["Price_per_product"] == 0 or row["Price_per_product"] is None for row in df_enriched_products.collect())

    # Check enriched orders
    assert all("profit" in row.asDict() for row in df_enriched_orders.collect())

    # Check profit aggregation
    assert all("total_profit" in row.asDict() for row in df_profit_agg.collect())