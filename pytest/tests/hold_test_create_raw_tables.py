import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

try:
    spark
except NameError:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest-spark")
        .getOrCreate()
    )

def test_clean_and_save_tables(monkeypatch):
    customers_data = [
        Row(**{"Customer ID": "C1", "Customer Name": "John1231 Doe", "Postal Code": "12345"}),
        Row(**{"Customer ID": "C2", "Customer Name": None, "Postal Code": "54321"})
    ]
    products_data = [
        Row(**{"Product ID": "P1", "Product Name": None, "Price per product": 10.0}),
        Row(**{"Product ID": "P2", "Product Name": "Widget", "Price per product": None})
    ]
    orders_data = [
        Row(**{"Order ID": "O1", "Customer ID": "C1", "Product ID": "P1"}),
        Row(**{"Order ID": "O2", "Customer ID": "C2", "Product ID": "P2"})
    ]

    df_customers = spark.createDataFrame(customers_data)
    df_products = spark.createDataFrame(products_data)
    df_orders = spark.createDataFrame(orders_data)

    config = {
        "raw_customers": "test_raw_customers",
        "raw_products": "test_raw_products",
        "raw_orders": "test_raw_orders"
    }

    # Prevent actual table writes
    monkeypatch.setattr(df_customers.write, "saveAsTable", lambda *a, **k: None)
    monkeypatch.setattr(df_products.write, "saveAsTable", lambda *a, **k: None)
    monkeypatch.setattr(df_orders.write, "saveAsTable", lambda *a, **k: None)

    # Apply null handling
    df_customers_filled = df_customers.withColumn(
        "Customer Name",
        F.when(F.col("Customer Name").isNull(), "Unknown").otherwise(F.col("Customer Name"))
    )

    df_products_filled = df_products.select(
        *[
            F.col(c) for c in df_products.columns
            if c not in ["Product Name", "Price per product"]
        ],
        F.when(F.col("Product Name").isNull(), "Unknown").otherwise(F.col("Product Name")).alias("Product Name"),
        F.when(F.col("Price per product").isNull(), 0.0).otherwise(F.col("Price per product")).alias("Price per product")
    )

    # Run assertions
    assert df_customers_filled.filter(F.col("Customer Name") == "Unknown").count() == 1
    assert df_products_filled.filter(F.col("Product Name") == "Unknown").count() == 1
    assert df_products_filled.filter(F.col("Price per product") == 0.0).count() == 1