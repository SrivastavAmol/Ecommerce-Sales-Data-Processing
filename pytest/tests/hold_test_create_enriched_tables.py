import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType


def test_enrich_and_save_tables(tmp_path):
    customers_data = [
        Row(Customer_ID="C1", Customer_Name="Alice", email="alice@example.com", phone="123", address="Addr1", Segment="Consumer", Country="USA", City="NY", State="NY", Postal_Code="10001", Region="East"),
        Row(Customer_ID="C2", Customer_Name=None, email="bob@example.com", phone="456", address="Addr2", Segment="Corporate", Country="USA", City="SF", State="CA", Postal_Code="94105", Region="West"),
    ]
    customers_schema = StructType([
        StructField("Customer_ID", StringType(), True),
        StructField("Customer_Name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal_Code", StringType(), True),
        StructField("Region", StringType(), True),
    ])
    df_customers_clean = spark.createDataFrame(customers_data, customers_schema)
    # Continue with your test logic

    # Sample data for products
    products_data = [
        Row(Product_ID="P1", Category="Furniture", **{"Sub-Category": "Chair"}, Product_Name="Office Chair", State="NY", Price_per_product="100.00"),
        Row(Product_ID="P2", Category=None, **{"Sub-Category": None}, Product_Name=None, State="CA", Price_per_product=None),
    ]
    products_schema = StructType([
        StructField("Product_ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product_Name", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Price_per_product", StringType(), True),
    ])
    df_products_clean = spark.createDataFrame(products_data, products_schema)

    # Sample data for orders
    orders_data = [
        Row(Order_ID="O1", Customer_ID="C1", Product_ID="P1", Quantity=2, Price=100.0, Order_Date="1/1/2024"),
        Row(Order_ID="O2", Customer_ID="C2", Product_ID="P2", Quantity=1, Price=200.0, Order_Date="2/2/2024"),
    ]
    orders_schema = StructType([
        StructField("Order_ID", StringType(), True),
        StructField("Customer_ID", StringType(), True),
        StructField("Product_ID", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Order_Date", StringType(), True),
    ])
    df_orders_clean = spark.createDataFrame(orders_data, orders_schema)

    config = {
        'enriched_customers': 'test_enriched_customers',
        'enriched_products': 'test_enriched_products',
        'enriched_orders': 'test_enriched_orders',
        'profit_agg_by_year_category_subcat_customer': 'test_profit_agg'
    }

    # Call the function
    df_enriched_customers, df_enriched_products, df_enriched_orders, df_profit_agg = enrich_and_save_tables(
        df_cutomers_clean, df_products_clean, df_orders_clean, config
    )

    # Check enriched customers
    customers = df_enriched_customers.collect()
    assert any(row.Customer_Name == "Unknown" for row in customers), "Missing Customer_Name not filled as 'Unknown'"
    assert all(row.total_orders >= 0 for row in customers)
    assert all(row.total_spent is not None for row in customers)

    # Check enriched products
    products = df_enriched_products.collect()
    assert any(row.product_name == "Unknown" for row in products), "Missing Product_Name not filled as 'Unknown'"
    assert any(row.category == "Unknown" for row in products), "Missing Category not filled as 'Unknown'"
    assert any(row.sub_category == "Unknown-sub-category" for row in products), "Missing Sub-Category not filled as 'Unknown-sub-category'"
    assert any(row.price_per_product == "0" or row.price_per_product == 0 for row in products), "Missing Price_per_product not filled as 0"

    # Check enriched orders
    orders = df_enriched_orders.collect()
    assert all(row.profit == row.price * row.quantity for row in orders)

    # Check profit aggregation
    profit_agg = df_profit_agg.collect()
    assert all(row.total_profit >= 0 for row in profit_agg)