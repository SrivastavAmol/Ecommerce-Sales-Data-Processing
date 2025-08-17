from pyspark.sql.functions import trim, regexp_replace,col

def clean_and_save_tables(df_customers, df_products, df_orders, config):
    # Clean customers table
    df_cutomers_clean = (
        df_customers
        .withColumnRenamed("Customer ID", "Customer_ID")
        .withColumnRenamed("Customer Name", "Customer_Name")
        .withColumnRenamed("Postal Code", "Postal_Code")
    )
    df_cutomers_clean = df_cutomers_clean.withColumn(
        "Customer_Name",
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("Customer_Name"), r"\d+", ""),  # Remove digits
                    r"[^a-zA-Z\s]", ""  # Remove special characters except spaces
                ),
                r"\s+", " "
            )
        )
    )
    df_cutomers_clean.write.mode("overwrite").saveAsTable(config['raw_customers'])

    # Clean products table
    df_products_clean = (
        df_products
        .withColumnRenamed("Product ID", "Product_ID")
        .withColumnRenamed("Product Name", "Product_Name")
        .withColumnRenamed("Price per product", "Price_per_product")
    )
    df_products_clean.write.mode("overwrite").saveAsTable(config['raw_products'])

    # Clean orders table
    df_orders_clean = df_orders.select(
        [df_orders[col].alias(col.replace(" ", "_").replace("-", "_")) for col in df_orders.columns]
    )
    df_orders_clean.write.mode("overwrite").saveAsTable(config['raw_orders'])

    return df_cutomers_clean, df_products_clean, df_orders_clean