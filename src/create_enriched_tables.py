from pyspark.sql.functions import trim, regexp_replace, col, sum, max, expr, countDistinct, coalesce,lit
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import year, to_date, col, sum
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType
from pyspark.sql import DataFrame

def enrich_and_save_tables(df_cutomers_clean, df_products_clean, df_orders_clean, config):
    # Enrich Customers
    df_enriched_customers = (
        df_cutomers_clean.alias("c")
        .join(
            df_orders_clean.alias("o"),
            col("c.Customer_ID") == col("o.Customer_ID"),
            "left"
        )
        .join(
            df_products_clean.alias("p"),
            col("o.Product_ID") == col("p.Product_ID"),
            "left"
        )
        .groupBy(
            "c.Customer_ID",
            "c.Customer_Name",
            "c.email",
            "c.phone",
            "c.address",
            "c.Segment",
            "c.Country",
            "c.City",
            "c.State",
            "c.Postal_Code",
            "c.Region"
        )
        .agg(
            countDistinct("o.Order_ID").alias("total_orders"),
            sum(
                col("o.Quantity") * expr("try_cast(p.Price_per_product as decimal(18,2))")
            ).alias("total_spent"),
            max("o.Order_Date").alias("last_order_date")
        )
    )
    df_enriched_customers.write.mode("overwrite").saveAsTable(config['enriched_customers'])

    # Enrich Products
    df_enriched_products = (
        df_products_clean.alias("p")
        .join(
            df_orders_clean.alias("o"),
            col("p.Product_ID") == col("o.product_id"),
            "left"
        )
        .groupBy(
            col("p.Product_ID"),
            col("p.Category"),
            col("p.Sub-Category"),
            col("p.Product_Name"),
            col("p.State"),
            col("p.Price_per_product")
        )
        .agg(
            sum(col("o.quantity")).alias("total_units_sold"),
            sum(
                col("o.quantity") * coalesce(
                    expr("try_cast(p.Price_per_product as decimal(18,2))"),
                    lit(0)
                )
            ).alias("total_revenue")
        )
        .withColumnRenamed("Product_ID", "product_id")
        .withColumnRenamed("Category", "category")
        .withColumnRenamed("Sub-Category", "sub_category")
        .withColumnRenamed("Product_Name", "product_name")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Price_per_product", "price_per_product")
    )
    df_enriched_products.write.mode("overwrite").saveAsTable(config['enriched_products'])

    # Enrich Orders
    df_enriched_orders = (
        df_orders_clean.alias("o")
        .join(
            df_cutomers_clean.alias("c"),
            col("o.Customer_ID") == col("c.Customer_ID"),
            "left"
        )
        .join(
            df_products_clean.alias("p"),
            col("o.Product_ID") == col("p.Product_ID"),
            "left"
        )
        .select(
            col("o.Order_ID").alias("order_id"),
            col("o.Order_Date").alias("order_date"),
            col("o.Quantity").alias("quantity"),
            spark_round(col("o.Price"), 2).alias("price"),  # Use spark_round instead of round
            col("c.Customer_Name").alias("customer_name"),
            col("c.Country").alias("country"),
            coalesce(col("p.Category"), lit("Unknown")).alias("category"),
            coalesce(col("p.Sub-Category"), lit("Unknown-sub-category")).alias("sub_category")
        )
    )
    df_enriched_orders = (df_enriched_orders.withColumn("profit",df_enriched_orders["price"] * df_enriched_orders["quantity"]))
    df_enriched_orders.write.mode("overwrite").saveAsTable(config['enriched_orders'])

    # enrich aggregated table
    
    df_profit_agg = (
        df_enriched_orders
        .withColumn(
            "order_date_parsed",
            to_date(
                col("order_date"),
                "d/M/yyyy"
            )
        )
        .withColumn(
            "year",
            year(col("order_date_parsed"))
        )
        .groupBy(
            "year",
            "category",
            "sub_category",
            "customer_name"
        )
        .agg(
            sum("profit").alias("total_profit")
        )
    )

    df_profit_agg.write.mode("overwrite").saveAsTable(config['profit_agg_by_year_category_subcat_customer'])

    return df_enriched_customers, df_enriched_products, df_enriched_orders, df_profit_agg