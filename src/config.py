import yaml

def read_config(config_path="../configs/config.yaml"):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Access file paths
    customers_path = config["file_paths"]["customers"]
    products_path = config["file_paths"]["products"]
    orders_path = config["file_paths"]["orders"]

    # Access table names
    raw_customers = config["tables_name"]["raw_tables"]["raw_customers"]
    raw_products = config["tables_name"]["raw_tables"]["raw_products"]
    raw_orders = config["tables_name"]["raw_tables"]["raw_orders"]
    enriched_customers = config["tables_name"]["enriched_tables"]["enriched_customers"]
    enriched_products = config["tables_name"]["enriched_tables"]["enriched_products"]
    enriched_orders = config["tables_name"]["enriched_tables"]["enriched_orders"]
    profit_agg_by_year_category_subcat_customer=config["tables_name"]["enriched_tables"]["profit_agg_by_year_category_subcat_customer"]

    return {
        "customers_path": customers_path,
        "products_path": products_path,
        "orders_path": orders_path,
        "raw_customers": raw_customers,
        "raw_products": raw_products,
        "raw_orders": raw_orders,
        "enriched_customers": enriched_customers,
        "enriched_products": enriched_products,
        "enriched_orders": enriched_orders,
        "profit_agg_by_year_category_subcat_customer":profit_agg_by_year_category_subcat_customer
    }