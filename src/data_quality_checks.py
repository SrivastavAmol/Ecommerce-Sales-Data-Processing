# Imports for data quality checks
import warnings
import os
import pandas as pd
import yaml
from IPython.display import display

def data_quality_checks(file_path_customers, file_path_products, file_path_orders):
    # Customers
    file_exists_customers = os.path.exists(file_path_customers)
    pdf = pd.read_excel(file_path_customers, engine='openpyxl') if file_exists_customers else pd.DataFrame()
    required_columns_customers = ["Customer ID", "Customer Name", "email", "phone", "address"]
    missing_columns_customers = [col for col in required_columns_customers if col not in pdf.columns] if file_exists_customers else required_columns_customers
    duplicate_customer_ids = pdf.loc[pdf["Customer ID"].duplicated(keep=False), "Customer ID"].dropna().unique().tolist() if file_exists_customers and "Customer ID" in pdf.columns else []
    null_columns_customers = pdf[required_columns_customers].isnull().any() if file_exists_customers and not missing_columns_customers else pd.Series([True]*len(required_columns_customers), index=required_columns_customers)
    missing_customers = list(null_columns_customers[null_columns_customers].index) if file_exists_customers and not missing_columns_customers else required_columns_customers
    if file_exists_customers and not missing_columns_customers and null_columns_customers.any():
        warnings.warn(f"Null values found in columns: {missing_customers}.")
    pdf["phone"] = pdf["phone"].astype(str) if "phone" in pdf.columns else pdf
    phone_nulls = pdf["phone"].isnull().any() if "phone" in pdf.columns else False
    customer_ids_with_missing_values = pdf.loc[pdf[required_columns_customers].isnull().any(axis=1), "Customer ID"].dropna().unique().tolist() if file_exists_customers and not missing_columns_customers else []

    # Products
    file_exists_products = os.path.exists(file_path_products)
    pdf_products = pd.read_csv(file_path_products) if file_exists_products else pd.DataFrame()
    required_columns_products = ["Product ID", "Product Name", "Category", "Sub-Category", "Price per product"]
    missing_columns_products = [col for col in required_columns_products if col not in pdf_products.columns] if file_exists_products else required_columns_products
    duplicate_product_ids = pdf_products.loc[pdf_products["Product ID"].duplicated(keep=False), "Product ID"].dropna().unique().tolist() if file_exists_products and "Product ID" in pdf_products.columns else []
    null_columns_products = pdf_products[required_columns_products].isnull().any() if file_exists_products and not missing_columns_products else pd.Series([True]*len(required_columns_products), index=required_columns_products)
    missing_products = list(null_columns_products[null_columns_products].index) if file_exists_products and not missing_columns_products else required_columns_products
    if file_exists_products and not missing_columns_products and null_columns_products.any():
        warnings.warn(f"Null values found in columns: {missing_products}.")
    product_ids_with_missing_values = pdf_products.loc[pdf_products[required_columns_products].isnull().any(axis=1), "Product ID"].dropna().unique().tolist() if file_exists_products and not missing_columns_products else []

    # Orders
    file_exists_orders = os.path.exists(file_path_orders)
    pdf_orders = pd.read_json(file_path_orders, lines=False) if file_exists_orders else pd.DataFrame()
    required_columns_orders = ["Order ID", "Customer ID", "Product ID", "Order_Date", "Quantity"]
    missing_columns_orders = [col for col in required_columns_orders if col not in pdf_orders.columns] if file_exists_orders else required_columns_orders
    duplicate_order_ids = pdf_orders.loc[pdf_orders["Order ID"].duplicated(keep=False), "Order ID"].dropna().unique().tolist() if file_exists_orders and "Order ID" in pdf_orders.columns else []
    null_columns_orders = pdf_orders[required_columns_orders].isnull().any() if file_exists_orders and not missing_columns_orders else pd.Series([True]*len(required_columns_orders), index=required_columns_orders)
    missing_orders = list(null_columns_orders[null_columns_orders].index) if file_exists_orders and not missing_columns_orders else required_columns_orders
    if file_exists_orders and not missing_columns_orders and null_columns_orders.any():
        warnings.warn(f"Null values found in columns: {missing_orders}.")
    order_ids_with_missing_values = pdf_orders.loc[pdf_orders[required_columns_orders].isnull().any(axis=1), "Order ID"].dropna().unique().tolist() if file_exists_orders and not missing_columns_orders else []

    # Combined summary
    summary_combined = pd.DataFrame([
        {
            "table": "customers",
            "file_exists": file_exists_customers,
            "missing_columns": missing_columns_customers,
            "total_rows": len(pdf),
            "unique_ids": pdf["Customer ID"].nunique() if file_exists_customers and "Customer ID" in pdf.columns else 0,
            "missing_values": pdf[required_columns_customers].isnull().sum().to_dict() if file_exists_customers and not missing_columns_customers else {col: None for col in required_columns_customers},
            "null_columns": missing_customers,
            "ids_with_missing_values": customer_ids_with_missing_values,
            "duplicate_ids": duplicate_customer_ids
        },
        {
            "table": "products",
            "file_exists": file_exists_products,
            "missing_columns": missing_columns_products,
            "total_rows": len(pdf_products),
            "unique_ids": pdf_products["Product ID"].nunique() if file_exists_products and "Product ID" in pdf_products.columns else 0,
            "missing_values": pdf_products[required_columns_products].isnull().sum().to_dict() if file_exists_products and not missing_columns_products else {col: None for col in required_columns_products},
            "null_columns": missing_products,
            "ids_with_missing_values": product_ids_with_missing_values,
            "duplicate_ids": duplicate_product_ids
        },
        {
            "table": "orders",
            "file_exists": file_exists_orders,
            "missing_columns": missing_columns_orders,
            "total_rows": len(pdf_orders),
            "unique_ids": pdf_orders["Order ID"].nunique() if file_exists_orders and "Order ID" in pdf_orders.columns else 0,
            "missing_values": pdf_orders[required_columns_orders].isnull().sum().to_dict() if file_exists_orders and not missing_columns_orders else {col: None for col in required_columns_orders},
            "null_columns": missing_orders,
            "ids_with_missing_values": order_ids_with_missing_values,
            "duplicate_ids": duplicate_order_ids
        }
    ])
    
    return display(summary_combined)

