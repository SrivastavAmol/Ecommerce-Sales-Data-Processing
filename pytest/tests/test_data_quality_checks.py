import pytest
import pandas as pd
import tempfile
import os
import json
import sys
src_path = "/Workspace/Repos/srivastav_amol@yahoo.com/Ecommerce-Sales-Data-Processing/"
if src_path not in sys.path:
    sys.path.insert(0, src_path)
from src.data_quality_checks import data_quality_checks

@pytest.fixture
def sample_files():
    # Customers Excel
    customers_df = pd.DataFrame({
        "Customer ID": [1, 2, 2, None],
        "Customer Name": ["Alice", None, "Bob", "Charlie"],
        "email": ["a@x.com", "b@x.com", "b@x.com", None],
        "phone": ["123", "456", None, "789"],
        "address": ["Addr1", "Addr2", "Addr2", None]
    })
    customers_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    customers_df.to_excel(customers_file.name, index=False, engine='openpyxl')

    # Products CSV
    products_df = pd.DataFrame({
        "Product ID": [10, 20, 20, None],
        "Product Name": ["Widget", None, "Gadget", "Thing"],
        "Category": ["A", None, "B", "C"],
        "Sub-Category": ["X", None, "Y", "Z"],
        "Price per product": [100, None, 200, 300]
    })
    products_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    products_df.to_csv(products_file.name, index=False)

    # Orders JSON
    orders_df = pd.DataFrame({
        "Order ID": [100, 200, 200, None],
        "Customer ID": [1, 2, 2, 3],
        "Product ID": [10, 20, 20, None],
        "Order_Date": ["2025-08-01", None, "2025-08-03", "2025-08-04"],
        "Quantity": [1, None, 2, 3]
    })
    orders_file = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    orders_df.to_json(orders_file.name, orient="records", lines=False)

    yield customers_file.name, products_file.name, orders_file.name

    os.unlink(customers_file.name)
    os.unlink(products_file.name)
    os.unlink(orders_file.name)

def test_data_quality_checks(sample_files):
    customers_path, products_path, orders_path = sample_files
    # The function returns display(), so we just check for no exceptions and correct summary structure
    result = data_quality_checks(customers_path, products_path, orders_path)
    # No assertion on display output, but we can check for warnings using pytest.warns if you refactor warnings to be raised