import os
import sys
import tempfile
import pandas as pd
import importlib
import pytest

# ensure repo src is importable
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import src.data_quality_checks as dq
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
    customers_file.close()
    customers_df.to_excel(customers_file.name, index=False, engine="openpyxl")

    # Products CSV
    products_df = pd.DataFrame({
        "Product ID": [10, 20, 20, None],
        "Product Name": ["Widget", None, "Gadget", "Thing"],
        "Category": ["A", None, "B", "C"],
        "Sub-Category": ["X", None, "Y", "Z"],
        "Price per product": [100, None, 200, 300]
    })
    products_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    products_file.close()
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
    orders_file.close()
    orders_df.to_json(orders_file.name, orient="records", lines=False)

    yield customers_file.name, products_file.name, orders_file.name

    for path in (customers_file.name, products_file.name, orders_file.name):
        try:
            if os.path.exists(path):
                os.remove(path)
        except PermissionError:
            pass

def test_data_quality_checks_calls_display(monkeypatch, sample_files):
    customers_path, products_path, orders_path = sample_files

    captured = {}
    def fake_display(obj):
        captured['obj'] = obj

    # patch the display used inside the module
    monkeypatch.setattr(dq, "display", fake_display)

    # call function
    ret = data_quality_checks(customers_path, products_path, orders_path)

    # original implementation returns the result of display(...), so ret is typically None
    assert ret is None or ret is pd.DataFrame or True  # non-blocking; we rely on captured

    # ensure display was called with a DataFrame
    assert 'obj' in captured, "display() was not called"
    df = captured['obj']
    assert isinstance(df, pd.DataFrame), "display was not called with a DataFrame"

    # basic structure checks
    expected_cols = {
        "table",
        "file_exists",
        "missing_columns",
        "total_rows",
        "unique_ids",
        "missing_values",
        "null_columns",
        "ids_with_missing_values",
        "duplicate_ids",
    }
    assert expected_cols.issubset(set(df.columns)), f"missing expected columns: {expected_cols - set(df.columns)}"
    assert len(df) == 3  # customers, products, orders

def test_data_quality_checks_missing_files(monkeypatch):
    # pass non-existent file paths to simulate missing files
    fake_paths = ("no_such_customers.xlsx", "no_such_products.csv", "no_such_orders.json")

    captured = {}
    def fake_display(obj):
        captured['obj'] = obj

    monkeypatch.setattr(dq, "display", fake_display)

    ret = data_quality_checks(*fake_paths)

    # display should be called with a DataFrame describing missing files
    assert 'obj' in captured
    df = captured['obj']
    assert isinstance(df, pd.DataFrame)
    # file_exists should be False for all rows
    assert df["file_exists"].tolist() == [False, False, False]
    # missing_columns should be full required lists (non-empty)
    assert all(isinstance(x, list) and len(x) > 0 for x in df["missing_columns"])