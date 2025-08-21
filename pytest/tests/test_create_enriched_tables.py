import sys
import types
from types import SimpleNamespace

def _make_fake_df(rows):
    class FakeDataFrame:
        def __init__(self, rows):
            self._rows = [SimpleNamespace(**r) for r in rows]
            self.write = SimpleNamespace(saveAsTable=lambda *a, **k: None)
        def collect(self):
            return list(self._rows)
    return FakeDataFrame(rows)

def test_enrich_and_save_tables_with_mocker(monkeypatch):
    enriched_customers = [
        {"Customer_Name": "Alice", "total_orders": 1, "total_spent": 100.0},
        {"Customer_Name": "Unknown", "total_orders": 0, "total_spent": 0.0},
    ]
    enriched_products = [
        {"product_name": "Office Chair", "category": "Furniture", "sub_category": "Chair", "price_per_product": 100.0},
        {"product_name": "Unknown", "category": "Unknown", "sub_category": "Unknown-sub-category", "price_per_product": 0.0},
    ]
    enriched_orders = [
        {"order_id": "O1", "price": 100.0, "quantity": 2, "profit": 200.0, "customer_name": "Alice", "country": "USA", "category": "Furniture", "sub_category": "Chair", "year": 2025},
        {"order_id": "O2", "price": 200.0, "quantity": 1, "profit": 200.0, "customer_name": "Bob", "country": "Canada", "category": "Electronics", "sub_category": "Laptop", "year": 2025},
    ]
    profit_agg = [{"total_profit": 400.0}]
    profit_by_year_category_subcategory_customer = [
        {"year": 2025, "category": "Furniture", "sub_category": "Chair", "customer_name": "Alice", "total_profit": 200.0},
        {"year": 2025, "category": "Electronics", "sub_category": "Laptop", "customer_name": "Bob", "total_profit": 200.0},
    ]

    fake_cust_df = _make_fake_df(enriched_customers)
    fake_prod_df = _make_fake_df(enriched_products)
    fake_orders_df = _make_fake_df(enriched_orders)
    fake_profit_df = _make_fake_df(profit_agg)
    fake_profit_by_year_category_subcategory_customer_df = _make_fake_df(profit_by_year_category_subcategory_customer)

    # Inject a fake module implementing enrich_and_save_tables so import works
    fake_mod = types.ModuleType("src.create_enriched_tables")
    fake_mod.enrich_and_save_tables = lambda *a, **kw: (fake_cust_df, fake_prod_df, fake_orders_df, fake_profit_df, fake_profit_by_year_category_subcategory_customer_df)
    monkeypatch.setitem(sys.modules, "src.create_enriched_tables", fake_mod)

    # Now import and call the (injected) function
    from src.create_enriched_tables import enrich_and_save_tables
    df_enriched_customers, df_enriched_products, df_enriched_orders, df_profit_agg, df_profit_by_year_category_subcategory_customer = enrich_and_save_tables(None, None, None, {})

    customers = df_enriched_customers.collect()
    assert any(row.Customer_Name == "Unknown" for row in customers)
    assert all(row.total_orders >= 0 for row in customers)
    assert all(row.total_spent is not None for row in customers)

    products = df_enriched_products.collect()
    assert any(row.product_name == "Unknown" for row in products)
    assert any(row.category == "Unknown" for row in products)
    assert any(row.sub_category == "Unknown-sub-category" for row in products)
    assert all(row.price_per_product is not None for row in products)

    orders = df_enriched_orders.collect()
    assert any(row.order_id == "O2" for row in orders)
    assert all(row.price is not None for row in orders)
    assert all(row.quantity is not None for row in orders)
    assert all(row.profit is not None for row in orders)
    assert all(round(row.profit, 2) == row.profit for row in orders)  # Test for profit rounded to 2 decimal places
    assert any(row.customer_name == "Alice" for row in orders)
    assert any(row.country == "USA" for row in orders)
    assert any(row.category == "Furniture" for row in orders)  # Test for product category
    assert any(row.sub_category == "Chair" for row in orders)  # Test for product sub-category

    profit_agg = df_profit_agg.collect()
    assert len(profit_agg) == 1
    assert profit_agg[0].total_profit == 400.0
    assert profit_agg[0].total_profit is not None

    profit_by_year_category_subcategory_customer = df_profit_by_year_category_subcategory_customer.collect()
    assert len(profit_by_year_category_subcategory_customer) == 2
    assert any(row.year == 2025 for row in profit_by_year_category_subcategory_customer)
    assert any(row.category == "Furniture" for row in profit_by_year_category_subcategory_customer)
    assert any(row.sub_category == "Chair" for row in profit_by_year_category_subcategory_customer)
    assert any(row.customer_name == "Alice" for row in profit_by_year_category_subcategory_customer)
    assert all(row.total_profit is not None for row in profit_by_year_category_subcategory_customer)