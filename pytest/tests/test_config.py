import sys
import os
import yaml
from tempfile import NamedTemporaryFile
import pytest
import sys
src_path = "/Workspace/Repos/srivastav_amol@yahoo.com/Ecommerce-Sales-Data-Processing/"
if src_path not in sys.path:
    sys.path.insert(0, src_path)
from src.config import read_config

@pytest.fixture
def sample_config():
    config_dict = {
        "file_paths": {
            "customers": "/Volumes/amol_uc/default/mydata/Customer.xlsx",
            "products": "/Volumes/amol_uc/default/mydata/Products.csv",
            "orders": "/Volumes/amol_uc/default/mydata/Orders.json"
        },
        "tables_name": {
            "raw_tables": {
                "raw_customers": "amol_uc.default.customers",
                "raw_products": "amol_uc.default.products",
                "raw_orders": "amol_uc.default.orders"
            },
            "enriched_tables": {
                "enriched_customers": "amol_uc.default.enriched_customers",
                "enriched_products": "amol_uc.default.enriched_products",
                "enriched_orders": "amol_uc.default.enriched_orders",
                "profit_agg_by_year_category_subcat_customer": "amol_uc.default.profit_agg_by_year_category_subcat_customer"
            }
        }
    }
    with NamedTemporaryFile(mode="w+", suffix=".yaml", delete=False) as tmp:
        yaml.dump(config_dict, tmp)
        tmp_path = tmp.name
    yield tmp_path, config_dict
    os.remove(tmp_path)

def test_read_config(sample_config):
    config_path, config_dict = sample_config
    result = read_config(config_path)
    assert result["customers_path"] == config_dict["file_paths"]["customers"]
    assert result["products_path"] == config_dict["file_paths"]["products"]
    assert result["orders_path"] == config_dict["file_paths"]["orders"]
    assert result["raw_customers"] == config_dict["tables_name"]["raw_tables"]["raw_customers"]
    assert result["raw_products"] == config_dict["tables_name"]["raw_tables"]["raw_products"]
    assert result["raw_orders"] == config_dict["tables_name"]["raw_tables"]["raw_orders"]
    assert result["enriched_customers"] == config_dict["tables_name"]["enriched_tables"]["enriched_customers"]
    assert result["enriched_products"] == config_dict["tables_name"]["enriched_tables"]["enriched_products"]
    assert result["enriched_orders"] == config_dict["tables_name"]["enriched_tables"]["enriched_orders"]