from pathlib import Path
import pandas as pd
import pytest

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "outputs"


@pytest.fixture
def analytics_outputs(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "test_analytics.OUTPUT_DIR",
        tmp_path,
    )

    pd.DataFrame({
        "total_orders": [100],
        "product_sales": [5000.0],
    }).to_csv(tmp_path / "kpis.csv", index=False)

    pd.DataFrame({
        "month": ["2017-01"],
        "product_sales": [1000.0],
    }).to_csv(tmp_path / "monthly_sales.csv", index=False)

    pd.DataFrame({
        "category": [f"category_{i}" for i in range(10)],
        "items_sold": [100] * 10,
        "product_sales": [500.0] * 10,
    }).to_csv(tmp_path / "top_categories.csv", index=False)

    pd.DataFrame({
        "seller_id": [f"seller_{i}" for i in range(10)],
        "items_sold": [50] * 10,
        "product_sales": [300.0] * 10,
    }).to_csv(tmp_path / "top_sellers.csv", index=False)


def test_all_reports_exist(analytics_outputs):
    expected_files = [
        "kpis.csv",
        "monthly_sales.csv",
        "top_categories.csv",
        "top_sellers.csv",
    ]

    for filename in expected_files:
        assert (OUTPUT_DIR / filename).is_file()


def test_top_categories_has_ten_rows(analytics_outputs):
    df = pd.read_csv(OUTPUT_DIR / "top_categories.csv")

    assert len(df) == 10
    assert {"category", "items_sold", "product_sales"}.issubset(df.columns)


def test_top_sellers_has_ten_rows(analytics_outputs):
    df = pd.read_csv(OUTPUT_DIR / "top_sellers.csv")

    assert len(df) == 10
    assert {"seller_id", "items_sold", "product_sales"}.issubset(df.columns)
from pathlib import Path
import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "outputs"


def test_all_reports_exist():
    expected_files = [
        "kpis.csv",
        "monthly_sales.csv",
        "top_categories.csv",
        "top_sellers.csv",
    ]

    for filename in expected_files:
        assert (OUTPUT_DIR / filename).is_file(), (
            f"Missing report: {filename}"
        )


def test_top_categories_has_ten_rows():
    df = pd.read_csv(OUTPUT_DIR / "top_categories.csv")

    assert len(df) == 10
    assert {"category", "items_sold", "product_sales"}.issubset(df.columns)


def test_top_sellers_has_ten_rows():
    df = pd.read_csv(OUTPUT_DIR / "top_sellers.csv")

    assert len(df) == 10
    assert {"seller_id", "items_sold", "product_sales"}.issubset(df.columns)

