
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

