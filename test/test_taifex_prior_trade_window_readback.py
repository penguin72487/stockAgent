from datetime import date, datetime
from decimal import Decimal
import zipfile

from scripts.verify_taifex_futures_prior_trade_window import scan_archive


def test_csv_readback_keeps_both_boundaries_and_all_prices_in_a_second(tmp_path):
    path = tmp_path / "Daily_2026_09_24.zip"
    data = """date,product,month,time,price,quantity,near,far,opening
20260923,TX,202610,235859,22000,2,-,-,
20260923,TX,202610,235900,22001,2,-,-,
20260924,TX,202610,084459,22050,2,-,-,
20260924,TX,202610,084459,22051,2,-,-,
20260924,TX,202610,084500,22099,2,-,-,
20260924,MTX,202610,084459,1,2,-,-,
"""
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("Daily_2026_09_24.csv", data)
    rows = scan_archive(path, date(2026, 9, 24), {("TX", "202610")})
    assert rows[("TX", "202610")]["midnight"] == (
        datetime(2026, 9, 23, 23, 58, 59), {Decimal("22000")})
    assert rows[("TX", "202610")]["decision"] == (
        datetime(2026, 9, 24, 8, 44, 59), {Decimal("22050"), Decimal("22051")})
