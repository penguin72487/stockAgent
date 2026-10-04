from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from downloader.download_finmind_complement import Task, _store
from downloader.finmind_volume_units import annotate_stock_share_units


def test_finmind_daily_price_volume_is_already_shares() -> None:
    source = [{"date": "2026-09-24", "stock_id": "2330", "Trading_Volume": 14557662}]
    output, unit = annotate_stock_share_units("TaiwanStockPrice", source)
    assert output is source
    assert unit["source_volume_units"] == {"Trading_Volume": "shares"}
    assert output[0]["Trading_Volume"] == 14557662


def test_finmind_collateral_thousand_shares_gets_explicit_share_fields(tmp_path) -> None:
    source = [{"date": "2026-04-01", "stock_id": "2330", "market": "集中市場",
               "MarginBuy": 1268, "MarginCurrentDayBalance": 26353,
               "SettlementMarginBuy": 0}]
    task = Task("TaiwanStockLoanCollateralBalance", "", "2026-04-01", "day", 1, "pending")
    receipt = _store(tmp_path, task, source, datetime(2026, 9, 26, tzinfo=UTC))
    stored = pq.read_table(tmp_path / receipt["parquet_path"]).to_pylist()[0]
    assert stored["MarginBuy"] == 1268  # original thousand-share source
    assert stored["MarginBuy_shares"] == 1_268_000
    assert stored["MarginCurrentDayBalance_shares"] == 26_353_000
    assert stored["SettlementMarginBuy_shares"] == 0
    assert receipt["volume_units"]["source_volume_units"]["MarginBuy"] == "thousand_shares"
    assert receipt["volume_units"]["canonical_share_fields"]["MarginBuy_shares"] == "shares"


def test_finmind_unmapped_or_invalid_volume_never_becomes_shares() -> None:
    rows = [{"date": "2026-09-24", "volume": 5}]
    output, unit = annotate_stock_share_units("TaiwanFuturesDaily", rows)
    assert output is rows and unit["stock_share_unit_status"] == "not_mapped_do_not_assume_shares"
    with pytest.raises(ValueError, match="whole shares"):
        annotate_stock_share_units("TaiwanStockLoanCollateralBalance", [{"MarginBuy": 0.0001}])


@pytest.mark.parametrize("value", [-1, 1e40])
def test_finmind_collateral_rejects_negative_or_int64_overflow(value) -> None:
    with pytest.raises(ValueError, match="nonnegative Int64"):
        annotate_stock_share_units("TaiwanStockLoanCollateralBalance", [{"MarginBuy": value}])
