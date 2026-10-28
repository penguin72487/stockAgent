import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import zipfile

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.audit_simulateio_futures_ticks import compare


def test_tick_comparison_keeps_offset_and_extra_rows_diagnostic_only(tmp_path: Path):
    root = tmp_path / "complement"
    receipt = root / "receipts/TaiwanFuturesTick/all/2013-12-25.json"
    payload = root / "parquet/TaiwanFuturesTick/all/2013-12-25/native.parquet"
    receipt.parent.mkdir(parents=True)
    payload.parent.mkdir(parents=True)
    pl.DataFrame({"date": ["2013-12-25 08:45:00", "2013-12-25 08:45:00"],
                  "futures_id": ["MTX", "MTX"], "contract_date": ["201401", "201402"],
                  "price": [8496.0, 8400.0], "volume": [352, 10]}).write_parquet(payload)
    receipt.write_text(json.dumps({"dataset": "TaiwanFuturesTick", "partition": "2013-12-25",
        "status": "complete", "historical_point_in_time": False,
        "parquet_path": str(payload.relative_to(root)), "sha256": sha256_file(payload)}))
    local = ZoneInfo("Asia/Taipei")
    def ms(hour, minute):
        return int(datetime(2013, 12, 25, hour, minute, tzinfo=local).timestamp() * 1000)
    candidate = tmp_path / "continuous.tic"
    rows = [{"m": ms(12, 0), "s": 9, "c": 8550, "v": 2},
            {"m": ms(8, 45), "s": 10, "c": 8542, "v": 176}]
    with zipfile.ZipFile(candidate, "w") as archive:
        archive.writestr("continuous.rpt", json.dumps(rows))

    kwargs = dict(day="2013-12-25", product="MTX", contract="201401", shift=46,
                  candidate_volume_multiplier=2)
    result = compare(candidate, receipt, **kwargs)
    assert result["candidate"]["timestamp_regressions_in_native_order"] == 1
    assert result["candidate"]["first_session_record"]["price"] == 8542
    assert result["comparison"]["unadjusted_matched_rows"] == 0
    assert result["comparison"]["diagnostic_matched_rows"] == 1
    assert result["comparison"]["diagnostic_candidate_only_rows"] == 1
    assert result["admitted_to_execution"] is False
    assert result["finmind"]["historical_point_in_time_verified"] is False

    payload.write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="hash differs"):
        compare(candidate, receipt, **kwargs)
