from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from scripts import audit_tw_option_priority_day as audit_module


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sources(tmp_path: Path, monkeypatch) -> tuple[Path, Path, Path]:
    monkeypatch.setattr(audit_module, "ROOT", tmp_path)
    for rel in ("scripts/audit_taifex_option_contract_presence.py",
                "scripts/audit_taifex_option_market_daily_contracts.py",
                "downloader/finmind_option_contract_keys.py"):
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("pinned fixture\n")
    official = tmp_path / "official.csv"
    fields = ["交易日期", "契約", "到期月份(週別)", "履約價", "買賣權", "開盤價", "成交量", "交易時段"]
    with official.open("w", encoding="cp950", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(fields)
        writer.writerows([
            ["2026/10/29", "TXO", "202612", "100", "買權", "2", "1", "一般"],
            ["2026/10/29", "TXO", "202612", "110", "賣權", "2", "1", "一般"],
            ["2026/10/29", "TGO", "202612", "120", "賣權", "2", "1", "一般"],
        ])
    official.with_suffix(".receipt.json").write_text(json.dumps({
        "contract": "taifex_option_all_products_daily_raw_v1", "range_start": "2026-10-29",
        "range_end": "2026-10-29", "point_in_time_snapshot": False,
        "path": str(official), "bytes": official.stat().st_size, "sha256": _hash(official),
    }))
    root = tmp_path / "finmind"
    data = root / "parquet/source.parquet"
    data.parent.mkdir(parents=True)
    pl.DataFrame({"option_id": ["TXO", "TXO"], "contract_date": ["202612", "202612"],
                  "strike_price": [100.0, 110.0], "call_put": ["CALL", "PUT"],
                  "volume": [1, 0]}).write_parquet(data)
    receipt = root / "receipts/TaiwanOptionDaily/all/2026-10-29.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({
        "dataset": "TaiwanOptionDaily", "data_id": "", "partition": "2026-10-29",
        "kind": "day", "status": "complete", "rows": 2,
        "parquet_path": "parquet/source.parquet", "parquet_size_bytes": data.stat().st_size,
        "sha256": _hash(data),
    }))
    return official, root, tmp_path / "artifacts/markets/result.json"


def test_real_contract_keys_distinguish_absent_and_zero_volume(tmp_path, monkeypatch):
    from datetime import date
    official, root, output = _sources(tmp_path, monkeypatch)
    result = audit_module.audit(date(2026, 10, 29), official, root, output)
    assert result["finmind_comparison"]["official_keys"] == 3
    assert result["finmind_comparison"]["missing_keys"] == 1
    assert result["finmind_comparison"]["missing_positive_volume_keys"] == 2
    assert result["finmind_missing_positive_products"] == ["TGO"]
    assert {r["missing_entirely"] for r in result["missing_positive_contracts"]} == {False, True}
    assert json.loads(output.read_text())["financial_or_training_admitted"] is False


def test_tampered_vendor_payload_fails_closed(tmp_path, monkeypatch):
    from datetime import date
    official, root, output = _sources(tmp_path, monkeypatch)
    receipt = root / "receipts/TaiwanOptionDaily/all/2026-10-29.json"
    values = json.loads(receipt.read_text())
    values["sha256"] = "0" * 64
    receipt.write_text(json.dumps(values))
    with pytest.raises(ValueError, match="FinMind independent readback failed"):
        audit_module.audit(date(2026, 10, 29), official, root, output)
    assert not output.exists()
