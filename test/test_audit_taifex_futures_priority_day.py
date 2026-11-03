import json
from pathlib import Path
import zipfile

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.audit_taifex_futures_priority_day import audit
from scripts.audit_taifex_futures_priority_spreads import audit as audit_spreads
from scripts.audit_taifex_futures_historical_missing_spreads import audit as audit_historical_spreads


def test_source_presence_is_receipt_bound_and_keeps_physical_date(tmp_path: Path):
    daily = tmp_path / "daily.csv"
    daily.write_bytes(("交易日期,契約,到期月份(週別),交易時段,成交量\n"
                       "2020/10/28,TX,202011,一般,2\n"
                       "2020/10/28,ABC,202011,一般,1\n"
                       "2020/10/28,ABC,202012,一般,0\n").encode("cp950"))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"contract": "taifex_futures_daily_priority_source_v1",
        "request": {"queryStartDate": "2020/10/28", "queryEndDate": "2020/10/28"},
        "path": str(daily), "bytes": daily.stat().st_size, "sha256": sha256_file(daily),
        "source_rows": 3, "source_product_count": 2}))
    tick = tmp_path / "tick.zip"
    with zipfile.ZipFile(tick, "w") as archive:
        archive.writestr("tick.csv", ("成交日期,商品代號,到期月份(週別),成交價格,成交數量(B+S)\n"
                                      "20201027,ABC,202011,10,2\n"
                                      "20201028,TX,202011,0,2\n").encode("cp950"))
    manifest = tmp_path / "ticks.json"
    manifest.write_text(json.dumps({"status": "complete", "raw_downloads": [{"kind": "futures",
        "trading_date": "2020-10-28", "path": str(tick), "bytes": tick.stat().st_size,
        "sha256": sha256_file(tick)}]}))
    catalog = tmp_path / "catalog.csv"
    catalog.write_text("product,asset_class\nTX,index_future\n")
    result = audit(receipt, manifest, catalog, sha256_file(catalog), tmp_path / "output.json")
    assert result["source_products_outside_fixed_catalog"] == ["ABC"]
    assert result["positive_day_session_monthly_contract_keys"] == 2
    assert result["positive_keys_found_anywhere_in_trading_date_tick_archive"] == 1
    assert result["positive_keys_missing_from_trading_date_tick_archive"] == [["TX", "202011"]]
    assert result["tick_physical_date_rows"] == {"20201027": 1, "20201028": 1}

    root = tmp_path / 'provider'
    payload = root / 'parquet' / 'ticks.parquet'
    payload.parent.mkdir(parents=True)
    pl.DataFrame({'date': ['2020-10-28 09:00:00', '2020-10-28 09:01:00'],
                  'futures_id': ['TX', 'ABC'], 'contract_date': ['202011', '202011'],
                  'volume': [2, 0]}).write_parquet(payload)
    provider_receipt = root / 'receipts' / 'TaiwanFuturesTick' / 'all' / '2020-10-28.json'
    provider_receipt.parent.mkdir(parents=True)
    provider_receipt.write_text(json.dumps({'dataset': 'TaiwanFuturesTick', 'data_id': '',
        'partition': '2020-10-28', 'status': 'complete', 'rows': 2,
        'parquet_path': str(payload.relative_to(root)), 'parquet_size_bytes': payload.stat().st_size,
        'sha256': sha256_file(payload)}))
    compared = audit(receipt, manifest, catalog, sha256_file(catalog), tmp_path / 'compared.json',
                     finmind_root=root)['finmind_natural_day_comparison']
    assert compared['status'] == 'verified_source_payload'
    assert compared['positive_official_keys_found_only_in_finmind'] == 1
    assert compared['positive_official_keys_found_in_either_source'] == 2
    assert compared['positive_official_keys_missing_from_both_sources'] == []
    payload.write_bytes(payload.read_bytes() + b'tampered')
    invalid = audit(receipt, manifest, catalog, sha256_file(catalog), tmp_path / 'invalid.json',
                    finmind_root=root)['finmind_natural_day_comparison']
    assert invalid['status'] == 'receipt_or_payload_invalid'
    assert 'positive_official_keys_found_in_either_source' not in invalid
    tick.write_bytes(tick.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="tick archive differs"):
        audit(receipt, manifest, catalog, sha256_file(catalog), tmp_path / "other.json")


def _spread_source_bytes():
    return ("交易日期,契約,到期月份(週別),成交量,開盤價,最高價,最低價,收盤價,交易時段\n"
            "2016/01/28,DDF,201602,3,100,101,99,100,一般\n"
            "2016/01/28,DDF,201602/201603,3,1,1,1,1,一般\n"
            "2016/01/28,EEF,201602,5,100,101,99,100,一般\n"
            "2016/01/28,EEF,201602/201603,2,1,1,1,1,一般\n").encode("cp950")


def test_day_spread_readback_distinguishes_residual_and_rejects_source_drift(tmp_path: Path):
    daily = tmp_path / "daily.csv"
    daily.write_bytes(_spread_source_bytes())
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"contract": "taifex_futures_daily_priority_source_v1",
                                   "path": str(daily), "sha256": sha256_file(daily)}))
    report = tmp_path / "presence.json"
    report.write_text(json.dumps({"contract": "taifex_futures_priority_day_source_presence_v1",
        "trading_date": "2016-01-28", "input_pins": {str(receipt): sha256_file(receipt)},
        "positive_keys_missing_from_trading_date_tick_archive": [["DDF", "201602"], ["EEF", "201602"]]}))
    output, directory = tmp_path / "result.json", tmp_path / "evidence"
    result = audit_spreads(report, receipt, directory, output)
    assert result["reconciled_reasons"] == [{"official_reason": "outright_volume", "len": 1},
                                             {"official_reason": "spread_legs_only", "len": 1}]
    with pytest.raises(FileExistsError):
        audit_spreads(report, receipt, directory, output)
    daily.write_bytes(daily.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="daily source does not match receipt"):
        audit_spreads(report, receipt, tmp_path / "other-evidence", tmp_path / "other.json")
    assert not (tmp_path / "other-evidence").exists()


def test_historical_spread_readback_binds_unmatched_and_annual_original(tmp_path: Path):
    archive = tmp_path / "annual" / "2016_fut.zip"
    archive.parent.mkdir()
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("2016.csv", _spread_source_bytes())
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"receipts": [{"path": str(archive), "sha256": sha256_file(archive)}]}))
    unmatched = tmp_path / "unmatched.json"
    unmatched.write_text(json.dumps([{"date": "2016-01-28", "product": "DDF", "contract": "201602"},
                                     {"date": "2016-01-28", "product": "EEF", "contract": "201602"}]))
    official = tmp_path / "official.parquet"
    official.write_bytes(b"fixed prior audit input")
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"contract": "taifex_finmind_futures_contract_day_presence_v1",
        "unmatched_sha256": sha256_file(unmatched), "official_manifest_sha256": sha256_file(manifest),
        "official_path": str(official), "official_sha256": sha256_file(official),
        "unmatched_contract_days_on_verified_natural_dates": 2}))
    result = audit_historical_spreads(summary, unmatched, manifest, tmp_path / "evidence", tmp_path / "result.json")
    assert result["reconciled_reasons"] == {"outright_volume": 1, "spread_legs_only": 1}
    assert result["reconciled_reasons_by_product"]["DDF"] == {"spread_legs_only": 1}
    unmatched.write_text("[]")
    with pytest.raises(ValueError, match="fixed summary"):
        audit_historical_spreads(summary, unmatched, manifest, tmp_path / "other-evidence", tmp_path / "other.json")
    assert not (tmp_path / "other-evidence").exists()
