from datetime import date
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file
from scripts.audit_finlab_dated_version_inventory import inventory


def _source(root: Path, receipt: Path, name: str, days: list[date]) -> None:
    obj = root / 'datasets' / f'{name}.parquet'
    obj.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': days, 'value': list(range(len(days)))}).write_parquet(obj)
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps({'dataset': 'tw_option_daily_delta', 'parquet_path': str(obj.relative_to(root)),
                                   'sha256': sha256_file(obj), 'rows': len(days)}))


def test_verified_rolling_versions_locate_dates_lost_from_current_head(tmp_path: Path):
    current = tmp_path / 'receipts' / 'tw_option_daily_delta-example.json'
    old = tmp_path / 'versions' / current.stem / 'old.json'
    _source(tmp_path, current, 'new', [date(2026, 10, 2), date(2026, 10, 3)])
    _source(tmp_path, old, 'old', [date(2026, 10, 1), date(2026, 10, 2)])
    result = inventory(tmp_path, current, 'tw_option_daily_delta')
    assert result['current_head_verified']
    assert result['verified_versions'] == 2
    assert result['verified_event_dates_any_version'] == 3
    assert result['event_dates_absent_from_current_but_in_verified_versions'] == ['2026-10-01']
    assert not result['failed_versions']


def test_modified_old_object_is_reported_and_not_counted(tmp_path: Path):
    current = tmp_path / 'receipts' / 'tw_option_daily_delta-example.json'
    old = tmp_path / 'versions' / current.stem / 'old.json'
    _source(tmp_path, current, 'new', [date(2026, 10, 2)])
    _source(tmp_path, old, 'old', [date(2026, 10, 1)])
    (tmp_path / 'datasets' / 'old.parquet').write_bytes(b'changed')
    result = inventory(tmp_path, current, 'tw_option_daily_delta')
    assert result['verified_versions'] == 1
    assert result['event_dates_absent_from_current_but_in_verified_versions'] == []
    assert result['failed_versions'][0]['reason'] == 'object SHA mismatch'
