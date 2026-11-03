"""Retained official receipt versions must produce a traceable window."""
from datetime import date
import json

import pytest

from downloader.artifact_io import sha256_file
from scripts.build_taifex_option_reference_source_window import pin_source_window


def write_receipt(path, raw_root, entries):
    raw = []
    for day, digest in entries:
        name = f"OptionsDaily_{day.replace('-', '_')}.zip"
        raw.append({'kind': 'options', 'trading_date': day, 'path': str(raw_root / name),
                    'url': 'https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/' + name,
                    'bytes': 123, 'sha256': digest})
    path.write_text(json.dumps({'dataset': 'taifex_recent_tx_txo_trade_by_trade',
                                'status': 'complete', 'raw_downloads': raw}))


def test_retained_receipt_union_pins_each_day_to_its_original_receipt(tmp_path):
    older, newer = tmp_path / 'old.json', tmp_path / 'new.json'
    write_receipt(older, tmp_path, [('2026-09-09', 'a' * 64), ('2026-09-10', 'b' * 64)])
    write_receipt(newer, tmp_path, [('2026-09-10', 'b' * 64), ('2026-09-11', 'c' * 64)])
    pins, manifests = pin_source_window([older, newer], date(2026, 9, 9), date(2026, 9, 11))
    assert [p['date'] for p in pins] == ['2026-09-09', '2026-09-10', '2026-09-11']
    assert [p['receipt_path'] for p in pins] == [str(older), str(older), str(newer)]
    assert [p['receipt_sha256'] for p in pins] == [sha256_file(older), sha256_file(older), sha256_file(newer)]
    assert manifests == {str(older): sha256_file(older), str(newer): sha256_file(newer)}


def test_retained_receipt_union_rejects_a_changed_overlapping_archive(tmp_path):
    older, newer = tmp_path / 'old.json', tmp_path / 'new.json'
    write_receipt(older, tmp_path, [('2026-09-10', 'a' * 64)])
    write_receipt(newer, tmp_path, [('2026-09-10', 'b' * 64)])
    with pytest.raises(ValueError, match='overlapping receipts disagree'):
        pin_source_window([older, newer], date(2026, 9, 10), date(2026, 9, 10))
    with pytest.raises(ValueError, match='distinct canonical receipt'):
        pin_source_window([older, older], date(2026, 9, 10), date(2026, 9, 10))


def test_retained_receipt_union_rejects_repeated_days_inside_one_receipt(tmp_path):
    receipt = tmp_path / 'receipt.json'
    write_receipt(receipt, tmp_path, [('2026-09-10', 'a' * 64), ('2026-09-10', 'a' * 64)])
    with pytest.raises(ValueError, match='duplicate date'):
        pin_source_window([receipt], date(2026, 9, 10), date(2026, 9, 10))
