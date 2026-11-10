from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import sys

import polars as pl
import pytest

from scripts import download_taifex_final_settlement_history as downloader
from stockagent.data_sync import tw_option_raw_workspace_guard as guard
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError
from scripts.download_taifex_final_settlement_history import (
    parse_taifex_txo_final_settlement_html,
)


def test_parse_official_txo_final_settlement_table() -> None:
    body = """
    <html><body><table>
      <thead><tr><th>最後結算日</th><th>契約月份</th><th>最後結算價</th></tr></thead>
      <tbody>
        <tr><td>2013/05/02</td><td>201305W1</td><td>8,230</td></tr>
        <tr><td>2013/05/08</td><td>201305W2</td><td>8,350</td></tr>
      </tbody>
    </table></body></html>
    """.encode()

    result = parse_taifex_txo_final_settlement_html(
        body,
        start_date=date(2013, 5, 1),
        end_date=date(2013, 5, 31),
        source_file="receipt.html",
        source_sha256="a" * 64,
        source_url="https://www.taifex.com.tw/cht/5/optIndxFSP",
    )

    assert result.height == 2
    assert result["settlement_date"].to_list() == [
        date(2013, 5, 2),
        date(2013, 5, 8),
    ]
    assert result["option_series"].to_list() == ["201305W1", "201305W2"]
    assert result["final_settlement_price"].to_list() == [8_230, 8_350]


def test_live_settlement_receipt_and_projection_work_without_e(tmp_path, monkeypatch):
    repo = tmp_path/'repo'; output = repo/'data_tw_index_options_daily'
    output.mkdir(parents=True)
    monkeypatch.setattr(downloader, 'REPO_ROOT', repo)
    monkeypatch.setattr(guard, 'SOURCE', output/'raw')
    monkeypatch.setattr(guard, 'DESTINATION', tmp_path/'unmounted-E/raw')
    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.OPTIONS_RAW_NAME,))
    monkeypatch.setattr(handoff.volume, 'check', lambda: (_ for _ in ()).throw(
        AssertionError('live C settlement must not require E')))
    body = (b'<table><tr><th>date</th><th>contract</th><th>price</th></tr>'
            b'<tr><td>2013/05/02</td><td>201305W1</td><td>8230</td></tr></table>')
    def download(_url, target, **_kwargs):
        target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(body); return target
    monkeypatch.setattr(downloader, '_download_html', download)
    monkeypatch.setattr(sys, 'argv', ['download', '--output-dir', str(output),
        '--start-date', '2013-05-01', '--end-date', '2013-05-31', '--refresh'])
    assert downloader.main() == 0
    manifest = json.loads((output/'manifest_final_settlement.json').read_text())
    raw = Path(manifest['raw_receipt']['path'])
    assert raw.is_relative_to(output/'service_raw/final_settlement')
    assert manifest['raw_receipt']['sha256'] == hashlib.sha256(body).hexdigest()
    projection = pl.read_parquet(output/'txo_final_settlement_history.parquet')
    assert projection['final_settlement_price'].to_list() == [8230]
    assert projection['source_file'].to_list() == [str(raw)]
    assert projection['source_sha256'].to_list() == [hashlib.sha256(body).hexdigest()]


def test_custom_output_inside_handoff_still_requires_the_e_guard(tmp_path, monkeypatch):
    repo = tmp_path/'repo'; source = repo/'data_tw_index_options_daily/raw'
    source.mkdir(parents=True)
    monkeypatch.setattr(downloader, 'REPO_ROOT', repo)
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', tmp_path/'E/raw')
    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.OPTIONS_RAW_NAME,))
    monkeypatch.setattr(handoff, 'guard_path', lambda *_: (_ for _ in ()).throw(
        SnapshotError('source E mount is missing; C fallback refused')))
    monkeypatch.setattr(sys, 'argv', ['download', '--output-dir', str(source/'custom'),
        '--start-date', '2013-05-01', '--end-date', '2013-05-31'])
    monkeypatch.setattr(downloader, '_download_html', lambda *_args, **_kwargs: pytest.fail('network must not start'))
    with pytest.raises(SnapshotError, match='E mount is missing'):
        downloader.main()


def test_live_receipt_rejects_redirected_service_directory(tmp_path, monkeypatch):
    repo = tmp_path/'repo'; output = repo/'data_tw_index_options_daily'
    output.mkdir(parents=True)
    elsewhere = tmp_path/'other'; elsewhere.mkdir()
    (output/'service_raw').symlink_to(elsewhere, target_is_directory=True)
    monkeypatch.setattr(downloader, 'REPO_ROOT', repo)
    with pytest.raises(RuntimeError, match='redirected from C'):
        downloader._receipt_directory(output)
