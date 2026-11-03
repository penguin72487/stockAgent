from datetime import date, datetime
import io
import json
from types import SimpleNamespace
import zipfile
from zoneinfo import ZoneInfo

import pytest

from scripts import capture_taifex_priority_preliminary as capture
from scripts import download_taifex_recent_index_derivatives_ticks as owner
from scripts.audit_taifex_preliminary_source import audit
from scripts.run_taifex_final_after_preliminary import compare
from downloader.artifact_io import sha256_file


DAY = date(2026, 10, 29)
NOW = datetime(2026, 10, 29, 15, 0, tzinfo=ZoneInfo('Asia/Taipei'))


def zipped(name, body):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, body.encode('cp950'))
    return stream.getvalue()


class Transport:
    def __init__(self):
        self.calls = []
        self.futures = ('https://www.taifex.com.tw/file/taifex/Dailydownload/'
                        'DailydownloadCSV/Daily_2026_10_29.zip')
        self.options = ('https://www.taifex.com.tw/file/taifex/Dailydownload/'
                        'OptionsDailydownloadCSV/OptionsDaily_2026_10_29.zip')

    def request_bytes(self, url, headers):
        self.calls.append(url)
        if url == owner.FUTURES_LISTING_URL:
            body = self.futures.encode()
        elif url == owner.OPTIONS_LISTING_URL:
            body = self.options.encode()
        elif url == self.futures:
            body = zipped('Daily_2026_10_29.csv',
                '成交日期,商品代號,到期月份,成交時間,成交價格,成交數量,近月價格,遠月價格,開盤集合競價\n'
                + '20261028,TX,202610,150001,123456,2,-,-,\n' * 3)
        elif url == self.options:
            body = zipped('OptionsDaily_2026_10_29.csv',
                '成交日期,商品代號,履約價格,到期月份,買賣權別,成交時間,成交價格,成交數量,開盤集合競價\n'
                '-----------------\n' + '20261028,TXO,23000,202610,C,150001,12,1,\n' * 3)
        else:
            raise AssertionError('unexpected request ' + url)
        return SimpleNamespace(body=body, headers={'last-modified': 'test'})


def test_preliminary_snapshot_preserves_canonical_head(tmp_path, monkeypatch):
    root = tmp_path / 'data_tw_index_derivatives_ticks'
    root.mkdir()
    (root / 'manifest.json').write_text('original canonical receipt')
    monkeypatch.setattr(capture, 'priority_dispatch_status', lambda *a, **kw: {'state': 'admitted'})
    output = root / 'research' / 'preliminary'
    transport = Transport()
    result = capture.capture(root, output, day=DAY, now=NOW, transport=transport)
    assert result['status'] == 'observed_provisional'
    assert result['complete_trading_day'] is False and result['training_ready'] is False
    assert result['independent_readback']['futures']['physical_date_rows'] == {'2026-10-28': 3}
    assert result['independent_readback']['options']['source_rows'] == 3
    assert (root / 'manifest.json').read_text() == 'original canonical receipt'
    assert (output / 'manifest.json').is_file()
    assert len(transport.calls) == 4
    checked = audit(output / 'manifest.json')
    assert checked['sources']['options']['source_rows'] == 3
    assert checked['sources']['futures']['physical_date_rows'] == {'2026-10-28': 3}
    assert checked['training_ready'] is False
    (output / 'OptionsDaily_2026_10_29.zip').write_bytes(b'changed')
    with pytest.raises(ValueError, match='archive or listing hash differs'):
        audit(output / 'manifest.json')


def test_capture_rejects_completed_day_and_unadmitted_capacity(tmp_path, monkeypatch):
    root = tmp_path / 'data_tw_index_derivatives_ticks'
    root.mkdir()
    transport = Transport()
    with pytest.raises(ValueError, match='pre-completion'):
        capture.capture(root, root / 'research' / 'late', day=DAY,
                        now=NOW.replace(hour=17), transport=transport)
    monkeypatch.setattr(capture, 'priority_dispatch_status', lambda *a, **kw: {'state': 'deferred'})
    with pytest.raises(RuntimeError, match='capacity not admitted'):
        capture.capture(root, root / 'research' / 'blocked', day=DAY, now=NOW, transport=transport)
    assert transport.calls == []


def test_completed_receipt_comparison_preserves_changed_csv(tmp_path, monkeypatch):
    root = tmp_path / 'data_tw_index_derivatives_ticks'
    root.mkdir()
    monkeypatch.setattr(capture, 'priority_dispatch_status', lambda *a, **kw: {'state': 'admitted'})
    output = root / 'research' / 'preliminary'
    transport = Transport()
    capture.capture(root, output, day=DAY, now=NOW, transport=transport)
    records = []
    for kind, prefix, url in [('futures', 'Daily', transport.futures),
                              ('options', 'OptionsDaily', transport.options)]:
        name = f'{prefix}_2026_10_29.zip'
        final = root / 'raw' / kind / name
        final.parent.mkdir(parents=True)
        if kind == 'futures':
            final.write_bytes((output / name).read_bytes())
        else:
            final.write_bytes(zipped('OptionsDaily_2026_10_29.csv', 'different final content'))
        records.append({'kind': kind, 'trading_date': DAY.isoformat(), 'path': str(final),
                        'sha256': sha256_file(final), 'bytes': final.stat().st_size, 'url': url})
    head = root / 'manifest.json'
    head.write_text(json.dumps({'dataset': 'taifex_recent_tx_txo_trade_by_trade',
                                'status': 'complete', 'raw_downloads': records}))
    report = tmp_path / 'comparison.json'
    result = compare(output / 'manifest.json', head, report)
    assert result['sources']['futures']['csv_contents_identical'] is True
    assert result['sources']['options']['csv_contents_identical'] is False
    assert result['training_ready'] is False and report.is_file()
