"""A targeted recheck cannot shrink the owning provider's global evidence."""
import json
from types import SimpleNamespace

import pytest
from downloader import download_okx_perp_daily as okx
from downloader import download_bybit_perp_daily as bybit


@pytest.mark.parametrize('module', [okx, bybit])
def test_quality_subset_keeps_global_reports_and_catalog(tmp_path, monkeypatch, module):
    output = tmp_path / 'source'
    output.mkdir()
    global_names = ['symbols.csv', 'progress.json', 'download_report.csv', 'download_summary.json',
                    'historical_feature_report.csv', 'okx_historical_feature_catalog.json',
                    'historical_feature_summary.json', 'candles_download_summary.json']
    for name in global_names:
        (output / name).write_bytes(b'preserved full source evidence')
    values = dict.fromkeys(module.SymbolRecord.__dataclass_fields__)
    values.update(code='BTCUSDT', name='BTC', market='perpetual')
    if module is okx:
        values.update(okx_symbol='BTC-USDT-SWAP')
        factory, fetcher, native_symbol = 'OkxClient', '_fetch_swap_symbols', 'BTC-USDT-SWAP'
    else:
        values.update(bybit_symbol='BTCUSDT', category='linear')
        factory, fetcher, native_symbol = 'BybitClient', '_fetch_perp_symbols', 'BTCUSDT'
    record = module.SymbolRecord(**values)
    client = SimpleNamespace(request_interval=0.1, limiter_activity=lambda: {})
    monkeypatch.setattr(module, factory, lambda **_: client)
    monkeypatch.setattr(module, fetcher, lambda *_a, **_k: [record])
    if module is bybit:
        monkeypatch.setattr(module, '_stored_parquet_inventory', lambda *_a, **_k: {})
    seen = []
    def download(_client, item, _path, *_args, **_kwargs):
        seen.append(item.code)
        return module.DownloadResult('crypto_perp', item.code, native_symbol,
                                      item.market, 'skipped_up_to_date', 100, None)
    monkeypatch.setattr(module, '_download_symbol_1m', download)
    argv = ['collector', '--output-dir', str(output), '--symbols', native_symbol,
            '--start-date', '2020-01-01', '--end-date', '2020-01-01', '--workers', '1']
    if module is okx:
        argv.append('--skip-historical-features')
    monkeypatch.setattr(module.sys, 'argv', argv)
    module.main()
    assert seen == ['BTCUSDT']
    assert all((output / name).read_bytes() == b'preserved full source evidence' for name in global_names)
    paths = list((output / 'quality_repair_runs').glob('*/download_summary.json'))
    assert len(paths) == 1
    assert json.loads(paths[0].read_text())['provider_scope_is_complete'] is False
