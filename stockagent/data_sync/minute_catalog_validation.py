"""Terminal coverage and observed-tick lineage, separate from bar semantics."""
from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.source_validation import (
    VerificationCache, canonical_chunk_path, read_object, validator_identity,
)


def validate_stock_minute_catalog(root: Path, cache_path: Path) -> dict:
    """Every requested chunk is terminal; tick fallback retains real evidence.

    A declared source gap is terminal for collection, not complete for training.
    Filenames are derived from the selected root, never producer absolute paths.
    The full bar audit remains owned by audit_stock_minute_source.
    """
    from downloader import download_shioaji_tw_minute_kbars as collector
    summary = read_object(root/'download_summary.json')
    expected_chunks = {(a.isoformat(), b.isoformat()) for a, b in collector.iter_date_chunks(
        date.fromisoformat(summary['start_date']), date.fromisoformat(summary['end_date']),
        int(summary['chunk_days']))}
    with (root/'download_report.csv').open(newline='', encoding='utf-8') as stream:
        rows = list(csv.DictReader(stream))
    cache = VerificationCache(cache_path, validator_identity(Path(__file__)))
    available = receipts = tick_sources = 0
    try:
        for row in rows:
            if row['status'] == 'contract_unavailable':
                continue
            symbol = row['symbol']
            manifest = read_object(root/'symbols'/f'{symbol}.manifest.json')
            keys = {(c['start_date'], c['end_date']) for c in manifest['chunks']}
            if keys != expected_chunks or len(keys) != len(manifest['chunks']):
                raise SnapshotError(f'minute requested chunk coverage mismatch: {symbol}')
            returned, gaps = set(), set()
            for entry in manifest['chunks']:
                start, end = entry['start_date'], entry['end_date']
                receipt = read_object(canonical_chunk_path(root, symbol, start, end,
                                                          suffix='.receipt.json'))
                if (receipt.get('storage_frequency') != collector.STORAGE_FREQUENCY
                        or sorted(receipt.get('source_gap_dates', [])) != sorted(entry.get('source_gap_dates', []))):
                    raise SnapshotError(f'minute receipt coverage mismatch: {symbol}/{start}')
                for key, target in (('returned_dates', returned), ('source_gap_dates', gaps)):
                    for value in receipt.get(key, []):
                        if not start <= date.fromisoformat(value).isoformat() <= end:
                            raise SnapshotError(f'minute coverage date outside chunk: {symbol}/{value}')
                        target.add(value)
                sources = receipt.get('raw_tick_sources', [])
                if (receipt.get('underlying_data_method') in {
                        'observed_ticks_aggregated_to_right_labelled_1m',
                        'mixed_provider_kbars_and_observed_ticks'} and not sources):
                    raise SnapshotError(f'tick recovery has no retained trade evidence: {symbol}/{start}')
                for source in sources:
                    day, expected = source['session_date'], source['sha256']
                    if (source['symbol'] != symbol or len(expected) != 64
                            or not all(c in '0123456789abcdef' for c in expected)
                            or not start <= date.fromisoformat(day).isoformat() <= end):
                        raise SnapshotError('invalid tick lineage identity')
                    path = root/'raw_tick_recovery'/symbol/f'{day}.{expected}.parquet'
                    selected = path.resolve(strict=True)
                    if (not selected.is_relative_to(root.resolve(strict=True))
                            or selected.stat().st_size != source['size']):
                        raise SnapshotError('tick lineage escapes release or size differs')
                    cache.check(selected, expected, lambda: {'retained_trade_evidence': True})
                    tick_sources += 1
                receipts += 1
            if (sorted(gaps) != manifest['source_gap_dates']
                    or sorted(returned | gaps) != manifest['terminal_coverage_dates']
                    or (row['status'] == 'complete') != (not gaps)):
                raise SnapshotError(f'minute terminal coverage differs from receipts: {symbol}')
            available += 1
        return {'contract': 'terminal_minute_chunk_and_tick_lineage_v1',
                'available_symbols': available, 'receipts': receipts,
                'retained_tick_sources': tick_sources, 'checked_tick_files': cache.checked,
                'reused_tick_files': cache.reused, 'validator_sha256': cache.contract,
                'coverage_complete_claim': False}
    finally:
        cache.close()
