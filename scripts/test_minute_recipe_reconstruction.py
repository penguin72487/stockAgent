"""Temporary real-data reconstruction; retain evidence, not another panel."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.desync_snapshots import sha256_file
from scripts.shioaji_minute_backfill_state import source_fingerprint


def trial(source: Path, calendar: Path, reference: Path, symbols: list[str], receipt: Path):
    import polars as pl
    started = time.monotonic()
    summary = source/'download_summary.json'
    source_identity = source_fingerprint(source)
    summary_sha = sha256_file(summary)
    reference_sha = sha256_file(reference/'manifest.json')
    result = {'contract': 'temporary_canonical_minute_reconstruction_v1',
              'source_fingerprint_sha256': source_identity, 'summary_sha256': summary_sha,
              'reference_manifest_sha256': reference_sha, 'symbols': symbols,
              'scope': 'selected_symbols_all_available_dates_not_full_universe',
              'state': 'running', 'assembled_data_retained': False}
    atomic_write_json(receipt, result)
    owned = tempfile.TemporaryDirectory(prefix='stockagent-minute-recipe-')
    output = Path(owned.name)/'assembled'
    try:
        commands = [
            ['scripts/build_shioaji_tw_minute_dataset.py', '--input-root', str(source),
             '--calendar-root', str(calendar), '--output-root', str(output),
             '--symbols', ','.join(symbols)],
            ['scripts/audit_shioaji_tw_minute_dataset.py', '--dataset-root', str(output),
             '--calendar-root', str(calendar), '--all-partitions', '--allow-research-subset',
             '--output', str(output/'audit.json')],
        ]
        for command in commands:
            subprocess.run([sys.executable, '-m', command[0][:-3].replace('/', '.'),
                            *command[1:]], cwd=ROOT, check=True,
                           env={**os.environ, 'POLARS_MAX_THREADS': '4'})
        manifest = json.loads((output/'manifest.json').read_text())
        rows = 0
        null_metadata_alignment = 0
        for position, item in enumerate(manifest['partitions'], start=1):
            day = item['trade_date']
            rebuilt = pl.read_parquet(output/f'trade_date={day}/data.parquet').sort('ts','symbol')
            # Compare every column, not just prices or footer row counts.
            baseline = (pl.read_parquet(reference/f'trade_date={day}/data.parquet')
                        .filter(pl.col('symbol').is_in(symbols)).sort('ts','symbol'))
            # Whole-universe schema union may retain this optional raw lineage
            # field while both selected legacy symbols lack it. The canonical
            # reader inserts NULL for such metadata, not historical prices.
            # Align only a proven all-NULL optional field, never missing inputs,
            # non-NULL evidence, model features or executable labels.
            if 'volume_unit_proof' in baseline.columns and 'volume_unit_proof' not in rebuilt.columns:
                if baseline['volume_unit_proof'].null_count() != baseline.height:
                    raise RuntimeError('subset lost non-NULL volume lineage')
                rebuilt = rebuilt.with_columns(pl.lit(None, dtype=baseline.schema['volume_unit_proof'])
                                                .alias('volume_unit_proof'))
                null_metadata_alignment += 1
            if set(rebuilt.columns) == set(baseline.columns):
                rebuilt = rebuilt.select(baseline.columns)
            from polars.testing import assert_frame_equal
            assert_frame_equal(rebuilt, baseline, check_exact=True)
            rows += rebuilt.height
            if position % 200 == 0:
                print(f'[minute-recipe-trial] parity={position}/{len(manifest["partitions"])} '
                      f'rows={rows}', flush=True)
        if (source_fingerprint(source) != source_identity or sha256_file(summary) != summary_sha
                or sha256_file(reference/'manifest.json') != reference_sha):
            raise RuntimeError('source/reference changed during reconstruction')
        result.update(state='verified_exact_selected_symbol_parity', rows=rows,
                      partitions=len(manifest['partitions']),
                      all_null_optional_metadata_aligned_partitions=null_metadata_alignment,
                      output_manifest_sha256=sha256_file(output/'manifest.json'),
                      audit=json.loads((output/'audit.json').read_text()))
        return result
    except BaseException as exc:
        result.update(state='failed', error=type(exc).__name__+': '+str(exc))
        raise
    finally:
        owned.cleanup()
        result.update(trial_output_removed=not output.parent.exists(),
                      wall_seconds=time.monotonic()-started)
        atomic_write_json(receipt, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT/'data_tw_minute/shioaji_1m')
    parser.add_argument('--calendar', type=Path, default=ROOT/'data_tw_public')
    parser.add_argument('--reference', type=Path, default=ROOT/'data_tw_minute/research_dataset')
    parser.add_argument('--symbols', default='0050,2330')
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(trial(args.source, args.calendar, args.reference,
                           args.symbols.split(','), args.receipt), ensure_ascii=False))


if __name__ == '__main__':
    main()
