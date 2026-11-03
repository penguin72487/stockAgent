#!/usr/bin/env python3
"""Build a versioned, source-only two-clock window from retained option ZIPs.

The first source has no earlier retained state. A current candidate policy is
applied retrospectively, so this output is not a training dataset, historical
listing, market fill, complete last trade, or option-account contract.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo

import polars as pl

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_ticks import iter_official_option_trade_batches
from stockagent.data.tw_option_reference_candidates import compact_last_observations, reference_candidates
from stockagent.data_sync.cache_capacity import acquisition_dispatch_status


CONTRACT = 'taifex_option_observed_reference_source_window_v1'
SOURCE_COLUMNS = ('event_ts', 'product_raw', 'series_raw', 'strike_raw', 'right_raw',
                  'price_raw', 'source_sha256')


def derive_day(prior: pl.DataFrame | None, current: pl.DataFrame, day: date,
               windows: dict[str, tuple[int, int]]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Use a compact prior state and this archive only; preserve price ties."""
    current = current.select(*SOURCE_COLUMNS)
    if current.is_empty() or set(current['product_raw'].unique().to_list()) - set(windows):
        raise ValueError('source day has no rows or has products without candidate windows')
    if prior is not None:
        prior = prior.select(*SOURCE_COLUMNS)
        if prior.filter(pl.col('event_ts').dt.date() >= day).height:
            raise ValueError('prior source state crosses the decision date')
    cutoff = datetime(day.year, day.month, day.day, 9, tzinfo=ZoneInfo('Asia/Taipei'))
    morning = current.filter(pl.col('event_ts') + pl.duration(seconds=1) <= cutoff)
    before = morning if prior is None else pl.concat([prior, morning])
    if before.is_empty():
        raise ValueError('no observed rows are available by the latest decision clock')
    outputs = []
    for clock in ('08:45', '09:00'):
        frame = reference_candidates(before, trading_date=day, decision_time=clock, session_windows=windows)
        outputs.append(frame.with_columns(pl.lit(day).cast(pl.Date).alias('decision_date'),
                                          pl.lit(clock).alias('decision_time')))
    all_rows = current if prior is None else pl.concat([prior, current])
    return pl.concat(outputs), compact_last_observations(all_rows)


def pin_source_window(manifests: list[Path], start: date, end: date) -> tuple[list[dict], dict[str, str]]:
    """Union retained receipt versions, rejecting conflicting observations for a day."""
    paths = [path.resolve() for path in manifests]
    if not paths or len(paths) != len(set(paths)):
        raise ValueError('distinct canonical receipt versions are required')
    manifest_pins = {str(path): sha256_file(path) for path in paths}
    by_day: dict[str, dict] = {}
    for path in paths:
        doc = json.loads(path.read_bytes())
        days = sorted(date.fromisoformat(item['trading_date']) for item in doc['raw_downloads']
                      if item['kind'] == 'options' and start <= date.fromisoformat(item['trading_date']) <= end)
        if len(days) != len(set(days)):
            raise ValueError('duplicate date in original archive inventory')
        for item in pin_official_option_tick_inventory(path, days):
            previous = by_day.get(item['date'])
            if previous is not None and any(previous[key] != item[key] for key in ('path', 'sha256', 'bytes', 'url')):
                raise ValueError('overlapping receipts disagree on the original archive')
            # Keep the earliest supplied receipt when the raw identity agrees.
            # Each day still references a real native receipt, never a merged one.
            by_day.setdefault(item['date'], item)
    if not by_day:
        raise ValueError('a nonempty original archive inventory is required')
    if any(sha256_file(Path(path)) != digest for path, digest in manifest_pins.items()):
        raise ValueError('source receipt changed while pinning the window')
    return [by_day[day] for day in sorted(by_day)], manifest_pins


def build(manifest: Path, policy_path: Path, start: date, end: date, output: Path,
          *, additional_manifests: tuple[Path, ...] = ()) -> dict:
    if start > end or output.exists() or output.is_symlink():
        raise ValueError('an ordered date range and new output directory are required')
    manifest_sha, policy_sha = sha256_file(manifest), sha256_file(policy_path)
    policy = json.loads(policy_path.read_bytes())
    if (policy.get('contract') != 'taifex_option_session_policy_candidate_v1'
            or policy.get('research_candidate_only') is not True):
        raise ValueError('an explicit research-only candidate policy is required')
    windows = {key: tuple(value['day_minutes']) for key, value in policy['products'].items()}
    linked_pins = {}
    for name, fields in [('inventory_comparison_pins', (('inventory_path', 'inventory_sha256'),
                                                        ('receipt_path', 'receipt_sha256'))),
                         ('current_catalog_comparison_pins', (('raw_path', 'raw_sha256'),
                                                               ('receipt_path', 'receipt_sha256')))]:
        for path_field, hash_field in fields:
            evidence = policy.get(name, {})
            path = Path(evidence[path_field]).resolve()
            if sha256_file(path) != evidence[hash_field]:
                raise ValueError('candidate-policy linked evidence changed')
            linked_pins[str(path)] = evidence[hash_field]
    sources, manifest_pins = pin_source_window([manifest, *additional_manifests], start, end)
    days = [date.fromisoformat(item['date']) for item in sources]
    implementations = [Path(__file__).resolve(), Path('stockagent/data/tw_option_product_ticks.py').resolve(),
                       Path('stockagent/data/tw_option_reference_candidates.py').resolve()]
    implementation_pins = {str(p): sha256_file(p) for p in implementations}
    states, outputs, day_rows = None, [], []
    for day, source in zip(days, sources, strict=True):
        parts = [frame.select(*SOURCE_COLUMNS) for frame in iter_official_option_trade_batches(source)]
        if not parts:
            raise ValueError('source archive has no observed option rows')
        raw = pl.concat(parts)
        result, states = derive_day(states, raw, day, windows)
        outputs.append(result)
        day_rows.append({'day': day.isoformat(), 'source_side_rows': raw.height,
                         'rows_by_clock': dict(Counter(result['decision_time'].to_list())),
                         'retained_state_rows': states.height})
        print(json.dumps(day_rows[-1]), flush=True)
    result = pl.concat(outputs).sort('decision_date', 'decision_time', 'product_raw', 'series_raw',
                                     'strike_decimal', 'right_raw')
    if (manifest_pins[str(manifest.resolve())] != manifest_sha
            or any(sha256_file(Path(path)) != digest for path, digest in manifest_pins.items())
            or sha256_file(policy_path) != policy_sha
            or any(sha256_file(Path(path)) != digest for path, digest in linked_pins.items())
            or any(sha256_file(Path(path)) != digest for path, digest in implementation_pins.items())
            or any(sha256_file(Path(row['path'])) != row['sha256'] for row in sources)):
        raise ValueError('source or implementation changed during the complete window')
    capacity = acquisition_dispatch_status(manifest.parent.resolve(), maximum_growth_bytes=128 * 1024 * 1024)
    if capacity.get('state') not in {'admitted', 'not_enrolled'}:
        raise RuntimeError(f"physical capacity not admitted: {capacity.get('reason')}")
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'observed_references.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.reference-', suffix='.tmp', delete=False) as stream:
        temporary = Path(stream.name)
    try:
        result.write_parquet(temporary, compression='zstd', statistics=True)
        restored = pl.read_parquet(temporary)
        if not result.equals(restored):
            raise ValueError('independent source-window parquet readback differs')
        durable_replace(temporary, payload)
    finally:
        temporary.unlink(missing_ok=True)
    data_identity = {'contract': CONTRACT, 'source_manifest_sha256': manifest_sha,
                     'policy_sha256': policy_sha, 'source_sha256s': [row['sha256'] for row in sources],
                     'implementation_sha256': implementation_pins}
    if additional_manifests:
        data_identity['additional_source_manifest_sha256s'] = [manifest_pins[str(path.resolve())]
                                                               for path in additional_manifests]
    fingerprint = hashlib.sha256(json.dumps(data_identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    receipt = {'contract': CONTRACT, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'requested_window': [start.isoformat(), end.isoformat()],
               'archive_days': [day.isoformat() for day in days], 'data_fingerprint': fingerprint,
               'source_manifest': str(manifest.resolve()), 'source_manifest_sha256': manifest_sha,
               'additional_source_manifests': [dict(path=str(path.resolve()), sha256=manifest_pins[str(path.resolve())])
                                               for path in additional_manifests],
               'source_pins': sources, 'policy_path': str(policy_path.resolve()), 'policy_sha256': policy_sha,
               'policy_linked_pins': linked_pins, 'implementation_sha256': implementation_pins,
               'data_path': str(payload.resolve()), 'data_sha256': sha256_file(payload),
               'rows': result.height, 'daily_counts': day_rows, 'training_ready': False,
               'limitations': ['Research candidates from fixed observed files only; no live PIT receipt.',
                 'Policy uses current evidence retrospectively and does not prove historic sessions or listing.',
                 'No prior source before the first selected archive; later missing archives are not inferred.',
                 'Observed keys may have expired or be otherwise ineligible at the decision.',
                 'Whole-second source ties are ambiguous; no order, quote, fill or margin is inferred.',
                 'No expiration, risk, fees, account, model, checkpoint or training admission is implied.']}
    atomic_write_json(output / 'manifest.json', receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--additional-manifest', type=Path, action='append', default=[],
                        help='additional retained canonical receipt version; repeat to include older days')
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = build(args.manifest.resolve(), args.policy.resolve(), args.start, args.end, args.output.resolve(),
                   additional_manifests=tuple(path.resolve() for path in args.additional_manifest))
    print(json.dumps({'manifest': str(args.output / 'manifest.json'), 'rows': result['rows'],
                      'training_ready': False}), flush=True)


if __name__ == '__main__':
    main()
