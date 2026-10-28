#!/usr/bin/env python3
"""Prepare paired 08:45/09:00 carry entries without daily-OPEN substitution.

Reuse receipt-verified full-session minutes and exact-contract KBar readers.
Missing contract-days block admission unless an exact source-bound quarantine
receipt explicitly disables their orders while retaining official valuation.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_decision_clock import (
    ASSET_SCOPES, OPENING_REFERENCE_DATASET, clock_asset_classes, clock_dataset, decision_clock_contract,
    opening_reference_contract, resolve_opening_references, observed_window_entries,
    validate_execution_quarantine,
    bind_entry_specifications, dated_entry_sessions,
)
from stockagent.data.tw_stock_futures_history import _reuse_verified_contract
from stockagent.data.tw_stock_futures_kbars import read_futures_kbar_sources


def _entry_minute_filter(window_minutes):
    return (((pl.col('minute') >= 527) & (pl.col('minute') <= 526 + window_minutes))
        | ((pl.col('minute') >= 542) & (pl.col('minute') <= 541 + window_minutes)))


def _complete_kbar_days(days, gaps):
    """Keep only receipt-complete days, including a correctly typed empty set."""
    rejected = pl.DataFrame([
        dict(date=str(g['date']), physical_contract=g['physical_contract']) for g in gaps
    ], schema={'date': pl.String, 'physical_contract': pl.String}).with_columns(
        pl.col('date').str.to_date())
    return days.select('date', 'physical_contract').join(
        rejected.unique(), on=['date', 'physical_contract'], how='anti')


def _load_clock_history(scan, end, alias_columns, reference_columns, bulk_columns):
    """Preserve the original history scan, including its checkpoint byte identity."""
    return scan.filter(pl.col('date') <= end).select(
        'date', 'product', 'contract', 'asset_class', 'physical_contract', 'symbol',
        'open', 'high', 'low', 'close', 'volume', 'executable',
        *alias_columns, *reference_columns, *bulk_columns).collect()


def _bulk_checkpoint_read(path, identity, sources, workers):
    """Reuse reductions only after all original raw objects are read back."""
    from concurrent.futures import ThreadPoolExecutor
    manifest_path = path/'manifest.json'
    if not manifest_path.is_file():
        return None
    manifest_sha = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if manifest.get('contract') != 'verified_bulk_window_checkpoint_v1' or manifest['identity'] != identity:
        raise ValueError('bulk checkpoint identity differs')
    def verify(item):
        member = Path(item['path'])
        before = member.stat()
        if sha256_file(member) != item['sha256']:
            raise ValueError('bulk checkpoint original raw SHA mismatch')
        after = member.stat()
        fields = lambda st: (st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
        if fields(before) != fields(after):
            raise ValueError('bulk checkpoint raw changed during readback')
        return member, fields(after)
    pinned_members = {}
    def add_pin(member):
        path, digest = str(member['path']), member['sha256']
        if path in pinned_members and pinned_members[path] != digest:
            raise ValueError('bulk checkpoint gives one raw path conflicting SHA values')
        pinned_members[path] = digest
    for source in sources:
        add_pin(source)
        if source.get('receipt_path'):
            add_pin(dict(path=source['receipt_path'], sha256=source['receipt_sha256']))
        if source.get('inventory_path'):
            add_pin(dict(path=source['inventory_path'], sha256=source['inventory_sha256']))
        for product in source.get('products', []):
            add_pin(product)
            add_pin(dict(path=product['receipt_path'], sha256=product['receipt_sha256']))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        originals = list(pool.map(verify, [dict(path=path, sha256=digest)
                                          for path, digest in pinned_members.items()]))
    frames = {}
    for name, item in manifest['outputs'].items():
        member = path/(name+'.parquet')
        if not member.resolve().is_relative_to(path.resolve()) or sha256_file(member) != item['sha256']:
            raise ValueError('bulk checkpoint output SHA/path mismatch')
        frames[name] = pl.read_parquet(member)
        if sha256_file(member) != item['sha256']:
            raise ValueError('bulk checkpoint output changed during readback')
    for member, expected in originals:
        st = member.stat()
        if (st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns) != expected:
            raise ValueError('bulk checkpoint original changed while loading reductions')
    if sha256_file(manifest_path) != manifest_sha:
        raise ValueError('bulk checkpoint manifest changed during readback')
    return frames


def _bulk_checkpoint_write(path, identity, frames):
    """Publish one source-bound checkpoint after every full-session check."""
    path.mkdir(parents=True, exist_ok=True)
    if (path/'manifest.json').exists():
        raise FileExistsError('completed bulk checkpoints are immutable')
    outputs = {}
    for name, frame in frames.items():
        member = path/(name+'.parquet')
        atomic_write_parquet(member, frame)
        outputs[name] = dict(sha256=sha256_file(member), rows=frame.height)
    atomic_write_json(path/'manifest.json', dict(contract='verified_bulk_window_checkpoint_v1',
        identity=identity, outputs=outputs))


def _bulk_checkpoint_parent(path, source_receipt, current_identity):
    """Bind a previous verified core to identical inputs, windows and code."""
    from stockagent.runtime_identity import verify_source_release
    path, source_receipt = Path(path).resolve(), Path(source_receipt).resolve()
    source_root = source_receipt.parent/'build-source'
    source_proof = verify_source_release(source_receipt, source_root)
    source_files = json.loads(source_receipt.read_text())['code']['files']
    manifest = path/'manifest.json'
    before = sha256_file(manifest)
    doc = json.loads(manifest.read_text())
    previous = doc.get('identity', {})
    fingerprint = hashlib.sha256(json.dumps(previous,sort_keys=True).encode()).hexdigest()
    if doc.get('contract') != 'verified_bulk_window_checkpoint_v1' or path.name != fingerprint:
        raise ValueError('parent bulk checkpoint identity is invalid')
    fields = ('source_frame_sha256','block_sha256','objects','asset_scope','window_minutes',
              'entry_windows','streaming','polars_version')
    if any(previous.get(k) != current_identity.get(k) for k in fields):
        raise ValueError('parent bulk checkpoint source, scope or runtime differs')
    loader = 'scripts/prepare_tw_futures_decision_clocks.py'
    if previous.get('loader_sha256') != source_files.get(loader):
        raise ValueError('parent bulk checkpoint loader differs from frozen release')
    implementation = previous.get('implementation_sha256s', {})
    expected = {'stockagent/data/tw_futures_decision_clock.py',
                'stockagent/data/tw_futures_finmind_ticks.py',
                'stockagent/data/tw_stock_futures_repair.py',
                'stockagent/data/tw_price_rules.py'}
    optional = {'stockagent/data/tw_futures_official_ticks.py',
                'scripts/download_taifex_recent_index_derivatives_ticks.py'}
    observed = set()
    for name, digest in implementation.items():
        try:
            relative = str(Path(name).resolve().relative_to(source_root.resolve()))
        except ValueError as exc:
            raise ValueError('parent bulk implementation escapes frozen release') from exc
        if relative not in expected | optional or source_files.get(relative) != digest:
            raise ValueError('parent bulk implementation differs from frozen release')
        observed.add(relative)
    if observed & optional:
        expected |= optional
    if observed != expected or sha256_file(manifest) != before:
        raise ValueError('parent bulk checkpoint implementation or manifest changed')
    return previous, dict(contract='verified_prior_core_reuse_v1', checkpoint=str(path),
        checkpoint_manifest_sha256=before, frozen_source=source_proof,
        rule='identical_input_scope_runtime_and_raw_readback_previous_implementation_retained')


def _validate_bulk_delta(previous, current, changed_days):
    """A whole date is the smallest safely recomputable raw-tape partition."""
    days = sorted(set(changed_days))
    if not days or len(days) != len(changed_days) or previous.schema != current.schema:
        raise ValueError('bulk delta requires unique dates and the identical source schema')
    keys = ['date', 'physical_contract']
    if previous.select(keys).is_duplicated().any() or current.select(keys).is_duplicated().any():
        raise ValueError('bulk delta requires unique dated physical identities')
    outside = lambda frame: frame.filter(~pl.col('date').is_in(days)).sort(keys)
    if not outside(previous).equals(outside(current)):
        raise ValueError('bulk delta changes source data outside its explicit dates')
    for day in days:
        old = previous.filter(pl.col('date') == day).sort(keys)
        new = current.filter(pl.col('date') == day).sort(keys)
        if old.is_empty() or new.is_empty() or old.equals(new):
            raise ValueError('bulk delta date must contain a real source change in both releases')
    return days


def _validate_bulk_prepend(previous, current, added_days):
    """Only add complete earlier dates to an otherwise identical parent."""
    days = sorted(set(added_days))
    if not days or len(days) != len(added_days) or previous.schema != current.schema:
        raise ValueError('bulk prepend requires unique dates and the identical source schema')
    keys = ['date', 'physical_contract']
    if any(frame.select(keys).null_count().row(0) != (0, 0) for frame in (previous, current)):
        raise ValueError('bulk prepend requires non-null dated physical identities')
    if previous.is_empty() or previous.select(keys).is_duplicated().any() or current.select(keys).is_duplicated().any():
        raise ValueError('bulk prepend requires unique dated physical identities and a nonempty parent')
    if days[-1] >= previous['date'].min():
        raise ValueError('bulk prepend dates must all precede the parent')
    current_days = set(current['date'].unique().to_list())
    previous_days = set(previous['date'].unique().to_list())
    if set(days) != current_days - previous_days:
        raise ValueError('bulk prepend must declare every new source date')
    if not current.filter(~pl.col('date').is_in(days)).sort(keys).equals(previous.sort(keys)):
        raise ValueError('bulk prepend changes the parent source')
    return days


def _refresh_bulk_checkpoint_days(restored, source_days, by_day, sources, *, asset_scope, contracts):
    """Re-read complete raw days and replace every date-owned checkpoint output."""
    from stockagent.data.tw_futures_finmind_ticks import read_pinned_tick_day
    days = set(source_days)
    selected = [item for item in sources if date.fromisoformat(item['date']) in days]
    if {date.fromisoformat(item['date']) for item in selected} != days:
        raise ValueError('bulk delta is missing a pinned whole-market date')
    additions = {name:[] for name in restored}
    expected = {'minutes','coverage','trades'} | {
        f'entry_{w}_{c.replace(":", "")}' for w,c in contracts}
    if set(restored) != expected:
        raise ValueError('bulk delta checkpoint output contract differs')
    for item in selected:
        bars, coverage, trades = read_pinned_tick_day(item, by_day[item['date']], asset_scope=asset_scope)
        additions['minutes'].append(bars.head(0))
        additions['coverage'].append(coverage)
        additions['trades'].append(trades)
        for (window, clock), contract in contracts.items():
            additions[f'entry_{window}_{clock.replace(":", "")}'].append(observed_window_entries(bars,contract))
    result = {}
    for name, original in restored.items():
        field = 'trade_date' if name == 'trades' else 'date'
        if field not in original.columns:
            raise ValueError('bulk delta checkpoint lacks its date ownership')
        result[name] = pl.concat([original.filter(~pl.col(field).is_in(sorted(days))), *additions[name]],
                                 how='vertical_relaxed')
    return result


def load_bulk_minutes(root, block_manifest, tick_roots, source, *, progress, workers=4,
                      asset_scope='stock_only', window_minutes=1,
                      prepared_entries=None, entry_windows=(), checkpoint_root=None,
                      reuse_checkpoint=None, reuse_source_receipt=None,
                      delta_parent_source=None, delta_days=(), delta_parent_proof=None,
                      prepend_days=(),
                      product_fallback_manifest=None, official_fallback_manifest=None):
    """Read each complete global object once instead of one file per contract.

    Full-session quantity/HLC certificates precede the small two-window view.
    Independently evidenced block-only days remain eligible but cannot fill.
    """
    from concurrent.futures import ThreadPoolExecutor
    import time
    from stockagent.data.tw_futures_finmind_ticks import (
        BULK_SOURCE, BULK_EQUITY_SOURCE, BULK_PRICE_POLICY, pin_mixed_tick_inventory,
        BULK_ALL_TWD_SOURCE, BULK_DATED_PRICE_POLICY,
        PRODUCT_FALLBACK_SOURCE, read_pinned_tick_day, stock_print_views,
    )
    from stockagent.data.tw_stock_futures_repair import apply_block_evidence
    from stockagent.data.tw_stock_futures_history import _read_verified_tick_partition, normalize_continuous_ticks
    from stockagent.data.tw_price_rules import (TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION,
                                               taifex_futures_day_session_minutes)
    streaming = prepared_entries is not None
    if streaming and (prepared_entries or not entry_windows):
        raise ValueError('streamed window output must be empty with explicit entry windows')
    contracts = {(w,c):decision_clock_contract(c,asset_scope=asset_scope,window_minutes=w)
        for w in entry_windows for c in ('08:45','09:00')} if streaming else {}
    collected = {key:[] for key in contracts}
    selected = source.filter(pl.col('executable'))
    # Product review windows are contiguous. Check both selected endpoints
    # before hashing/reading thousands of full-day tick objects; individual
    # prints still receive their dated session and price-grid validation.
    if asset_scope=='all_twd':
        dated_entry_sessions(selected)
    else:
        bounds = selected.group_by('product', 'asset_class').agg(
            pl.col('date').min().alias('_first'), pl.col('date').max().alias('_last'))
        for product, asset, first, last in bounds.iter_rows():
            for day in {first, last}:
                try:
                    taifex_futures_day_session_minutes(day, product_code=product, asset_class=asset)
                except ValueError as exc:
                    raise ValueError(f'bulk source preflight {product} {day}: {exc}') from exc
    block_sha = sha256_file(block_manifest)
    facts, block_proof = apply_block_evidence(selected, Path(block_manifest))
    by_day = {str(f['date'][0]): f for f in facts.partition_by('date')}
    if (product_fallback_manifest or official_fallback_manifest) and asset_scope != 'all_twd':
        raise ValueError('tick source fallbacks are available only for dated all-TWD clocks')
    sources = pin_mixed_tick_inventory(Path(root), sorted(facts['date'].unique().to_list()),
                                       product_fallback_manifest, official_fallback_manifest)
    checkpoint, identity, restored, parent_identity, parent_proof = None, None, None, None, None
    if (reuse_checkpoint is None) != (reuse_source_receipt is None) or (reuse_checkpoint and not checkpoint_root):
        raise ValueError('prior bulk reuse requires both checkpoint and frozen source receipt')
    if delta_parent_source is not None and (not reuse_checkpoint or not streaming or not delta_parent_proof):
        raise ValueError('bulk delta requires a verified parent checkpoint and streamed windows')
    if delta_parent_source is None and (delta_days or prepend_days or delta_parent_proof):
        raise ValueError('bulk delta must include the previous source')
    if delta_days and prepend_days:
        raise ValueError('bulk delta and prepend are distinct reconstruction contracts')
    changed_days = (_validate_bulk_prepend(delta_parent_source, source, prepend_days)
                    if prepend_days else _validate_bulk_delta(delta_parent_source, source, delta_days)
                    if delta_parent_source is not None else [])
    if checkpoint_root is not None:
        import stockagent.data.tw_futures_decision_clock as clock_module
        import stockagent.data.tw_futures_finmind_ticks as tick_module
        import stockagent.data.tw_stock_futures_repair as repair_module
        import stockagent.data.tw_price_rules as price_module
        implementation_modules = [clock_module, tick_module, repair_module, price_module]
        if official_fallback_manifest:
            import stockagent.data.tw_futures_official_ticks as official_module
            import scripts.download_taifex_recent_index_derivatives_ticks as archive_parser
            implementation_modules += [official_module, archive_parser]
        identity = dict(source_frame_sha256=hashlib.sha256(source.serialize()).hexdigest(),
            block_sha256=block_sha, objects=sources, asset_scope=asset_scope,
            window_minutes=window_minutes, entry_windows=sorted(entry_windows), streaming=streaming,
            polars_version=pl.__version__, implementation_sha256s={str(Path(m.__file__).resolve()):
                sha256_file(Path(m.__file__)) for m in implementation_modules},
            loader_sha256=sha256_file(Path(__file__)))
        if reuse_checkpoint is not None:
            parent_comparison = dict(identity)
            if delta_parent_source is not None:
                parent_comparison['source_frame_sha256'] = hashlib.sha256(delta_parent_source.serialize()).hexdigest()
            if prepend_days:
                added = set(changed_days)
                parent_comparison['objects'] = [item for item in sources
                    if date.fromisoformat(item['date']) not in added]
            parent_identity, parent_proof = _bulk_checkpoint_parent(
                reuse_checkpoint, reuse_source_receipt, parent_comparison)
            if prepend_days:
                def module_hashes(value):
                    paths = value.get('implementation_sha256s', {})
                    names = [Path(name).name for name in paths]
                    if len(names) != len(set(names)):
                        raise ValueError('bulk prepend has ambiguous implementation modules')
                    return {Path(name).name: digest for name, digest in paths.items()}
                if (parent_identity.get('loader_sha256') != identity.get('loader_sha256')
                        or module_hashes(parent_identity) != module_hashes(identity)):
                    raise ValueError('bulk prepend cannot mix different per-day implementations')
            if changed_days:
                parent_proof.update(contract=('verified_prior_core_prepend_v1' if prepend_days
                                              else 'verified_prior_core_date_delta_v1'),
                    rule=('identical_parent_and_complete_new_earlier_dates_with_verified_original_raw'
                          if prepend_days else 'identical_source_outside_full_recomputed_dates_and_verified_original_raw'),
                    previous_source_frame_sha256=parent_comparison['source_frame_sha256'],
                    changed_days=[str(d) for d in changed_days], parent_financial_source=delta_parent_proof)
            identity['reused_parent_core'] = parent_proof
        fingerprint=hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()
        checkpoint=Path(checkpoint_root)/fingerprint
        restored=_bulk_checkpoint_read(checkpoint,identity,sources,workers)
        if restored is None and parent_identity is not None:
            restored=_bulk_checkpoint_read(Path(reuse_checkpoint),parent_identity,sources,workers)
            if restored is None:
                raise ValueError('verified prior core checkpoint is missing')
            if changed_days:
                restored = _refresh_bulk_checkpoint_days(restored, changed_days, by_day, sources,
                    asset_scope=asset_scope, contracts=contracts)
                if any(sha256_file(Path(delta_parent_proof[f'{field}_path'])) != delta_parent_proof[f'{field}_sha256']
                       for field in ('daily','rules')):
                    raise ValueError('bulk delta parent financial source changed during reconstruction')
                progress(stage='bulk_checkpoint_delta_days_recomputed', dates_recomputed=len(changed_days),
                    dates_total=len(sources), original_raw_sha_readback_complete=True)
            _bulk_checkpoint_write(checkpoint,identity,restored)
    started = time.monotonic()
    minutes, coverage, trades = [], [], []
    def inspect(item):
        bars, days, events = read_pinned_tick_day(item, by_day[item['date']], asset_scope=asset_scope)
        if streaming:
            # Full-session source certification still runs above. Each requested
            # reduction is independent by date: retain O(contract-days * windows)
            # outputs instead of O(observed contract-minutes) historical bars.
            from stockagent.data.tw_futures_decision_clock import observed_window_entries
            reduced = {key:observed_window_entries(bars,contract) for key,contract in contracts.items()}
            return bars.head(0), days, events, reduced
        return bars.filter(_entry_minute_filter(window_minutes)), days, events, None
    if restored is None:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for i, (bars, days, events, reduced) in enumerate(pool.map(inspect, sources), 1):
                minutes.append(bars); coverage.append(days); trades.append(events)
                if streaming:
                    for key, frame in reduced.items():
                        collected[key].append(frame)
                if i == 1 or i % 50 == 0 or i == len(sources):
                    progress(stage='bulk_tick_readback', dates_checked=i, dates_total=len(sources),
                        elapsed_s=round(time.monotonic()-started, 3))
        minutes, coverage, trades = [pl.concat(items, how='vertical_relaxed') for items in (minutes, coverage, trades)]
        reductions={key:pl.concat(items,how='vertical_relaxed') for key,items in collected.items()}
        if checkpoint is not None:
            _bulk_checkpoint_write(checkpoint,identity,dict(minutes=minutes,coverage=coverage,trades=trades,
                **{f'entry_{w}_{c.replace(":", "")}':frame for (w,c),frame in reductions.items()}))
    else:
        minutes,coverage,trades=[restored[name] for name in ('minutes','coverage','trades')]
        reductions={key:restored[f'entry_{key[0]}_{key[1].replace(":", "")}'] for key in contracts}
        progress(stage='bulk_checkpoint_reused',dates_checked=len(sources),dates_total=len(sources),
            original_raw_sha_readback_complete=True)
    complements = []
    pending = facts.join(coverage.filter(~pl.col('verified')).select('date', 'physical_contract'),
        on=['date', 'physical_contract'], how='semi')
    # R1 is admitted only for an independently mapped dated near contract, not
    # by comparing a current broker alias or stripping lifetime suffixes.
    for row in pending.iter_rows(named=True):
        if row['tenor_rank'] != 1 or row['outright_volume'] <= 0:
            continue
        alias = row['product'] + 'R1'
        for tick_root in tick_roots:
            raw, proof, status = _read_verified_tick_partition(Path(tick_root), alias, row['date'])
            if status != 'complete':
                continue
            bars, stats = normalize_continuous_ticks(raw, day=row['date'], alias=alias,
                physical=row['physical_contract'], digest=proof['data_sha256'],
                product=row['product'], asset_class=row['asset_class'],
                **({'dated_specification':pl.DataFrame([row])} if asset_scope=='all_twd' else {}))
            if (stats['tick_volume'] != row['outright_volume'] or any(
                    abs(stats['tick_'+c]-row[c]) > 1e-6 for c in ('high', 'low', 'close'))):
                continue
            if asset_scope=='all_twd':
                session=dated_entry_sessions(pl.DataFrame([row])).row(0,named=True)
                session_start_s,session_end_s=session['_start'],session['_end']
            else:
                session_start, session_end = taifex_futures_day_session_minutes(row['date'],
                    product_code=row['product'], asset_class=row['asset_class'])
                session_start_s,session_end_s=session_start*60,session_end*60
            prints = raw.filter(pl.col('event_ts').dt.date() == row['date']).with_columns(
                (pl.col('event_ts').dt.hour().cast(pl.Int32)*3600 + pl.col('event_ts').dt.minute().cast(pl.Int32)*60
                 + pl.col('event_ts').dt.second().cast(pl.Int32)).alias('_second')).filter(
                    pl.col('_second').is_between(session_start_s, session_end_s)).with_columns(
                        pl.lit(row['physical_contract']).alias('physical_contract'), pl.col('close').alias('price'))
            opening = (pl.DataFrame([row]).select('date', 'physical_contract', 'observed_open')
                       if 'observed_open' in row else None)
            _, _, events = stock_print_views(prints, day=row['date'], source_sha256=proof['data_sha256'],
                                              opening_evidence=opening)
            minutes = pl.concat([minutes, bars.filter(_entry_minute_filter(window_minutes))], how='vertical_relaxed')
            trades = pl.concat([trades, events], how='vertical_relaxed')
            match = (pl.col('date') == row['date']) & (pl.col('physical_contract') == row['physical_contract'])
            updates = {'verified': True, 'source_file_sha256': proof['data_sha256'], **stats}
            coverage = coverage.with_columns(*[pl.when(match).then(pl.lit(v)).otherwise(pl.col(k)).alias(k)
                for k,v in updates.items() if k in coverage.columns])
            complements.append({**proof, 'date': str(row['date']), 'physical_contract': row['physical_contract'],
                'mapping': 'dated_official_unique_product_tenor1_then_exact_full_book_quantity_and_hlc',
                'official_expiry': str(row['official_expiry']), 'tick_volume': stats['tick_volume']})
            break
    if sha256_file(block_manifest) != block_sha:
        raise ValueError('official block inventory changed during preparation')
    verified = coverage.filter(pl.col('verified')).select('date','physical_contract')
    proof = dict(source=BULK_SOURCE if asset_scope == 'stock_only' else BULK_EQUITY_SOURCE,
        price_policy=BULK_PRICE_POLICY,
        product_history_contract_version=TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION,
        quantity='native_even_double_sided_divided_by_two_no_dedup',
        session='dated_product_rules_inclusive_exact_close_no_night_or_spread',
        admission='exact_continuous_book_quantity_and_official_hlc_or_independent_official_zero_book',
        block_manifest=dict(path=str(block_manifest),sha256=block_sha,receipt=block_proof),
        objects=sources, complementary_ticks=complements)
    if asset_scope=='all_twd':
        proof.update(source=BULK_ALL_TWD_SOURCE,price_policy=BULK_DATED_PRICE_POLICY,
            product_history_contract_version='bound_dated_product_specification_unknown_final_certified_v4',
            unknown_final_session=('intersection_then_full_book_reconciliation; authenticated_official_original_'
                                   'may_use_union_candidate_only_if_full_book_matches'),
            deferred_cash_final_settlement='never_assume_final_settlement_day_is_last_trading_day',
            selected_day_after_known_final_date='reject')
        if any(item.get('source_kind') == PRODUCT_FALLBACK_SOURCE for item in sources):
            proof.update(source='finmind_whole_market_and_exact_product_dated_all_twd_prints_v2',
                product_fallback_price_representation='native_float64_decimal_exact_dated_tick',
                missing_product_rule='official_zero_book_only_positive_volume_unverified',
                product_fallback_reduction_order='physical_contract_then_minute')
        if official_fallback_manifest:
            proof.update(source='finmind_and_taifex_original_dated_all_twd_prints_v1',
                official_source='taifex_recent_original_all_product_futures_tick_v1',
                official_rule='dated_session_exact_full_book_quantity_and_official_hlc',
                official_reduction_order='physical_contract_then_minute')
    if streaming:
        prepared_entries.update({w:{c:reductions[w,c]
            for c in ('08:45','09:00')} for w in entry_windows})
        proof['entry_materialization'] = dict(contract='bounded_day_streamed_window_reductions_fp64_v1',
            windows=sorted(entry_windows), full_session_source_certification_preserved=True)
    if parent_proof is not None:
        proof['reused_verified_core'] = parent_proof
    return minutes, verified, proof, coverage, trades


def supplement_community_rpt(inventory, source, minutes, coverage, trades, *,
                             progress, workers=4, window_minutes=1, block_detail_inventory=None):
    """Fill only previously unverified contract-days from a pinned mirror."""
    from concurrent.futures import ThreadPoolExecutor
    import csv
    import zipfile
    from stockagent.data.tw_futures_community_rpt import (
        pin_community_rpt_inventory, read_community_rpt_day,
    )

    keys = ['date', 'physical_contract']
    missing = coverage.filter(~pl.col('verified')).select(*keys, 'outright_volume', 'block_volume')
    pending = source.filter(pl.col('executable')).drop('outright_volume').join(
        missing, on=keys, how='inner', validate='1:1').filter(pl.col('outright_volume') > 0)
    items, pin = pin_community_rpt_inventory(Path(inventory), pending['date'].unique().to_list())
    by_day = {str(f['date'][0]): f for f in pending.partition_by('date')}
    details, detail_proof = {}, None
    if block_detail_inventory is not None:
        from stockagent.data.tw_futures_official_block_details import read_official_block_details
        available = json.loads(Path(block_detail_inventory).read_text())['requested_dates']
        wanted = pending.filter(pl.col('block_volume') > 0)['date'].unique().to_list()
        wanted = [d for d in wanted if str(d) in available]
        details, detail_proof = read_official_block_details(Path(block_detail_inventory), wanted)

    def inspect(item):
        try:
            bars, days, events, detail = read_community_rpt_day(item, by_day[item['date']],
                official_block_details=details.get(item['date']))
            return bars, days, events, {**item, 'status':'examined', **detail}
        except (ValueError, UnicodeError, csv.Error, zipfile.BadZipFile, OverflowError) as exc:
            # A bad third-party file is a source boundary, not permission to
            # invent an empty day or abort validation of independent dates.
            return None, None, None, {**item, 'status':'rejected',
                'reason':f'{type(exc).__name__}: {str(exc)[:240]}'}

    records, accepted_bars, accepted_days, accepted_events = [], [], [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, (bars, days, events, record) in enumerate(pool.map(inspect, items), 1):
            records.append(record)
            if days is not None and days.height:
                accepted_bars.append(bars.filter(_entry_minute_filter(window_minutes)))
                accepted_days.append(days)
                accepted_events.append(events)
            if i == 1 or i % 25 == 0 or i == len(items):
                progress(stage='community_rpt_readback', dates_checked=i, dates_total=len(items),
                    accepted_contract_days=sum(r.get('admitted_contract_days', 0) for r in records))
    if accepted_days:
        replacements = pl.concat(accepted_days, how='vertical_relaxed').select(coverage.columns)
        if replacements.select(keys).is_duplicated().any():
            raise ValueError('community supplement produced duplicate contract-day')
        coverage = pl.concat([coverage.join(replacements.select(keys), on=keys, how='anti'),
                              replacements], how='vertical_relaxed').sort(keys)
        minutes = pl.concat([minutes, *accepted_bars], how='vertical_relaxed')
        trades = pl.concat([trades, *accepted_events], how='vertical_relaxed')
    if sha256_file(Path(inventory)) != pin['sha256']:
        raise ValueError('community RPT inventory changed during supplementation')
    proof = dict(contract='community_rpt_full_session_supplement_v1', inventory=pin,
        source_is_official_publication=False, exchange_origin_independently_verified=False,
        historical_point_in_time_verified=False,
        official_block_details=detail_proof,
        admission='same_dated_physical_contract_full_book_volume_hlc_legal_ticks_and_unambiguous_first_trade',
        source_records=records, accepted_contract_days=sum(r.get('admitted_contract_days', 0) for r in records),
        rejected_source_dates=sum(r['status'] == 'rejected' for r in records))
    return minutes, coverage.filter(pl.col('verified')).select(keys), coverage, trades, proof


def paired_entries(source, minutes, verified_days, *, asset_scope='stock_only',
                   window_minutes=1, quarantine_days=None, prepared_entries=None):
    """Pure alignment: execution availability never changes policy eligibility."""
    keys = ["date", "physical_contract"]
    if source.select(keys).is_duplicated().any() or minutes.select(*keys, "minute").is_duplicated().any():
        raise ValueError("duplicate daily/minute identity")
    proof = verified_days.select(keys).unique().with_columns(pl.lit(True).alias("_verified"))
    quarantined = (quarantine_days.select(keys).with_columns(pl.lit(True).alias('_quarantined'))
        if quarantine_days is not None else source.head(0).select(keys).with_columns(pl.lit(True).alias('_quarantined')))
    axis = source.select(*keys, 'symbol', 'executable').join(proof, on=keys, how='left', validate='1:1')
    axis = axis.join(quarantined, on=keys, how='left', validate='1:1')
    result = {}
    for clock in ("08:45", "09:00"):
        window = decision_clock_contract(clock, asset_scope=asset_scope, window_minutes=window_minutes,
            quarantine_unknown=quarantine_days is not None)
        observed = observed_window_entries(minutes, window)
        if prepared_entries is not None:
            observed = pl.concat([prepared_entries[clock],observed],how='vertical_relaxed')
        rows = axis.join(observed, on=keys, how="left", validate="1:1").with_columns(
            pl.when(~pl.col("executable")).then(pl.lit("daily_not_executable"))
            .when(pl.col('_quarantined').fill_null(False) & ~pl.col('_verified').fill_null(False))
                .then(pl.lit('unverified_execution_quarantined'))
            .when(~pl.col("_verified").fill_null(False)).then(pl.lit("missing_source"))
            .when(pl.col("entry_volume").fill_null(0) > 0).then(pl.lit("observed_minute"))
            .otherwise(pl.lit("verified_no_window_trade")).alias("status"))
        rows = rows.with_columns(
            pl.when(pl.col("status") == "observed_minute").then(pl.col("entry_quote"))
              .otherwise(None).cast(pl.Float64).alias("entry_quote"),
            pl.when(pl.col("status") == "observed_minute").then(pl.col("entry_volume"))
              .otherwise(0.).cast(pl.Float64).alias("entry_volume"))
        result[clock] = rows.drop("executable", "_verified", '_quarantined').sort(keys)
    return result


def paired_opening_references(source, minutes, verified_days):
    """Use only real prints; official marks never become fallback trades.

    Daily closing prints have date precision, so retain a nullable trade_ts and
    a separately labelled completed-session availability bound. A current daily
    OPEN is model-visible only once its first verified minute is complete.
    """
    keys = ["date", "physical_contract"]
    closes = source.filter(pl.col("observed_close").is_finite()
        & (pl.col("observed_close") > 0) & (pl.col("volume") > 0)
        & pl.col("source_row_observed")).select(
        "physical_contract", pl.col("observed_close").alias("price"),
        pl.col("date").alias("trade_date"),
        pl.lit(None, dtype=pl.Datetime("us")).alias("trade_ts"),
        (pl.col("date").cast(pl.Datetime("us")) + pl.duration(days=1)
            - pl.duration(microseconds=1)).alias("available_by"),
        pl.col("official_source_sha256").alias("source_sha256"),
        pl.lit(False).alias("is_session_open"))
    prefix = minutes.filter((pl.col("volume") > 0) & (pl.col("minute") <= 540)).join(
        verified_days.select(keys).unique(), on=keys, how="semi")
    minute_trades = prefix.select("physical_contract", pl.col("close").alias("price"),
        pl.col("date").alias("trade_date"),
        pl.lit(None, dtype=pl.Datetime("us")).alias("trade_ts"),
        (pl.col("date").cast(pl.Datetime("us")) + pl.duration(minutes=pl.col("minute"))).alias("available_by"),
        pl.col("source_file_sha256").alias("source_sha256"), pl.lit(False).alias("is_session_open"))
    first = prefix.sort("minute").group_by(keys, maintain_order=True).first().join(
        source.select(*keys, "observed_open"), on=keys, how="left", validate="1:1")
    # A daily opening outside the first observed minute contradicts the
    # alleged complete prefix. Do not expose it using a date-only OPEN clock.
    first = first.filter(pl.col("observed_open").is_finite() & (pl.col("observed_open") > 0)
        & (pl.col("observed_open") >= pl.col("low") - 1e-6)
        & (pl.col("observed_open") <= pl.col("high") + 1e-6))
    openings = first.select("physical_contract", pl.col("observed_open").alias("price"),
        pl.col("date").alias("trade_date"), pl.lit(None, dtype=pl.Datetime("us")).alias("trade_ts"),
        (pl.col("date").cast(pl.Datetime("us")) + pl.duration(minutes=pl.col("minute"))).alias("available_by"),
        pl.col("source_file_sha256").alias("source_sha256"), pl.lit(True).alias("is_session_open"))
    trades = pl.concat([closes, minute_trades, openings], how="vertical_relaxed")
    decisions = source.select(*keys, "symbol")
    return {clock: resolve_opening_references(decisions, trades, decision_time=clock)
            for clock in ("08:45", "09:00")}


def load_verified_history(history_root, tick_roots, source, *, progress=None, include_reference_minutes=False,
                          window_minutes=1):
    """Rebind old minute facts to new official rows through explicit identity checks.

    The historical bundle may be partial and bound to an older daily release.
    Its accepted rows are reused only after raw receipt/SHA readback and matching
    the new physical contract's observed high/low/close and volume upper bound.
    No source digest is rewritten to pretend that the two releases are equal.
    """
    root = Path(history_root)
    manifest_path = root / "manifest.json"
    manifest_sha = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("dataset") != "taifex_stock_futures_minute_history_v2" or manifest.get("contract_version") != 2:
        raise ValueError("unsupported historical minute bundle")
    proofs = []
    member_stats = {}
    for key in ("all_minutes", "coverage"):
        item = manifest["outputs"][key]
        relative = Path(item["file"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("unsafe historical minute member")
        path = root / relative
        member_stats[path] = path.stat()
        digest = sha256_file(path)
        if digest != item["sha256"]:
            raise ValueError("historical minute member SHA mismatch")
        proofs.append({"path": str(path), "sha256": digest})
    coverage_path = Path(proofs[1]["path"])
    minute_path = Path(proofs[0]["path"])
    keys = ["date", "physical_contract"]
    # The new release has lifetime/generation IDs; the historical producer
    # used product:delivery. Join through explicit dated product/delivery cells,
    # and reject ambiguity instead of stripping a physical-identity suffix.
    mapping = source.select("date", "physical_contract",
        (pl.col("product") + pl.lit(":") + pl.col("contract")).alias("_minute_identity"))
    if mapping.select("date", "_minute_identity").is_duplicated().any():
        raise ValueError("ambiguous dated product/delivery to physical lifetime")
    coverage = pl.read_parquet(coverage_path).filter(pl.col("status") == "minute_verified").rename(
        {"physical_contract": "_minute_identity"}).join(mapping, on=["date", "_minute_identity"], how="inner")
    def selected_minutes():
        return pl.scan_parquet(minute_path).filter(pl.col("date").is_between(
            source["date"].min(), source["date"].max())).rename({"physical_contract": "_minute_identity"}).join(
                mapping.lazy(), on=["date", "_minute_identity"], how="inner").drop("_minute_identity")
    summaries = selected_minutes().group_by(keys).agg(
        pl.col("high").max().alias("_high"), pl.col("low").min().alias("_low"),
        pl.col("close").sort_by("minute").last().alias("_close"),
        pl.col("volume").sum().alias("_volume")).collect()
    coverage = coverage.join(summaries, on=keys, how="left", validate="1:1").join(
        source.select(*keys, "high", "low", "close", "volume"), on=keys, how="left", validate="1:1")
    coverage = coverage.filter((pl.col("_high") - pl.col("high")).abs().le(1e-6)
        & (pl.col("_low") - pl.col("low")).abs().le(1e-6)
        & (pl.col("_close") - pl.col("close")).abs().le(1e-6)
        & (pl.col("_volume") > 0) & (pl.col("_volume") <= pl.col("volume")))
    accepted = []
    for i, row in enumerate(coverage.iter_rows(named=True)):
        if _reuse_verified_contract(tick_roots, {**row, "physical_contract": row["_minute_identity"]}):
            accepted.append({k: row[k] for k in keys})
        if progress is not None and i % 1000 == 0:
            progress(stage="historical_raw_readback", checked=i, candidates=coverage.height, verified=len(accepted))
    verified = pl.DataFrame(accepted, schema={"date": pl.Date, "physical_contract": pl.String})
    minute_filter = _entry_minute_filter(window_minutes)
    if include_reference_minutes:
        minute_filter |= pl.col("minute") <= 540
    minutes = selected_minutes().filter(minute_filter).collect().join(
        verified, on=keys, how="semi")
    if sha256_file(manifest_path) != manifest_sha:
        raise ValueError("historical minute manifest changed during readback")
    for path, before in member_stats.items():
        after = path.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError("historical minute member changed during readback")
    return minutes, verified, {"path": str(manifest_path), "sha256": manifest_sha,
        "original_daily_sha256": manifest["source_daily_sha256"], "members": proofs,
        "rebind": "dated_product_delivery_to_unique_official_lifetime_observed_hlc_volume_bound_original_raw_sha"}


def build(args):
    from stockagent.data.tw_futures_margin import validate_margin_rule_source

    scope = getattr(args, 'asset_scope', 'stock_only')
    spec_path=getattr(args,'entry_specifications',None)
    spec_pin=None
    if scope=='all_twd':
        from stockagent.data.tw_futures_margin_release import read_bound_output
        if spec_path is None:
            raise ValueError('all-TWD clocks require --entry-specifications')
        spec_path=Path(spec_path)
        specs,spec_proof=read_bound_output(spec_path)
        if spec_path.name!='specifications.parquet' or spec_proof.get('dataset')!='taifex_dated_product_specifications':
            raise ValueError('all-TWD clocks require canonical dated product specifications')
        spec_pin=dict(path=str(spec_path),data_sha256=sha256_file(spec_path),
            manifest_sha256=sha256_file(spec_path.with_name('manifest.json')))
    elif spec_path is not None:
        raise ValueError('entry specifications belong only to the explicit all-TWD v5 clock')
    primary_window = getattr(args, 'entry_window_minutes', 1)
    windows = sorted(set([primary_window,
        *getattr(args, 'also_entry_window_minutes', [])]))
    quarantine_path = getattr(args, 'execution_quarantine', None)
    quarantine_sha = sha256_file(quarantine_path) if quarantine_path else None
    quarantine = json.loads(Path(quarantine_path).read_text()) if quarantine_path else None
    for window in windows:
        decision_clock_contract('08:45', asset_scope=scope, window_minutes=window,
            quarantine_unknown=quarantine is not None)
    allowed = clock_asset_classes(scope)
    if scope != 'stock_only' and not args.finmind_bulk_root:
        raise ValueError('expanded clocks require receipt-bound whole-market tick admission')
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    def progress(**state):
        payload = {"contract": clock_dataset(scope), **state}
        atomic_write_json(output / "progress.json", payload)
        print(json.dumps(payload, ensure_ascii=False), flush=True)
    progress(stage="validating_canonical_daily_and_rules")
    _, rule_manifest = validate_margin_rule_source(args.rules, args.daily)
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    daily_scan = pl.scan_parquet(args.daily)
    alias_columns = ["shioaji_roots"] if "shioaji_roots" in daily_scan.collect_schema().names() else []
    bulk_columns = ['outright_volume','tenor_rank','official_expiry'] if args.finmind_bulk_root else []
    reference_columns = (["observed_open", "observed_close", "source_row_observed", "official_source_sha256"]
                         if args.with_opening_reference else [])
    all_source = _load_clock_history(daily_scan, end, alias_columns, reference_columns, bulk_columns)
    if scope=='all_twd':
        # Earlier observations used for references remain source observations;
        # bind only the requested execution horizon to its financial admission.
        source=bind_entry_specifications(all_source.filter(pl.col('date')>=start),specs)
    else:
        source = all_source.filter(pl.col("date") >= start)
    if not source.height or source.filter(pl.col('asset_class').is_null()
            | ~pl.col('asset_class').is_in(allowed)).height:
        raise ValueError("paired clocks require a nonempty source within their declared asset scope")
    daily_sha, rules_sha = sha256_file(args.daily), sha256_file(args.rules)
    parent_daily = getattr(args, 'reuse_bulk_parent_daily', None)
    parent_rules = getattr(args, 'reuse_bulk_parent_rules', None)
    delta_tokens = getattr(args, 'reuse_bulk_delta_day', [])
    prepend_tokens = getattr(args, 'reuse_bulk_prepend_day', [])
    parent_start_token = getattr(args, 'reuse_bulk_parent_start', None)
    parent_source, parent_proof = None, None
    if any((parent_daily, parent_rules, delta_tokens, prepend_tokens, parent_start_token)):
        if not (parent_daily and parent_rules and (delta_tokens or prepend_tokens) and scope == 'all_twd'
                and getattr(args, 'reuse_bulk_checkpoint', None)
                and getattr(args, 'reuse_bulk_source_receipt', None) and max(windows) > 60):
            raise ValueError('bulk date delta requires both prior financial files, explicit days and streamed parent checkpoint')
        if (delta_tokens and prepend_tokens) or bool(prepend_tokens) != bool(parent_start_token):
            raise ValueError('bulk prepend requires its parent start and cannot also declare delta days')
        parent_start = date.fromisoformat(parent_start_token) if parent_start_token else start
        if prepend_tokens and not start < parent_start <= end:
            raise ValueError('bulk prepend parent start must lie strictly after the new start')
        parent_daily, parent_rules = Path(parent_daily), Path(parent_rules)
        old_daily_sha, old_rules_sha = sha256_file(parent_daily), sha256_file(parent_rules)
        if (parent_daily.resolve(), parent_rules.resolve()) != (Path(args.daily).resolve(), Path(args.rules).resolve()):
            validate_margin_rule_source(parent_rules, parent_daily)
        old_scan = pl.scan_parquet(parent_daily)
        old_alias = ["shioaji_roots"] if "shioaji_roots" in old_scan.collect_schema().names() else []
        if old_alias != alias_columns:
            raise ValueError('bulk delta previous source has a different contract schema')
        # The verified parent hash was computed after the history scan and an
        # eager start-date filter. A pushed-down range predicate can produce an
        # equal Polars frame with different serialized bytes/chunk boundaries.
        old = _load_clock_history(old_scan, end, old_alias, reference_columns, bulk_columns).filter(
            pl.col('date') >= parent_start)
        parent_source = bind_entry_specifications(old,specs)
        if sha256_file(parent_daily) != old_daily_sha or sha256_file(parent_rules) != old_rules_sha:
            raise ValueError('bulk delta parent financial source changed during read')
        parent_proof = dict(daily_path=str(parent_daily),daily_sha256=old_daily_sha,
                            rules_path=str(parent_rules),rules_sha256=old_rules_sha,
                            source_start=str(parent_start))
        delta_days = [date.fromisoformat(v) for v in delta_tokens]
        prepend_days = [date.fromisoformat(v) for v in prepend_tokens]
    else:
        delta_days = []
        prepend_days = []
    reference_trades = None
    prepared_entries = {} if args.finmind_bulk_root and max(windows) > 60 else None
    if args.finmind_bulk_root:
        if not args.block_manifest:
            raise ValueError('bulk print admission requires independent official block evidence')
        progress(stage='bulk_tick_inventory', contract_days=source.height)
        minutes, verified, history_proof, coverage, reference_trades = load_bulk_minutes(
            args.finmind_bulk_root, args.block_manifest, args.tick_root, source,
            progress=progress, workers=args.source_workers, asset_scope=scope, window_minutes=max(windows),
            prepared_entries=prepared_entries, entry_windows=windows,
            checkpoint_root=output/'bulk_readback_checkpoints',
            reuse_checkpoint=getattr(args, 'reuse_bulk_checkpoint', None),
            reuse_source_receipt=getattr(args, 'reuse_bulk_source_receipt', None),
            delta_parent_source=parent_source,delta_days=delta_days,delta_parent_proof=parent_proof,
            prepend_days=prepend_days,
            product_fallback_manifest=getattr(args, 'finmind_product_fallback_manifest', None),
            official_fallback_manifest=getattr(args, 'taifex_recent_fallback_manifest', None))
        community_inventory = getattr(args, 'community_rpt_inventory', None)
        if community_inventory is not None:
            if scope != 'all_twd' or not args.with_opening_reference:
                raise ValueError('community RPT supplement needs all-TWD dated rules and opening references')
            minutes, verified, coverage, reference_trades, supplement = supplement_community_rpt(
                community_inventory, source, minutes, coverage, reference_trades,
                progress=progress, workers=args.source_workers, window_minutes=max(windows),
                block_detail_inventory=getattr(args, 'official_block_detail_inventory', None))
            history_proof['community_rpt_supplement'] = supplement
        atomic_write_parquet(output / 'source_coverage.parquet', coverage)
    else:
        if any(getattr(args, name, None) is not None for name in (
                'community_rpt_inventory', 'finmind_product_fallback_manifest',
                'taifex_recent_fallback_manifest')):
            raise ValueError('tick source supplements require the canonical whole-market source')
        progress(stage="historical_minute_validation", contract_days=source.height)
        minutes, verified, history_proof = load_verified_history(args.history_root, args.tick_root, source,
            progress=progress, include_reference_minutes=args.with_opening_reference, window_minutes=max(windows))
    sources, invalid_source_days = [], []
    missing = source.filter(pl.col("executable")).join(verified, on=["date", "physical_contract"], how="anti")
    for root in args.exact_kbar_root:
        if not missing.height:
            break
        progress(stage="exact_contract_kbars", root=str(root), remaining=missing.height)
        new_minutes, receipts, gaps = read_futures_kbar_sources(Path(root), missing,
            sorted(source["date"].unique().to_list()), all_futures=True, full_session=True,
            invalid_source_days=invalid_source_days)
        atomic_write_json(output / "rejected_kbar_source_days.json", {
            "source_days": invalid_source_days, "count": len(invalid_source_days),
            "policy": "unmodified_raw_retained_invalid_days_not_admitted"})
        # A provider must not create more outright contracts than the official
        # session. Missing data is a repair requirement, never a zero volume.
        totals = new_minutes.group_by("date", "physical_contract").agg(
            pl.col("volume").sum().alias("_v"), pl.col("high").max().alias("_high"),
            pl.col("low").min().alias("_low"), pl.col("close").sort_by("minute").last().alias("_close"))
        days = missing.select("date", "physical_contract", "volume", "high", "low", "close").join(totals,
            on=["date", "physical_contract"], how="inner").filter((pl.col("_v") > 0)
                & (pl.col("_v") <= pl.col("volume"))
                & ((pl.col("_high") - pl.col("high")).abs() <= 1e-6)
                & ((pl.col("_low") - pl.col("low")).abs() <= 1e-6)
                & ((pl.col("_close") - pl.col("close")).abs() <= 1e-6))
        # Receipts establish completed same-day coverage, not merely the
        # presence of the requested entry minute.
        days = _complete_kbar_days(days, gaps)
        new_minutes = new_minutes.join(days, on=["date", "physical_contract"], how="semi").filter(
            _entry_minute_filter(max(windows)) | ((pl.col("minute") <= 540) & args.with_opening_reference))
        minutes = pl.concat([minutes, new_minutes], how="vertical_relaxed")
        verified = pl.concat([verified, days]).unique()
        sources.extend(receipts)
        missing = missing.join(days, on=["date", "physical_contract"], how="anti")
    references = {}
    if args.with_opening_reference:
        if quarantine is not None and reference_trades is not None:
            # An incomplete tape cannot establish the session's first print.
            # Carry the last verified real trade with its original timestamp;
            # the unknown day supplies neither an opening claim nor a fill.
            reference_trades = reference_trades.join(verified.rename({'date': 'trade_date'}),
                on=['trade_date', 'physical_contract'], how='semi')
        references = ({clock: resolve_opening_references(source.select('date','physical_contract','symbol'),
            reference_trades, decision_time=clock) for clock in ('08:45','09:00')}
            if reference_trades is not None else paired_opening_references(all_source, minutes, verified))
    gaps = missing.select("date", "product", "contract", "asset_class", "physical_contract", "symbol", "volume").with_columns(
        pl.lit("source_evidence_missing").alias("status"), pl.col("volume").alias("outright_volume"))
    atomic_write_parquet(output / "coverage_gaps.parquet", gaps)
    if quarantine is not None:
        validate_execution_quarantine(quarantine, gaps, daily_sha256=daily_sha, rules_sha256=rules_sha)
        if sha256_file(quarantine_path) != quarantine_sha:
            raise ValueError('execution quarantine changed during source readback')
    ready = not gaps.height or quarantine is not None
    identity = {"daily": daily_sha, "rules": rules_sha, "start": str(source["date"].min()),
        "end": str(source["date"].max()), "historical_source": history_proof,
        "exact_sources": sources}
    if spec_pin is not None:
        identity['entry_specifications']=spec_pin
    if references:
        identity["opening_reference_contracts"] = [opening_reference_contract(c) for c in references]
        identity['reference_observations'] = ('first_second_official_open_reconciled_or_unavailable_v2'
            if reference_trades is not None else 'verified_minute_and_prior_official_observed_close')
        if quarantine is not None:
            identity['quarantine_reference_policy'] = 'ignore_unverified_day_prints_carry_last_verified_real_trade_with_original_clock'
    identities = {}
    for window in windows:
        options = dict(window_minutes=window, quarantine_unknown=quarantine is not None)
        clocks = paired_entries(source, minutes, verified, asset_scope=scope, window_minutes=window,
            quarantine_days=gaps if quarantine is not None else None,
            prepared_entries=prepared_entries[window] if prepared_entries is not None else None)
        version_identity = {**identity, 'clocks': [decision_clock_contract(c, asset_scope=scope, **options) for c in clocks]}
        if quarantine is not None:
            version_identity['execution_quarantine_receipt_sha256'] = quarantine_sha
        pair_identity = hashlib.sha256(json.dumps(version_identity, sort_keys=True, default=str).encode()).hexdigest()
        identities[str(window)] = pair_identity
        variant_root = output if window == primary_window else output / f'entry_window_{window}m'
        for clock, entries in clocks.items():
            directory = variant_root / clock.replace(':', '')
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / 'entries.parquet'
            atomic_write_parquet(path, entries)
            manifest = {
                'dataset': clock_dataset(scope, **options), 'status': 'complete' if ready else 'partial',
                'clock': decision_clock_contract(clock, asset_scope=scope, **options), 'pair_identity': pair_identity,
                'entries_sha256': sha256_file(path), 'rows': entries.height,
                'source_daily_sha256': daily_sha, 'source_rules_sha256': rules_sha,
                'start_date': identity['start'], 'end_date': identity['end'],
                'sources': version_identity, 'coverage_gaps': gaps.height,
                'counts': dict(entries.group_by('status').len().iter_rows()),
            }
            if spec_pin is not None:
                spec_output=directory/'entry_specifications'
                spec_output.mkdir(exist_ok=True)
                atomic_write_bytes(spec_output/'specifications.parquet',spec_path.read_bytes())
                atomic_write_bytes(spec_output/'manifest.json',spec_path.with_name('manifest.json').read_bytes())
                manifest['entry_specifications']=dict(directory=spec_output.name,
                    data_sha256=spec_pin['data_sha256'],manifest_sha256=spec_pin['manifest_sha256'])
            if quarantine is not None:
                receipt = directory / 'execution_quarantine.json'
                atomic_write_json(receipt, quarantine)
                manifest.update(source_coverage_complete=False, execution_quarantine={
                    'file': receipt.name, 'sha256': sha256_file(receipt), 'contract_days': gaps.height})
            atomic_write_json(directory / 'manifest.json', manifest)
            if clock in references:
                ref_path = directory / 'opening_reference.parquet'
                refs = references[clock].filter(pl.col('date').is_between(start, end))
                atomic_write_parquet(ref_path, refs)
                atomic_write_json(directory / 'opening_reference_manifest.json', {
                    'dataset': OPENING_REFERENCE_DATASET, 'status': 'complete' if ready else 'partial',
                    'contract': opening_reference_contract(clock), 'pair_identity': pair_identity,
                    'source_daily_sha256': daily_sha, 'data_sha256': sha256_file(ref_path), 'rows': refs.height,
                    'counts': dict(refs.group_by('reference_kind').len().iter_rows()),
                })
    if sha256_file(args.daily) != daily_sha or sha256_file(args.rules) != rules_sha:
        raise ValueError("canonical daily/rules changed during entry preparation")
    if spec_pin is not None and (sha256_file(spec_path)!=spec_pin['data_sha256']
            or sha256_file(spec_path.with_name('manifest.json'))!=spec_pin['manifest_sha256']):
        raise ValueError('dated entry specification changed during source readback')
    progress(stage="complete" if ready else "blocked_source_coverage",
        contract_days=source.height, verified_intraday_contract_days=verified.height,
        missing_executable_contract_days=gaps.height, pair_identity=identities[str(primary_window)],
        window_pair_identities=identities, quarantined_execution_contract_days=gaps.height if quarantine else 0)
    return 0 if ready else 2


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--daily", required=True)
    p.add_argument("--rules", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--start", default="2020-03-23")
    p.add_argument("--end", default="2026-09-24")
    p.add_argument("--history-root", default="data_tw_futures/taifex_stock_futures_minute_history_v2")
    p.add_argument("--tick-root", type=Path, action="append", default=[])
    p.add_argument("--exact-kbar-root", type=Path, action="append", default=[])
    p.add_argument('--finmind-bulk-root', type=Path,
                   help='Whole-market tick collector root or pinned portable raw inventory JSON')
    p.add_argument('--finmind-product-fallback-manifest', type=Path,
                   help='Explicit original product/day receipts for selected missing all-TWD bulk dates')
    p.add_argument('--taifex-recent-fallback-manifest', type=Path,
                   help='Frozen canonical TAIFEX recent archive receipt for explicit all-TWD days')
    p.add_argument('--community-rpt-inventory', type=Path,
                   help='Optional source-separated community archive inventory; full-session certificates required')
    p.add_argument('--official-block-detail-inventory', type=Path,
                   help='Optional SHA-bound TAIFEX block detail for uniquely removing unavailable block fills')
    p.add_argument('--block-manifest', type=Path, help='Independent official continuous-book quantity evidence')
    p.add_argument('--source-workers', type=int, choices=range(1,9), default=4)
    p.add_argument('--reuse-bulk-checkpoint', type=Path,
                   help='Optional previously verified immutable whole-market core checkpoint')
    p.add_argument('--reuse-bulk-source-receipt', type=Path,
                   help='Frozen source receipt that originally produced the reused checkpoint')
    p.add_argument('--reuse-bulk-parent-daily', type=Path,
                   help='Verified previous daily release for an explicit whole-date core refresh')
    p.add_argument('--reuse-bulk-parent-rules', type=Path,
                   help='Rules bound to the verified previous daily release')
    p.add_argument('--reuse-bulk-delta-day', action='append', default=[],
                   help='Complete source date to recompute; all other source rows must remain identical')
    p.add_argument('--reuse-bulk-parent-start', help='Prior checkpoint start date for a verified earlier-date extension')
    p.add_argument('--reuse-bulk-prepend-day', action='append', default=[],
                   help='Complete earlier date to add; all parent source rows and objects must remain identical')
    p.add_argument('--entry-window-minutes', type=int, default=1,
                   help='Fixed post-latency execution VWAP window, 1..300 minutes; >60 uses extended-window v4')
    p.add_argument('--also-entry-window-minutes', type=int, action='append', default=[],
                   help='Publish additional windows from the same verified raw readback under entry_window_Nm/')
    p.add_argument('--execution-quarantine', type=Path,
                   help='Exact daily/rules-bound unknown contract-day allowlist: zero orders, official carry valuation retained')
    p.add_argument('--asset-scope', choices=tuple(ASSET_SCOPES), default='stock_only',
                   help='Explicit stock/equity or v5 all-TWD scope; never infers missing product rules')
    p.add_argument('--entry-specifications',type=Path,
                   help='Required by all_twd: canonical source-bound dated specifications.parquet')
    p.add_argument("--with-opening-reference", action="store_true",
                   help="Add causal observed-open/last-real-trade inputs with separate clocks and source hashes")
    args = p.parse_args()
    if args.official_block_detail_inventory and not args.community_rpt_inventory:
        p.error('--official-block-detail-inventory requires --community-rpt-inventory')
    if not args.tick_root:
        args.tick_root = [Path("data_tw_futures/shioaji_history")]
    return build(args)


if __name__ == "__main__":
    raise SystemExit(main())
