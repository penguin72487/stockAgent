"""Source-bound research reconciliation for duplicate official spread labels.

The raw exchange archive remains unchanged.  A separately opted-in historical
research generation may correct a derived leg allocation only when two retained
tick tapes match and every involved official outright total and spread OHLCV
reconciles.  This is not an exchange correction or a point-in-time certificate.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
from datetime import date
from decimal import Decimal
import io
import json
from pathlib import Path
import re
import zipfile

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_community_rpt import pin_community_rpt_inventory
from stockagent.data.tw_index_futures import _decoded_csv_stream, _parse_trading_date, _taifex_futures_session
from stockagent.data.tw_price_rules import taifex_futures_day_session_minutes

CONTRACT = 'taifex_duplicate_spread_label_research_v1'


def reconcile_duplicate_spread(evidence: pl.DataFrame, policy_path: Path,
                               official_sources: list[dict]) -> tuple[pl.DataFrame, dict]:
    """Recompute a scoped leg correction from pinned raw data, or fail closed."""
    policy_path = Path(policy_path).resolve()
    policy_hash = sha256_file(policy_path)
    policy = json.loads(policy_path.read_bytes())
    if (policy.get('contract') != CONTRACT or policy.get('historical_point_in_time_verified') is not False
            or policy.get('exchange_correction_verified') is not False):
        raise ValueError('spread reconciliation needs an explicit non-PIT research policy')
    day = date.fromisoformat(policy['date'])
    product, asset_class = policy['product'], policy['asset_class']
    duplicate, actual = policy['official_duplicate_label'], policy['native_spread_labels']
    month_pattern = r'[0-9]{6}(?:W[1-5])?'
    spread_pattern = rf'{month_pattern}/{month_pattern}'
    if (not re.fullmatch(r'[A-Z0-9]{2,5}', product) or not re.fullmatch(spread_pattern, duplicate)
            or len(actual) != 2 or len(set(actual)) != 2 or duplicate not in actual
            or any(not re.fullmatch(spread_pattern, x) for x in actual)):
        raise ValueError('invalid scoped duplicate spread identities')
    months = set(duplicate.split('/')) | {m for label in actual for m in label.split('/')}
    if len(months) != 3:
        raise ValueError('a duplicate must reconcile exactly three physical legs')
    start, end = taifex_futures_day_session_minutes(day, product_code=product, asset_class=asset_class)

    def bound(entry):
        path = Path(entry['path']).resolve()
        if sha256_file(path) != entry['sha256']:
            raise ValueError('spread reconciliation input SHA mismatch')
        return path

    official_path = bound(policy['official_archive'])
    if policy['official_archive']['sha256'] not in {s['sha256'] for s in official_sources}:
        raise ValueError('spread correction does not belong to this official source inventory')
    inventory_path, receipt_path = bound(policy['community_inventory']), bound(policy['finmind_receipt'])
    items, inventory_proof = pin_community_rpt_inventory(inventory_path, [day])
    if len(items) != 1:
        raise ValueError('community inventory does not contain exactly one requested day')
    item = items[0]
    mirror_path = Path(item['path'])
    if mirror_path.stat().st_size != item['bytes'] or sha256_file(mirror_path) != item['sha256']:
        raise ValueError('community source identity differs from pinned inventory')
    receipt = json.loads(receipt_path.read_bytes())
    if (receipt.get('dataset') != 'TaiwanFuturesTick' or receipt.get('partition') != str(day)
            or receipt.get('status') != 'complete' or receipt.get('historical_point_in_time') is not False
            or receipt.get('source_first_date') != str(day) or receipt.get('source_last_date') != str(day)):
        raise ValueError('invalid FinMind native tick receipt')
    provider_root = Path(policy['finmind_root']).resolve()
    object_path = (provider_root / receipt['parquet_path']).resolve()
    if not object_path.is_relative_to(provider_root) or sha256_file(object_path) != receipt['sha256']:
        raise ValueError('FinMind object path or SHA mismatch')

    official, other_spreads = {}, []
    for stream, name, _ in _decoded_csv_stream(official_path):
        reader = csv.DictReader(stream)
        reader.fieldnames = [str(s).lstrip('\ufeff').strip() for s in reader.fieldnames]
        required = {'交易日期','契約','到期月份(週別)','成交量','開盤價','最高價','最低價','收盤價'}
        if not required <= set(reader.fieldnames):
            raise ValueError('official daily schema is incomplete')
        has_session = '交易時段' in reader.fieldnames
        for row in reader:
            if str(_parse_trading_date(row['交易日期'])) != str(day) or row['契約'].strip().upper() != product:
                continue
            session, _ = _taifex_futures_session(row.get('交易時段'), source_has_session=has_session, source_name=name)
            if session != '一般':
                continue
            label = row['到期月份(週別)'].strip()
            if label in months or label == duplicate:
                official.setdefault(label, []).append(row)
            elif '/' in label and months.intersection(label.split('/')):
                other_spreads.append(label)
    if other_spreads or set(official) != months | {duplicate} or len(official[duplicate]) != 2:
        raise ValueError('official spread rows require a larger or different reconciliation scope')
    if any(len(official[m]) != 1 for m in months):
        raise ValueError('official outright contract-day is duplicated')

    wanted = months | set(actual)
    native = []
    with zipfile.ZipFile(mirror_path) as archive:
        wanted_name = f"Daily_{day.strftime('%Y_%m_%d')}.rpt"
        members = archive.infolist()
        if len(members) != 1 or members[0].filename != wanted_name or members[0].is_dir():
            raise ValueError('unexpected community archive identity')
        with io.TextIOWrapper(archive.open(members[0]), encoding='cp950', newline='') as stream:
            for row in csv.reader(stream):
                if len(row) < 6 or row[0].strip() != day.strftime('%Y%m%d') or row[1].strip() != product:
                    continue
                label, clock = row[2].strip(), row[3].strip()
                if label not in wanted and not ('/' in label and months.intersection(label.split('/'))):
                    continue
                if len(clock) != 6 or not clock.isdigit():
                    raise ValueError('invalid native trade clock')
                hour, minute, second = int(clock[:2]), int(clock[2:4]), int(clock[4:])
                if hour > 23 or minute > 59 or second > 59:
                    raise ValueError('invalid native trade clock')
                seconds = hour*3600 + minute*60 + second
                if start*60 <= seconds <= end*60:
                    native.append((label, seconds, Decimal(row[4].strip()), int(row[5].strip())))
    if any(label not in wanted for label, *_ in native):
        raise ValueError('native tape contains an additional spread involving a selected leg')
    provider = pl.scan_parquet(object_path).filter(pl.col('futures_id') == product).collect()
    if provider.schema.get('date') != pl.String or provider.schema.get('contract_date') != pl.String:
        raise ValueError('unexpected FinMind tick schema')
    provider_rows = []
    for r in provider.iter_rows(named=True):
        label = r['contract_date']
        if label not in wanted and not ('/' in label and months.intersection(label.split('/'))):
            continue
        timestamp = r['date']
        if not isinstance(timestamp, str) or not timestamp.startswith(str(day) + ' '):
            raise ValueError('provider trade date differs from receipt')
        clock = timestamp[11:]
        if not re.fullmatch(r'\d\d:\d\d:\d\d', clock):
            raise ValueError('invalid provider trade clock')
        hour, minute, second = (int(x) for x in clock.split(':'))
        if hour > 23 or minute > 59 or second > 59:
            raise ValueError('invalid provider trade clock')
        seconds = hour*3600 + minute*60 + second
        if start*60 <= seconds <= end*60:
            provider_rows.append((label, seconds, Decimal(str(r['price'])), int(r['volume'])))
    if Counter(native) != Counter(provider_rows):
        raise ValueError('retained provider and community trade multisets differ')
    groups = defaultdict(list)
    for label, seconds, price, volume in native:
        divisor = 4 if '/' in label else 2
        if not price.is_finite() or volume <= 0 or volume % divisor:
            raise ValueError('invalid native price or matched volume')
        groups[label].append((seconds, price, volume))
    if set(groups) != wanted:
        raise ValueError('one or more scoped physical trades are absent')

    def source_stats(label):
        rows = sorted(groups[label], key=lambda r:r[0])
        first = {r[1] for r in rows if r[0] == rows[0][0]}
        last = {r[1] for r in rows if r[0] == rows[-1][0]}
        prices = [r[1] for r in rows]
        return dict(first=first, last=last, high=max(prices), low=min(prices),
                    volume=sum(r[2] for r in rows)//(4 if '/' in label else 2))

    def row_signature(row):
        return (Decimal(row['開盤價']), Decimal(row['最高價']), Decimal(row['最低價']),
                Decimal(row['收盤價']), int(row['成交量'].replace(',', '')))

    stats = {label:source_stats(label) for label in wanted}
    # Unique OHLCV signatures are needed to assign the duplicate official row
    # to its actual native label without inventing an exchange correction.
    native_signatures = []
    for label in actual:
        s = stats[label]
        if len(s['first']) != 1 or len(s['last']) != 1:
            raise ValueError('spread opening or closing second is ambiguous')
        native_signatures.append((next(iter(s['first'])), s['high'], s['low'], next(iter(s['last'])), s['volume']))
    if len(set(native_signatures)) != 2 or Counter(native_signatures) != Counter(
            row_signature(r) for r in official[duplicate]):
        raise ValueError('duplicate official rows do not uniquely match native spread OHLCV')

    corrections = []
    for month in sorted(months):
        row, s = official[month][0], stats[month]
        opening, high, low, closing, total = row_signature(row)
        corrected = sum(stats[label]['volume'] for label in actual if month in label.split('/'))
        original = sum(int(r['成交量'].replace(',', '')) for r in official[duplicate]
                       if month in duplicate.split('/'))
        if (opening not in s['first'] or closing not in s['last'] or high != s['high']
                or low != s['low'] or total != s['volume'] + corrected):
            raise ValueError('native outright OHLC or volume does not reconcile with official total')
        corrections.append(dict(date=day, physical_contract=f'{product}:{month}', official_volume=total,
                                old_spread=original, new_spread=corrected))

    rows = pl.DataFrame(corrections)
    selected = evidence.join(rows, on=['date', 'physical_contract'], how='inner', validate='1:1')
    valid = ((pl.col('official_source_sha256') == policy['official_archive']['sha256'])
        & (pl.col('official_volume') == pl.col('official_volume_right'))
        & (pl.col('spread_leg_volume') == pl.col('old_spread'))
        & (pl.col('outright_volume') == pl.col('official_volume') - pl.col('old_spread'))).fill_null(False)
    if selected.filter(~valid).height:
        raise ValueError('official baseline differs from independently reconciled originals')
    result = evidence.join(rows.select('date','physical_contract','old_spread','new_spread'),
        on=['date','physical_contract'], how='left', validate='1:1').with_columns(
            pl.coalesce('new_spread','spread_leg_volume').alias('spread_leg_volume'),
            pl.when(pl.col('new_spread').is_not_null())
                .then(pl.col('official_volume') - pl.col('new_spread'))
                .otherwise(pl.col('outright_volume')).alias('outright_volume'))
    result = result.with_columns(pl.when(pl.col('new_spread').is_null()).then(pl.col('official_reason'))
        .when(pl.col('official_volume') == 0).then(pl.lit('zero_total_volume'))
        .when(pl.col('outright_volume') == 0).then(pl.lit('spread_legs_only'))
        .otherwise(pl.lit('outright_volume')).alias('official_reason')).drop('old_spread','new_spread')
    if result.filter(pl.col('outright_volume') < 0).height:
        raise ValueError('corrected spread exceeds the official outright total')
    bound_paths = [official_path, inventory_path, receipt_path, mirror_path, object_path]
    hashes = [policy['official_archive']['sha256'], policy['community_inventory']['sha256'],
              policy['finmind_receipt']['sha256'], item['sha256'], receipt['sha256']]
    if sha256_file(policy_path) != policy_hash or any(sha256_file(p) != h for p,h in zip(bound_paths, hashes)):
        raise ValueError('spread reconciliation inputs changed during read')
    return result, dict(contract=CONTRACT, policy_path=str(policy_path), policy_sha256=policy_hash,
        implementation_sha256=sha256_file(Path(__file__)), date=str(day), product=product,
        raw_official_label=duplicate, inferred_native_labels=actual, affected_rows=selected.height,
        tick_rows_compared=len(native), official_archive_sha256=hashes[0],
        finmind_receipt_sha256=hashes[2], finmind_object_sha256=hashes[4], community_inventory=inventory_proof,
        community_object_sha256=hashes[3], exchange_origin_independently_verified=False,
        historical_point_in_time_verified=False, corrections=[{**r,'date':str(r['date'])} for r in corrections])
