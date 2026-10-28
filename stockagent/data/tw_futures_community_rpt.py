"""Strict, source-separated supplements from a community RPT archive.

The mirror is not an exchange publication. Every admission still requires the
canonical dated session, price grid, full-session official book volume/HLC,
and an unambiguous first trade. Unverified source days remain quarantined.
"""
from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
import io
import json
from pathlib import Path
import re
import zipfile

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_finmind_ticks import normalize_bulk_stock_ticks

INVENTORY = 'community_rpt_dated_prints_inventory_v1'
SOURCE = 'community_rpt_mirror_unverified_origin_v1'


class SourceChangedError(RuntimeError):
    """An inventory-bound object changed while being consumed."""


def pin_community_rpt_inventory(path: Path, dates: list[date]) -> tuple[list[dict], dict]:
    path = Path(path).resolve()
    before = sha256_file(path)
    document = json.loads(path.read_text())
    if (document.get('contract') != INVENTORY or document.get('source') != SOURCE
            or document.get('historical_point_in_time_verified') is not False):
        raise ValueError('invalid community RPT inventory provenance')
    wanted = {str(day) for day in dates}
    root = path.parent
    items, seen = [], set()
    for record in document['objects']:
        day = date.fromisoformat(record['date'])
        if str(day) in seen:
            raise ValueError('duplicate community RPT date')
        seen.add(str(day))
        expected = f"Daily_{day.strftime('%Y_%m_%d')}.zip"
        member = Path(record['path'])
        if member.name != expected or member.is_absolute():
            raise ValueError('community RPT path/date mismatch')
        member = (root / member).resolve()
        if not member.is_relative_to(root):
            raise ValueError('community RPT source path escapes inventory')
        if not re.fullmatch(r'[A-Za-z0-9_-]+', record['drive_id']):
            raise ValueError('invalid community source identity')
        if not re.fullmatch(r'[0-9a-f]{64}', record['sha256']):
            raise ValueError('invalid community source hash')
        if str(day) in wanted:
            items.append(dict(date=str(day), path=str(member), sha256=record['sha256'],
                bytes=record['bytes'], drive_id=record['drive_id']))
    if sha256_file(path) != before:
        raise SourceChangedError('community RPT inventory changed during pinning')
    return sorted(items, key=lambda x: x['date']), dict(path=str(path), sha256=before,
        catalog_sha256=document['catalog_sha256'], source=SOURCE,
        historical_point_in_time_verified=False)


def read_community_rpt_day(item: dict, expected: pl.DataFrame, *, official_block_details=None):
    """Read all native rows for exact requested contracts; reject altered input."""
    path = Path(item['path'])
    before = path.stat()
    if before.st_size != item['bytes'] or sha256_file(path) != item['sha256']:
        raise SourceChangedError('community RPT pinned source size or SHA mismatch')
    day = date.fromisoformat(item['date'])
    wanted = set(expected.select('product', 'contract').iter_rows())
    if not wanted or expected.filter(pl.col('date') != day).height:
        raise ValueError('community RPT expected contract-day mismatch')
    values = []
    try:
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            wanted_name = f"Daily_{day.strftime('%Y_%m_%d')}.rpt"
            if (len(members) != 1 or members[0].filename != wanted_name
                    or members[0].is_dir() or members[0].flag_bits & 1
                    or members[0].file_size > 128 * 1024 * 1024):
                raise ValueError('unexpected community archive member')
            with io.TextIOWrapper(archive.open(members[0]), encoding='cp950', newline='') as text:
                for row_number, fields in enumerate(csv.reader(text), 1):
                    cells = [field.strip() for field in fields]
                    if len(cells) < 3 or (cells[1], cells[2]) not in wanted:
                        continue
                    if len(cells) < 8:
                        raise ValueError(f'row {row_number}: malformed target identity')
                    # Daily exchange archives may also contain the preceding
                    # calendar day's after-hours session. It is not a regular
                    # session print and must never be backdated into this day.
                    if cells[0] != day.strftime('%Y%m%d'):
                        try:
                            event_day = date(int(cells[0][:4]), int(cells[0][4:6]), int(cells[0][6:8]))
                        except (ValueError, IndexError) as exc:
                            raise ValueError(f'row {row_number}: invalid source date') from exc
                        if len(cells[0]) != 8 or event_day > day:
                            raise ValueError(f'row {row_number}: unexpected future source date')
                        continue
                    clock = cells[3]
                    if (len(clock) != 6 or not clock.isdigit() or int(clock[:2]) > 23
                            or int(clock[2:4]) > 59 or int(clock[4:]) > 59):
                        raise ValueError(f'row {row_number}: invalid source time')
                    if not re.fullmatch(r'[1-9][0-9]*', cells[5]):
                        raise ValueError(f'row {row_number}: invalid native quantity')
                    quantity = int(cells[5])
                    if quantity % 2:
                        raise ValueError(f'row {row_number}: native B+S quantity must be even')
                    try:
                        price = float(cells[4])
                    except ValueError as exc:
                        raise ValueError(f'row {row_number}: invalid native price') from exc
                    values.append((f'{day} {clock[:2]}:{clock[2:4]}:{clock[4:]}',
                                   cells[1], cells[2], price, quantity))
    finally:
        after = path.stat()
        if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                or sha256_file(path) != item['sha256']):
            raise SourceChangedError('community RPT object changed during readback')
    if not values:
        raise ValueError('community RPT has no requested contract prints')
    # The historical RPT can include negotiated block prints, which are not
    # executions available to a regular-book strategy. Remove them only when
    # three separately retained official detail sources, the independent block
    # total, and unique native (price, quantity) matches all agree.
    block_checks, remove = [], set()
    if official_block_details is not None and expected.filter(pl.col('block_volume') > 0).height:
        from stockagent.data.tw_futures_decision_clock import dated_entry_sessions
        sessions = {r['physical_contract']:(r['_start'],r['_end'])
                    for r in dated_entry_sessions(expected).to_dicts()}
        by_contract = defaultdict(list)
        for detail in official_block_details:
            by_contract[(detail['product'], detail['contract'])].append(detail)
        for row in expected.filter(pl.col('block_volume') > 0).to_dicts():
            key = (row['product'], row['contract'])
            start, end = sessions[row['physical_contract']]
            def in_session(value):
                t = value[0][11:]
                second = int(t[:2])*3600 + int(t[3:5])*60 + int(t[6:8])
                return start <= second <= end
            candidates = [(i, value) for i,value in enumerate(values)
                          if value[1:3] == key and in_session(value)]
            raw_quantity = sum(value[4]//2 for _,value in candidates)
            details = by_contract[key]
            check = dict(product=key[0], contract=key[1], raw_quantity=raw_quantity,
                official_regular_quantity=row['outright_volume'], official_block_quantity=row['block_volume'],
                official_detail_quantity=sum(d['quantity'] for d in details))
            if raw_quantity == row['outright_volume']:
                check['status'] = 'native_already_regular_book_only'
            elif (raw_quantity != row['outright_volume'] + row['block_volume']
                  or check['official_detail_quantity'] != row['block_volume']):
                check['status'] = 'source_or_official_detail_volume_mismatch'
            else:
                target = Counter((Decimal(d['price']), d['quantity']) for d in details)
                source = defaultdict(list)
                for i, value in candidates:
                    signature = (Decimal(str(value[3])), value[4]//2)
                    if signature in target:
                        source[signature].append(i)
                if any(len(source[signature]) != count for signature,count in target.items()):
                    check['status'] = 'ambiguous_or_missing_exact_block_print'
                else:
                    for signature in target:
                        remove.update(source[signature])
                    check['status'] = 'official_detail_unique_exact_prints_excluded'
                    check['excluded_prints'] = sum(target.values())
            block_checks.append(check)
    if remove:
        values = [value for i,value in enumerate(values) if i not in remove]
    frame = pl.DataFrame(values, schema={'date':pl.String, 'futures_id':pl.String,
        'contract_date':pl.String, 'price':pl.Float64, 'volume':pl.Int64}, orient='row')
    bars, coverage, events = normalize_bulk_stock_ticks(frame, day=day, expected=expected,
        source_sha256=item['sha256'], asset_scope='all_twd', price_representation='native_decimal')
    # Whole-session equality alone cannot certify a unique first trade. Reuse
    # the canonical same-second opening check before permitting any execution.
    openings = events.filter(pl.col('is_session_open') & ~pl.col('opening_ambiguous')).select(
        pl.col('trade_date').alias('date'), 'physical_contract')
    diagnostic = coverage.select('date', 'physical_contract', 'product', 'contract',
        'outright_volume', 'tick_volume', 'high', 'tick_high', 'low', 'tick_low',
        'close', 'tick_close', 'verified').join(
        openings.with_columns(pl.lit(True).alias('opening_unambiguous')),
        on=['date', 'physical_contract'], how='left').with_columns(
            pl.col('date').cast(pl.String), pl.col('opening_unambiguous').fill_null(False))
    admitted = coverage.filter(pl.col('verified') & (pl.col('outright_volume') > 0)).join(
        openings, on=['date', 'physical_contract'], how='semi')
    keys = admitted.select('date', 'physical_contract')
    bars = bars.join(keys, on=['date', 'physical_contract'], how='semi')
    events = events.join(keys.rename({'date':'trade_date'}),
                         on=['trade_date', 'physical_contract'], how='semi')
    return bars, admitted, events, dict(native_rows=len(values),
        candidate_contract_days=expected.height, admitted_contract_days=admitted.height,
        archive_crc='verified_by_full_member_read', historical_point_in_time_verified=False,
        contract_day_diagnostics=diagnostic.to_dicts(), block_checks=block_checks)
