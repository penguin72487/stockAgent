"""Validate retained TAIFEX block detail responses, including explicit empties."""
from __future__ import annotations

from collections import defaultdict
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
import io
import json
from pathlib import Path

from downloader.artifact_io import sha256_file

ENDPOINTS = {
    'single': ('dlSingleOrderDown', 'dlSingleOrderView'),
    'combination': ('dlCombinationOrderDown', 'dlCombinationOrderView'),
    'bargain': ('dlBargainOrderDown', 'dlBargainOrderView'),
}


def response_status(body: bytes, kind: str) -> str:
    if body.startswith(b'<!DOCTYPE') or b'<html' in body[:400].lower():
        text = body.decode('utf-8')
        if 'alert("查無資料")' in text and f'window.location.replace("{ENDPOINTS[kind][1]}")' in text:
            return 'official_explicit_empty'
        return 'rejected_html'
    text = body.decode('cp950')
    header = text.splitlines()[0] if text else ''
    expected = ('交易日期,契約,到期月份(週別),履約價格,買/賣權,成交價格,成交數量,交易時段'
                if kind == 'single' else '組合式序號,日期,契約數,交易時段')
    return 'complete' if header.lstrip('\ufeff').rstrip(',') == expected else 'rejected_schema'


def _trade(product, contract, strike, option, price, quantity):
    # Options and calendar combinations are separate instruments; only an
    # outright futures leg can be removed from this futures print tape.
    if strike.strip() != '-' or option.strip() != '-':
        return None
    try:
        q, p = int(quantity), Decimal(price)
    except (ValueError, InvalidOperation) as exc:
        raise ValueError('invalid official block price or quantity') from exc
    if not p.is_finite() or p <= 0 or q <= 0:
        raise ValueError('nonpositive official block price or quantity')
    return dict(product=product.strip(), contract=contract.strip(), price=str(p), quantity=q)


def parse_block_detail(body: bytes, kind: str, day: date) -> list[dict]:
    if response_status(body, kind) != 'complete':
        raise ValueError('official block detail response is not a valid table')
    rows = list(csv.reader(io.StringIO(body.decode('cp950'))))
    result = []
    if kind == 'single':
        for row in rows[1:]:
            if len(row) < 8:
                raise ValueError('truncated single block detail')
            if row[0].strip() != day.strftime('%Y/%m/%d'):
                raise ValueError('single block detail date mismatch')
            if row[7].strip() != '一般':
                continue
            value = _trade(*row[1:7])
            if value is not None:
                result.append(value)
        return result
    if len(rows) < 2 or rows[1][:8] != ['組合式序號','子序號','契約','到期月份(週別)',
                                        '履約價格','買/賣權','成交價格','成交數量']:
        raise ValueError('invalid combination block detail header')
    groups = {}
    for row in rows[2:]:
        if len(row) >= 4 and row[1].strip() == day.strftime('%Y/%m/%d'):
            key = row[0].strip()
            if key in groups:
                raise ValueError('duplicate official block combination')
            try:
                groups[key] = dict(expected=int(row[2]), session=row[3].strip(), legs=[])
            except ValueError as exc:
                raise ValueError('invalid official combination size') from exc
        elif len(row) >= 8:
            key = row[0].strip()
            if key not in groups:
                raise ValueError('orphan official block combination leg')
            groups[key]['legs'].append(row)
        elif any(cell.strip() for cell in row):
            raise ValueError('truncated official block combination')
    for group in groups.values():
        if group['expected'] != len(group['legs']) or group['expected'] <= 0:
            raise ValueError('official block combination leg count mismatch')
        if group['session'] != '一般':
            continue
        for leg in group['legs']:
            value = _trade(*leg[2:8])
            if value is not None:
                result.append(value)
    return result


def read_official_block_details(path: Path, dates: list[date]) -> tuple[dict, dict]:
    """Read back three source-specific responses for each requested date."""
    path = Path(path).resolve()
    digest = sha256_file(path)
    manifest = json.loads(path.read_text())
    if manifest.get('contract') != 'taifex_official_block_detail_inventory_v1':
        raise ValueError('invalid official block detail manifest')
    wanted = {str(day) for day in dates}
    result, records = defaultdict(list), []
    seen = set()
    for item in manifest['sources']:
        day, kind = item['date'], item['kind']
        if day not in wanted:
            continue
        if kind not in ENDPOINTS or (day, kind) in seen:
            raise ValueError('duplicate or unknown official block detail source')
        seen.add((day, kind))
        source_day = date.fromisoformat(day)
        expected_url = 'https://www.taifex.com.tw/cht/3/'+ENDPOINTS[kind][0]
        expected_form = dict(queryType='', goDay='', dateaddcnt='',
            queryStartDate=source_day.strftime('%Y/%m/%d'), queryEndDate=source_day.strftime('%Y/%m/%d'))
        member = (path.parent/item['path']).resolve()
        if (not member.is_relative_to(path.parent) or item['url'] != expected_url
                or item['method'] != 'POST' or item['form'] != expected_form or item['http_status'] != 200):
            raise ValueError('invalid official block detail provenance')
        data = member.read_bytes()
        if len(data) != item['bytes'] or sha256_file(member) != item['sha256']:
            raise ValueError('official block detail SHA or size mismatch')
        status = response_status(data, kind)
        if status != item['status'] or status not in ('complete', 'official_explicit_empty'):
            raise ValueError('official block detail status or schema mismatch')
        if status == 'complete':
            result[day].extend(parse_block_detail(data, kind, source_day))
        records.append({k:item[k] for k in ('date','kind','url','sha256','status','bytes')})
    if seen != {(d, kind) for d in wanted for kind in ENDPOINTS}:
        raise ValueError('official block detail date lacks a required response')
    if sha256_file(path) != digest:
        raise ValueError('official block detail manifest changed during readback')
    return dict(result), dict(path=str(path), sha256=digest, sources=records)
