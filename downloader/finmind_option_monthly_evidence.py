"""Pin the exact official monthly sources behind option conflict rechecks.

This is source evidence for the existing FinMind owner, not a downloader or
an assertion that a contract was executable at a decision clock.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import re

from downloader.artifact_io import sha256_file


URL = 'https://www.taifex.com.tw/cht/3/optDataDown'
SOURCE_CONTRACT = 'tw_option_all_product_monthly_range_readback_v1'


def _path(workspace: Path, name: str) -> Path:
    path = Path(name)
    if not path.is_absolute():
        path = workspace / path
    if path.is_symlink() or not path.is_file():
        raise ValueError('monthly option source is missing or redirected')
    resolved = path.resolve()
    if not resolved.is_relative_to(workspace.resolve()):
        raise ValueError('monthly option source is outside the workspace')
    return resolved


def selected_sources(coverage: dict, dates: list[str]) -> list[dict]:
    if coverage.get('contract') != SOURCE_CONTRACT:
        raise ValueError('monthly option readback contract mismatch')
    start, end = date.fromisoformat(coverage['start']), date.fromisoformat(coverage['end'])
    sources = coverage.get('sources')
    if (start > end or start.year != end.year or not isinstance(sources, list)
            or coverage.get('months_verified') != len(sources) or not 0 < len(sources) <= 12):
        raise ValueError('monthly option readback scope mismatch')
    selected = {}
    for day_text in dates:
        day = date.fromisoformat(day_text)
        if day.isoformat() != day_text or not start <= day <= end:
            raise ValueError('option recheck date outside monthly readback')
        matches = [item for item in sources
                   if date.fromisoformat(item['start']) <= day <= date.fromisoformat(item['end'])]
        if len(matches) != 1:
            raise ValueError('option recheck date has ambiguous monthly source')
        item = matches[0]
        first, last = date.fromisoformat(item['start']), date.fromisoformat(item['end'])
        if (first > last or first.year != start.year or last.year != start.year
                or first.month != last.month or not start <= first <= last <= end):
            raise ValueError('invalid monthly source interval')
        selected[(first.isoformat(), last.isoformat())] = item
    return [selected[key] for key in sorted(selected)]


def make_pins(coverage: dict, dates: list[str], workspace: Path) -> list[dict]:
    pins = []
    for source in selected_sources(coverage, dates):
        raw = _path(workspace, source['path'])
        receipt = _path(workspace, source['receipt'])
        intent = _path(workspace, str(raw.with_suffix('.intent.json')))
        if receipt != raw.with_suffix('.receipt.json'):
            raise ValueError('monthly option receipt path differs')
        pins.append({'start': source['start'], 'end': source['end'],
                     'receipt_sha256': sha256_file(receipt), 'intent_sha256': sha256_file(intent)})
    return pins


def validate_sources(coverage: dict, dates: list[str], workspace: Path, pins: list[dict]) -> None:
    sources = selected_sources(coverage, dates)
    if not isinstance(pins, list) or len(pins) != len(sources):
        raise ValueError('monthly option evidence pins mismatch')
    for source, pin in zip(sources, pins):
        if not isinstance(pin, dict) or set(pin) != {'start', 'end', 'receipt_sha256', 'intent_sha256'}:
            raise ValueError('invalid monthly option evidence pin')
        if (pin['start'], pin['end']) != (source['start'], source['end']):
            raise ValueError('monthly option evidence interval mismatch')
        raw = _path(workspace, source['path'])
        receipt_path = _path(workspace, source['receipt'])
        intent_path = _path(workspace, str(raw.with_suffix('.intent.json')))
        if receipt_path != raw.with_suffix('.receipt.json'):
            raise ValueError('monthly option receipt path differs')
        if receipt_path.stat().st_size > 65536 or intent_path.stat().st_size > 65536:
            raise ValueError('oversized monthly option source evidence')
        receipt_bytes, intent_bytes = receipt_path.read_bytes(), intent_path.read_bytes()
        if (len(receipt_bytes) > 65536 or len(intent_bytes) > 65536
                or hashlib.sha256(receipt_bytes).hexdigest() != pin['receipt_sha256']
                or hashlib.sha256(intent_bytes).hexdigest() != pin['intent_sha256']):
            raise ValueError('monthly option source evidence changed')
        first, last = date.fromisoformat(source['start']), date.fromisoformat(source['end'])
        identity = {'contract': 'taifex_option_all_products_daily_raw_v1', 'url': URL,
                    'request': {'down_type': '1', 'queryStartDate': first.strftime('%Y/%m/%d'),
                                'queryEndDate': last.strftime('%Y/%m/%d'),
                                'commodity_id': 'all', 'commodity_id2': ''},
                    'range_start': source['start'], 'range_end': source['end'],
                    'point_in_time_snapshot': False}
        intent, receipt = json.loads(intent_bytes), json.loads(receipt_bytes)
        digest, size = source.get('sha256'), source.get('bytes')
        if (intent != identity or any(receipt.get(key) != value for key, value in identity.items())
                or not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest)
                or type(size) is not int or not 0 < size <= 64 * 1024**2
                or receipt.get('path') != str(raw) or receipt.get('bytes') != size
                or receipt.get('sha256') != digest or raw.stat().st_size != size
                or sha256_file(raw) != digest):
            raise ValueError('monthly option source identity or digest mismatch')
        if receipt_path.read_bytes() != receipt_bytes or intent_path.read_bytes() != intent_bytes:
            raise ValueError('monthly option source evidence changed during validation')
