#!/usr/bin/env python3
"""Audit retained recent official option events against FinMind date objects.

Uses an explicitly research-only comparison-window template. Products outside
that template are reported rather than assigned guessed historical sessions.
This is not an execution, point-in-time, or all-history admission proof.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import zipfile

from bs4 import BeautifulSoup

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_contract_events import audit


def observed_products(path: Path, expected_sha: str) -> list[str]:
    if sha256_file(path) != expected_sha:
        raise ValueError("official option source digest mismatch")
    products: set[str] = set()
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith('.csv')]
        if len(members) != 1:
            raise ValueError("ambiguous official option archive")
        with archive.open(members[0]) as binary:
            text = io.TextIOWrapper(binary, encoding='cp950', newline='')
            for row in csv.reader(text):
                if len(row) >= 9 and row[0].strip().isdigit():
                    products.add(row[1].strip().upper())
    if sha256_file(path) != expected_sha:
        raise ValueError("official option source changed during product inventory")
    return sorted(products)


def expand_stock_option_windows(template: dict, inventory: Path, receipt: Path) -> tuple[dict, dict]:
    """Extend a comparison-only window from the pinned provisional inventory."""
    receipt_bytes = receipt.read_bytes()
    evidence = json.loads(receipt_bytes)
    if (evidence.get('contract') != 'taifex_option_archive_product_day_inventory_v1'
            or Path(evidence['product_inventory']).resolve() != inventory.resolve()
            or sha256_file(inventory) != evidence['product_inventory_sha256']):
        raise ValueError('provisional option inventory evidence mismatch')
    products = dict(template['products'])
    added = []
    with inventory.open(encoding='utf-8', newline='') as source:
        for row in csv.DictReader(source):
            if row['provisional_class'] not in {'security_option_candidate', 'etf_option_candidate'}:
                continue
            product = row['product'].strip().upper()
            if product and product not in products:
                products[product] = {'day_minutes': [525, 825], 'night_minutes': None}
                added.append(product)
    if sha256_file(inventory) != evidence['product_inventory_sha256'] or receipt.read_bytes() != receipt_bytes:
        raise ValueError('provisional option inventory changed')
    pins = {'inventory_path': str(inventory), 'inventory_sha256': evidence['product_inventory_sha256'],
            'receipt_path': str(receipt), 'receipt_sha256': hashlib.sha256(receipt_bytes).hexdigest(),
            'added_provisional_products': sorted(added)}
    return {**template, 'products': products, 'inventory_comparison_pins': pins}, pins


def apply_current_stock_comparison_windows(template: dict, receipt: Path, workspace: Path) -> tuple[dict, dict]:
    """Apply the pinned current list's day windows to comparison candidates only."""
    receipt_bytes = receipt.read_bytes()
    record = json.loads(receipt_bytes)
    matches = [item for item in record['source_evidence']
               if item['url'] == 'https://www.taifex.com.tw/cht/2/stockLists']
    if record.get('contract') != 'current_official_family_catalog_membership_readback_v1' or len(matches) != 1:
        raise ValueError('invalid current exchange catalog receipt')
    source = matches[0]
    raw_path = (workspace / source['raw_path']).resolve()
    if not raw_path.is_relative_to(workspace.resolve()) or sha256_file(raw_path) != source['raw_sha256']:
        raise ValueError('current exchange catalog raw digest mismatch')
    body = gzip.decompress(raw_path.read_bytes())
    if hashlib.sha256(body).hexdigest() != source['body_sha256']:
        raise ValueError('current exchange catalog body digest mismatch')
    soup = BeautifulSoup(body, 'html.parser')
    tables = [table for table in soup.find_all('table') if '證券代號' in table.get_text()]
    if len(tables) != 1:
        raise ValueError('ambiguous current exchange catalog table')
    products = dict(template['products'])
    changed = {}
    indexes = None
    for row in tables[0].find_all('tr'):
        headers = [re.sub(r'\s+', '', cell.get_text()) for cell in row.find_all('th')]
        if headers:
            try:
                indexes = (headers.index('股票期貨、選擇權商品代碼'),
                           headers.index('是否為股票選擇權標的'),
                           next(i for i, text in enumerate(headers)
                                if '一般交易時段' in text and '交易時間' in text))
            except (ValueError, StopIteration) as error:
                raise ValueError('current exchange catalog schema changed') from error
            continue
        cells = [cell.get_text(' ', strip=True) for cell in row.find_all('td')]
        if not cells:
            continue
        if indexes is None or len(cells) <= max(indexes):
            raise ValueError('incomplete current exchange catalog row')
        root, option_member, session = (cells[index] for index in indexes)
        if '是股票選擇權標的' not in option_member:
            continue
        if not re.fullmatch(r'[A-Z0-9]{2,5}', root):
            raise ValueError('invalid current exchange option family')
        match = re.fullmatch(r'(\d{1,2}):(\d{2})\s*[~～]\s*(\d{1,2}):(\d{2})', session)
        if not match:
            raise ValueError('invalid current exchange option day window')
        hours = [int(part) for part in match.groups()]
        if hours[0] >= 24 or hours[2] >= 24 or hours[1] >= 60 or hours[3] >= 60:
            raise ValueError('current exchange option day window out of range')
        window = [hours[0] * 60 + hours[1], hours[2] * 60 + hours[3]]
        product = root + 'O'
        if product in products:
            before = products[product]['day_minutes']
            products[product] = {**products[product], 'day_minutes': window}
            if before != window:
                changed[product] = {'before': before, 'after': window}
    if sha256_file(raw_path) != source['raw_sha256'] or receipt.read_bytes() != receipt_bytes:
        raise ValueError('current exchange catalog changed during comparison setup')
    pins = {'receipt_path': str(receipt), 'receipt_sha256': hashlib.sha256(receipt_bytes).hexdigest(),
            'raw_path': str(raw_path), 'raw_sha256': source['raw_sha256'],
            'observed_at_utc': source['checked_at_utc'], 'changed_windows': changed,
            'historical_eligibility_verified': False}
    return {**template, 'products': products, 'current_catalog_comparison_pins': pins}, pins


def run(template_path: Path, tick_root: Path, finmind_root: Path, output: Path,
        inventory: Path | None = None, inventory_receipt: Path | None = None,
        current_catalog_receipt: Path | None = None, workspace: Path = Path('.')) -> dict:
    if output.exists():
        raise FileExistsError("preserve existing recent-event audit")
    template_bytes = template_path.read_bytes()
    template = json.loads(template_bytes)
    if template.get('contract') != 'taifex_option_session_policy_candidate_v1' or not template.get('research_candidate_only'):
        raise ValueError("invalid research comparison-window template")
    if (inventory is None) != (inventory_receipt is None):
        raise ValueError('inventory and receipt must be supplied together')
    inventory_pins = None
    if inventory is not None and inventory_receipt is not None:
        template, inventory_pins = expand_stock_option_windows(template, inventory, inventory_receipt)
    current_pins = None
    if current_catalog_receipt is not None:
        template, current_pins = apply_current_stock_comparison_windows(template, current_catalog_receipt, workspace)
    manifest_path = tick_root / 'manifest.json'
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    entries = [row for row in manifest['raw_downloads'] if row['kind'] == 'options']
    if not entries or len(entries) > 31 or len({row['trading_date'] for row in entries}) != len(entries):
        raise ValueError("official recent option manifest scope is invalid")
    output.mkdir(parents=True)
    atomic_write_json(output / 'expanded_comparison_template.json', template)
    results = []
    for entry in sorted(entries, key=lambda item: item['trading_date']):
        day = date.fromisoformat(entry['trading_date'])
        source = Path(entry['path'])
        names = observed_products(source, entry['sha256'])
        selected = sorted(set(names) & set(template['products']))
        unknown = sorted(set(names) - set(template['products']))
        record = {'date': day.isoformat(), 'observed_products': names,
                  'products_without_candidate_windows': unknown, 'selected_products': selected}
        receipt = finmind_root / 'receipts/TaiwanOptionTick/all' / f'{day}.json'
        if not selected or not receipt.exists():
            record['state'] = 'no_candidate_products' if not selected else 'missing_vendor_receipt'
        else:
            vendor = json.loads(receipt.read_text(encoding='utf-8'))
            if vendor.get('status') != 'complete':
                record['state'] = 'vendor_receipt_' + str(vendor.get('status', 'unknown'))
            else:
                policy = {**template, 'trading_date': day.isoformat(),
                          'products': {product: template['products'][product] for product in selected},
                          'limitations': ('Fixed research comparison windows carried from the pinned template; '
                                          'they are not evidence of dated listing or historical session rules.')}
                policy_path = output / f'{day}_candidate_policy.json'
                atomic_write_json(policy_path, policy)
                try:
                    result = audit(day, policy_path, tick_root, finmind_root)
                except (OSError, ValueError, KeyError, TypeError) as error:
                    record.update(state='audit_error', error_type=type(error).__name__, error=str(error)[:300])
                else:
                    result_path = output / f'{day}_event_readback.json'
                    atomic_write_json(result_path, result)
                    record.update(
                        state='source_event_readback_verified', output=str(result_path),
                        output_sha256=sha256_file(result_path),
                        missing_side_rows=sum(item['source_rows_missing_from_finmind'] for item in result['products']),
                        extra_side_rows=sum(item['source_rows_extra_in_finmind'] for item in result['products']),
                    )
        results.append(record)
        print(json.dumps({'date': str(day), 'state': record['state'],
                          'missing_side_rows': record.get('missing_side_rows'),
                          'products_without_candidate_windows': unknown}, ensure_ascii=False), flush=True)
    if template_path.read_bytes() != template_bytes or manifest_path.read_bytes() != manifest_bytes:
        raise ValueError("source manifest or window template changed during audit")
    if inventory_pins and (sha256_file(inventory) != inventory_pins['inventory_sha256']
                           or sha256_file(inventory_receipt) != inventory_pins['receipt_sha256']):
        raise ValueError('provisional option inventory changed during audit')
    if current_pins and (sha256_file(Path(current_pins['raw_path'])) != current_pins['raw_sha256']
                         or sha256_file(current_catalog_receipt) != current_pins['receipt_sha256']):
        raise ValueError('current exchange catalog changed during audit')
    summary = {
        'contract': 'taifex_recent_option_event_coverage_v1',
        'completed_at_utc': datetime.now(timezone.utc).isoformat(),
        'official_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
        'window_template_sha256': hashlib.sha256(template_bytes).hexdigest(),
        'provisional_inventory_pins': inventory_pins,
        'current_catalog_comparison_pins': current_pins,
        'script_sha256': sha256_file(Path(__file__)),
        'days': results,
        'limitations': ['Fixed candidate windows are not dated rule or listing proof.',
                        'Whole-second side rows and float32 comparisons do not prove quotes or fills.',
                        'Unlisted products and missing vendor receipts are not verified.',
                        'These 30 recent dates do not establish other historical dates.'],
    }
    atomic_write_json(output / 'summary.json', summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--template', required=True, type=Path)
    parser.add_argument('--tick-root', type=Path, default=Path('data_tw_index_derivatives_ticks'))
    parser.add_argument('--finmind-root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--inventory', type=Path)
    parser.add_argument('--inventory-receipt', type=Path)
    parser.add_argument('--current-catalog-receipt', type=Path)
    parser.add_argument('--workspace', type=Path, default=Path('.'))
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    run(args.template.resolve(), args.tick_root.resolve(), args.finmind_root.resolve(), args.output.resolve(),
        args.inventory.resolve() if args.inventory else None,
        args.inventory_receipt.resolve() if args.inventory_receipt else None,
        args.current_catalog_receipt.resolve() if args.current_catalog_receipt else None,
        args.workspace.resolve())


if __name__ == '__main__':
    main()
