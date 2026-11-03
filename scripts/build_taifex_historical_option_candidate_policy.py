#!/usr/bin/env python3
"""Extend the pinned research-only option policy with observed historical families.

This produces no listing, time-dependent eligibility, accounting or training
admission. Product scopes and cash currencies are kept separate.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_json, sha256_file


ADDITIONS = {
    'GTO': {'name': '櫃買選擇權', 'class': 'index_option', 'cash_currency': 'TWD',
            'day_minutes': [525, 825], 'official_lifecycle': ['2007-10-08', '2019-09-19']},
    'XIO': {'name': '非金電選擇權', 'class': 'index_option', 'cash_currency': 'TWD',
            'day_minutes': [525, 825], 'official_lifecycle': ['2007-10-08', '2019-09-19']},
    'MSO': {'name': 'MSCI臺指選擇權', 'class': 'index_option', 'cash_currency': 'USD',
            'day_minutes': [525, 825], 'official_lifecycle': ['2006-03-27', '2011-09-22']},
    'RHO': {'name': '美元兌人民幣選擇權', 'class': 'currency_option', 'cash_currency': 'CNY',
            'day_minutes': [525, 975], 'official_end': '2022-09-22'},
    'RTO': {'name': '小型美元兌人民幣選擇權', 'class': 'currency_option', 'cash_currency': 'CNY',
            'day_minutes': [525, 975], 'official_end': '2022-09-22'},
}
REFERENCE_URLS = {
    'historical_index_options': 'https://www.taifex.com.tw/chinese/txoevent/in3.html',
    'historical_index_lifecycle': 'https://www.taifex.com.tw/cht/5/optIndxFSP',
    'historical_index_cash_units': 'https://www.taifex.com.tw/file/taifex/CHINESE/10/新商品手冊-棕(完稿0919).pdf',
    'historical_mso_usd': 'https://www.taifex.com.tw/file/taifex/CHINESE/11/TechDocs/66/資訊傳輸作業手冊(V2.31.0S).pdf',
    'historical_rho_specification': 'https://www.taifex.com.tw/file/taifex/event/cht/AfterHoursTrading/8.盤後交易商品-美元兌人民幣選擇權.pdf',
    'historical_rto_hours': 'https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/造市者報價手續費折減及其他相關規定_202010(網頁).pdf',
    'historical_currency_lifecycle': 'https://www.taifex.com.tw/cht/5/fXFSP',
}


def source_inventory(summary_path: Path) -> tuple[dict, list[dict], Path]:
    summary = json.loads(summary_path.read_bytes())
    if summary.get('contract') != 'taifex_option_archive_product_day_inventory_v1':
        raise ValueError('unsupported official annual inventory')
    inventory = Path(summary['product_inventory']).resolve()
    if sha256_file(inventory) != summary['product_inventory_sha256']:
        raise ValueError('official annual product inventory differs from its receipt')
    with inventory.open(newline='', encoding='utf-8') as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) != summary['distinct_observed_products'] or len({row['product'] for row in rows}) != len(rows):
        raise ValueError('official annual inventory product count differs')
    return summary, rows, inventory


def assemble(base_path: Path, summary_path: Path) -> dict:
    base_path, summary_path = base_path.resolve(), summary_path.resolve()
    base = json.loads(base_path.read_bytes())
    if base.get('contract') != 'taifex_option_session_policy_candidate_v1' or base.get('research_candidate_only') is not True:
        raise ValueError('base policy is not a research-only candidate')
    summary, rows, inventory = source_inventory(summary_path)
    original = set(base['products'])
    observed = {row['product'] for row in rows}
    positive = {row['product'] for row in rows if int(row['positive_volume_days']) > 0}
    if observed - original != set(ADDITIONS) or not set(ADDITIONS).issubset(positive):
        raise ValueError('historical scope differs; review the full source inventory before extending')
    row_by_product = {row['product']: row for row in rows}
    for product, item in ADDITIONS.items():
        row = row_by_product[product]
        if row['provisional_class'] != item['class']:
            raise ValueError('historical class disagrees with the official inventory')
        base['products'][product] = {'day_minutes': item['day_minutes'], 'night_minutes': None}
    base['reference_urls'].update(REFERENCE_URLS)
    base['historical_extension'] = {
        'contract': 'taifex_historical_option_candidate_policy_extension_v1',
        'base_policy_path': str(base_path), 'base_policy_sha256': sha256_file(base_path),
        'inventory_summary_path': str(summary_path), 'inventory_summary_sha256': sha256_file(summary_path),
        'inventory_path': str(inventory), 'inventory_sha256': sha256_file(inventory),
        'observed_product_count': len(observed), 'positive_product_count': len(positive),
        'previously_uncovered_products': sorted(ADDITIONS),
        'uncovered_observed_products_after_extension': sorted(observed - set(base['products'])),
        'additions': ADDITIONS,
        'limitations': ['Hours are source-comparison candidates, not dated trading permissions.',
                        'USD and CNY option cash accounts cannot be silently combined with TWD.',
                        'The official daily-product union does not prove complete ticks, live sessions, or fills.',
                        'Night-session, expiry-day and exceptional-calendar eligibility require separate rules.']}
    base['limitations'] += (' This historical extension is research-only; source dates, opening windows, '
                            'FX accounting and listed-contract eligibility still need dated verification.')
    return base


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', type=Path, required=True)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--verify', action='store_true')
    args = parser.parse_args()
    expected = assemble(args.base, args.summary)
    if args.verify:
        if json.loads(args.output.read_bytes()) != expected:
            raise ValueError('historical candidate policy no longer matches its inputs')
    else:
        if args.output.exists() or args.output.is_symlink():
            raise ValueError('preserve the existing research policy')
        atomic_write_json(args.output, expected)
    print(json.dumps({'policy': str(args.output), 'sha256': sha256_file(args.output),
                      'products': len(expected['products']),
                      'uncovered_observed_products': expected['historical_extension']['uncovered_observed_products_after_extension'],
                      'training_ready': False}), flush=True)


if __name__ == '__main__':
    main()
