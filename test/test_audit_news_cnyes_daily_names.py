from datetime import datetime
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from scripts import audit_news_cnyes_daily_names as audit


def _raw(root, venue, day, names, *, url_override=None):
    spec = audit.VENUES[venue]
    fields = [spec['symbol'], spec['name'], '其他欄位']
    payload = {'stat': 'OK', 'date': day.replace('-', ''), 'tables': [{'title': spec['title'],
        'fields': fields, 'data': [[symbol, name, 'PRIVATE_NOT_NAME'] for symbol, name in names]}]}
    raw = json.dumps(payload, ensure_ascii=False).encode()
    path = root / 'raw' / spec['dataset'] / (day + '.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    domain = sorted(spec['hosts'])[0]
    value = day.replace('-', '') if venue == 'twse' else day.replace('-', '/')
    url = url_override or f'https://{domain}{spec["path"]}?date={value}&type={spec["type"]}'
    item = {'dataset': spec['dataset'], 'date': day, 'status': 'data', 'raw_path': str(path.relative_to(root)),
        'raw_sha256': hashlib.sha256(raw).hexdigest(), 'raw_size': len(raw),
        'recorded_at_utc': '2026-10-01T00:00:00+00:00', 'source': 'raw_bootstrap', 'url': url}
    journal = root / 'state/journals' / (spec['dataset'] + '.jsonl')
    journal.parent.mkdir(parents=True, exist_ok=True)
    with journal.open('a') as stream:
        stream.write(json.dumps(item, ensure_ascii=False) + '\n')
    return path


def _row(number, symbol, name, day='2024-05-15'):
    epoch = int(datetime.fromisoformat(day + 'T09:01:00').replace(tzinfo=ZoneInfo('Asia/Taipei')).timestamp())
    return {'news_id': number, 'source_post_sha256': f'{number:064x}', 'stock_symbol_candidate': symbol,
        'publisher_day_taipei': day, 'publisher_epoch': epoch, 'title_company_prefix_literal': name,
        'title_subject_literal': '公告', 'tentative_finlab_key': [symbol, day, '公告'],
        'market_names_for_symbol': ['另一名稱']}


def _fixture(root):
    _raw(root, 'twse', '2024-05-15', [('1234', '測試公司'), ('5678', '甲公司'), ('7890', '實際名稱')])
    _raw(root, 'tpex', '2024-05-15', [('5678', '乙公司')])
    rows = [_row(1, '1234', '測試公司'), _row(2, '5678', '甲公司'), _row(3, '7890', '不同名稱'),
            _row(4, '9999', '未找到'), _row(5, '1234', '測試公司', '2024-05-16'),
            {'news_id': 6, 'source_post_sha256': 'f' * 64, 'tentative_finlab_key': None}]
    return {'contract': 'cnyes_unkeyed_tentative_issuer_v1', 'accepted': True, 'source_failures': [], 'rows': rows,
        'counts': {'unkeyed_targets': len(rows), 'verified_post_identities': len(rows),
                   'source_envelopes_failed': 0, 'changed_heads': 0, 'unverified_post_identities': 0}}


def test_receipt_backed_same_day_names_keep_conflict_and_availability_separate(tmp_path):
    report = _fixture(tmp_path)
    result = audit.audit(report, tmp_path)
    assert result['accepted'] and result['counts']['tentative_rows_checked'] == 5
    assert result['counts']['unkeyed_without_candidate'] == 1 and result['counts']['source_days'] == 2
    assert {row['news_id']: row['state'] for row in result['rows']} == {
        1: 'same_day_literal_name_match', 2: 'conflicting_same_day_names', 3: 'same_day_different_name',
        4: 'no_listed_name_in_both_verified_venues', 5: 'daily_sources_incomplete'}
    row = result['rows'][0]
    assert row['cnyes_market_names_for_symbol'] == ['另一名稱'] and row['exchange_names_by_venue']['twse'] == ['測試公司']
    assert row['official_event_identity_verified'] is row['point_in_time_availability_verified'] is False
    evidence = result['source_day_evidence']['2024-05-15']['twse']
    assert evidence['receipts'][0]['source'] == 'raw_bootstrap'
    assert evidence['receipts'][0]['recorded_at_utc'] == '2026-10-01T00:00:00+00:00'
    assert 'PRIVATE_NOT_NAME' not in json.dumps(result, ensure_ascii=False)
    assert result['publisher_requests'] == result['queue_writes'] == 0


def test_raw_hash_mismatch_never_creates_a_name_match(tmp_path):
    report = _fixture(tmp_path)
    path = tmp_path / 'raw/twse_daily_ohlcv/2024-05-15.json'
    path.write_bytes(path.read_bytes() + b' ')
    result = audit.audit(report, tmp_path)
    assert result['accepted'] is False and result['counts']['invalid_source_day_venues'] == 1
    assert result['source_day_evidence']['2024-05-15']['twse']['state'] == 'raw_receipt_mismatch'
    assert result['rows'][0]['state'] == 'daily_sources_incomplete'


def test_unapproved_receipt_domain_is_not_source_proof(tmp_path):
    _raw(tmp_path, 'twse', '2024-05-15', [('1234', '測試公司')],
         url_override='https://example.com/rwd/zh/afterTrading/MI_INDEX?date=20240515&type=ALLBUT0999')
    _raw(tmp_path, 'tpex', '2024-05-15', [])
    report = _fixture_report_one()
    result = audit.audit(report, tmp_path)
    assert result['accepted'] is False
    assert result['source_day_evidence']['2024-05-15']['twse']['state'] == 'raw_receipt_mismatch'


def test_conflicting_duplicate_date_parameter_is_not_source_proof():
    spec = audit.VENUES['twse']
    assert not audit._journal_url_ok(
        'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date=20240515&date=20240516&type=ALLBUT0999',
        spec, '2024-05-15')


def test_extra_nested_date_cannot_hide_matching_top_level_receipt(tmp_path):
    _raw(tmp_path, 'twse', '2024-05-15', [('1234', '測試公司')])
    _raw(tmp_path, 'tpex', '2024-05-15', [])
    journal = tmp_path / 'state/journals/twse_daily_ohlcv.jsonl'
    item = json.loads(journal.read_text())
    journal.write_text(json.dumps({'details': {'date': '2024-05-14'}, **item}, ensure_ascii=False) + '\n')
    result = audit.audit(_fixture_report_one(), tmp_path)
    assert result['accepted'] is True
    assert result['rows'][0]['state'] == 'same_day_literal_name_match'


def _fixture_report_one():
    return {'contract': 'cnyes_unkeyed_tentative_issuer_v1', 'accepted': True, 'source_failures': [],
        'rows': [_row(1, '1234', '測試公司')],
        'counts': {'unkeyed_targets': 1, 'verified_post_identities': 1, 'source_envelopes_failed': 0,
                   'changed_heads': 0, 'unverified_post_identities': 0}}


def test_publication_day_must_match_source_epoch(tmp_path):
    report = _fixture(tmp_path)
    report['rows'][0]['publisher_epoch'] -= 86400
    with pytest.raises(ValueError, match='candidate_date_mismatch'):
        audit.audit(report, tmp_path)
