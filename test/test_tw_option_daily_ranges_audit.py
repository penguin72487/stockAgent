from datetime import date
import hashlib
import json

import pytest

from scripts.audit_tw_option_daily_ranges import audit
from scripts.download_taifex_option_daily_history import TAIFEX_OPTION_DOWNLOAD_URL


def _range(root, first, last, rows):
    root.mkdir(parents=True, exist_ok=True)
    path = root / f'{first.isoformat()}_{last.isoformat()}_all.csv'
    header = '交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,收盤價,結算價,成交量,交易時段\n'
    path.write_bytes((header + rows).encode('cp950'))
    identity = {'contract': 'taifex_option_all_products_daily_raw_v1', 'url': TAIFEX_OPTION_DOWNLOAD_URL,
                'request': {'down_type': '1', 'queryStartDate': first.strftime('%Y/%m/%d'),
                            'queryEndDate': last.strftime('%Y/%m/%d'),
                            'commodity_id': 'all', 'commodity_id2': ''},
                'range_start': first.isoformat(), 'range_end': last.isoformat(),
                'point_in_time_snapshot': False}
    path.with_suffix('.intent.json').write_text(json.dumps(identity), encoding='utf-8')
    receipt = {**identity, 'path': str(path), 'bytes': path.stat().st_size,
               'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    path.with_suffix('.receipt.json').write_text(json.dumps(receipt), encoding='utf-8')
    return path


def test_readback_preserves_duplicate_unknown_volume_and_session(tmp_path):
    _range(tmp_path, date(2026, 1, 1), date(2026, 1, 31),
           '2026/01/03,TXO,202601F1,23000,買權,1,2,2,5,一般\n'
           '2026/01/03,TXO,202601F1,23000,買權,1,2,2,5,一般\n'
           '2026/01/03,CAO,202601,23.50,賣權,1,2,2,-,\n')
    _range(tmp_path, date(2026, 2, 1), date(2026, 2, 28),
           '2026/02/03,CAO,202602,23.50,賣權,1,2,2,2,盤後\n')
    result = audit(tmp_path, date(2026, 1, 1), date(2026, 2, 28), include_daily_products=True)
    assert result['months_verified'] == 2
    assert result['raw_rows'] == 4
    assert result['product_union_count'] == result['positive_volume_product_union_count'] == 2
    assert result['observed_products'] == ['CAO', 'TXO']
    assert result['unknown_volume_rows'] == 1
    assert result['sessions'] == {'day': 2, 'night': 1, 'unreported': 1}
    assert result['series_scopes'] == {'monthly_candidate': 2, 'weekly_candidate': 2}
    assert result['daily_positive_products'] == [
        {'date': '2026-01-03', 'day_positive_products': 'TXO',
         'unreported_session_positive_products': '', 'night_positive_products': '',
         'unknown_session_positive_products': ''},
        {'date': '2026-02-03', 'day_positive_products': '',
         'unreported_session_positive_products': '', 'night_positive_products': 'CAO',
         'unknown_session_positive_products': ''},
    ]


@pytest.mark.parametrize('case', ['missing', 'digest', 'outside', 'intent', 'symlink'])
def test_readback_rejects_incomplete_or_inconsistent_evidence(tmp_path, case):
    if case == 'missing':
        with pytest.raises(ValueError, match='missing'):
            audit(tmp_path, date(2026, 1, 1), date(2026, 1, 31))
        return
    day = '2026/02/01' if case == 'outside' else '2026/01/03'
    path = _range(tmp_path, date(2026, 1, 1), date(2026, 1, 31),
                  f'{day},TXO,202601,23000,買權,1,2,2,5,一般\n')
    if case == 'digest':
        with path.open('ab') as out:
            out.write(b'x')
    elif case == 'intent':
        path.with_suffix('.intent.json').write_text('{}', encoding='utf-8')
    elif case == 'symlink':
        actual = tmp_path / 'actual.csv'
        path.rename(actual)
        path.symlink_to(actual)
    with pytest.raises(ValueError):
        audit(tmp_path, date(2026, 1, 1), date(2026, 1, 31))
