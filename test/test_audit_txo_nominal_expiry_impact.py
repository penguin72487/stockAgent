from datetime import date, datetime
from io import StringIO
import sqlite3
from zoneinfo import ZoneInfo

import pytest

from scripts.audit_txo_nominal_expiry_impact import _group_events, _strike_key
from scripts.audit_txo_nominal_expiry_native_cases import _native_csv_keys
from stockagent.data.tw_option_full_carry_index import create_schema


def test_forward_shift_groups_only_the_target_series_and_date() -> None:
    zone = ZoneInfo('Asia/Taipei')
    db = sqlite3.connect(':memory:')
    create_schema(db)
    def stamp(day: int, hour: int = 9) -> int:
        return int(datetime(2026, 10, day, hour, tzinfo=zone).timestamp()) * 1_000_000
    rows = [
        ('TXO', '202610F4', '22000', 'C', stamp(23), '1', 'a' * 64, 0),
        ('TXO', '202610F4', '22000.0', 'C', stamp(27), '1', 'a' * 64, 0),
        ('TXO', '202610F4', '22000', 'C', stamp(27, 10), '2', 'a' * 64, 0),
        ('TXO', '202610F4', '22000', 'P', stamp(27), '3', 'a' * 64, 0),
        ('TXO', '202610F4', '22000', 'P', stamp(28), '4', 'a' * 64, 0),
        ('TXO', '202610F5', '22000', 'C', stamp(27), '5', 'a' * 64, 0),
    ]
    db.executemany('INSERT INTO events VALUES (?,?,?,?,?,?,?,?)', rows)
    result = _group_events(db, '202610F4', date(2026, 10, 23), date(2026, 10, 27))
    assert result == {date(2026, 10, 27): {('22000', 'C'), ('22000', 'P')}}
    with pytest.raises(ValueError, match='forward settlement shift'):
        _group_events(db, '202610F4', date(2026, 10, 23), date(2026, 10, 23))


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-1', '123.5', 'not-a-number'])
def test_cross_source_strike_identity_fails_closed(value: str) -> None:
    with pytest.raises(ValueError):
        _strike_key(value)


def test_native_csv_uses_day_session_and_positive_volume() -> None:
    csv = StringIO('交易日期,契約,到期月份(週別),履約價,買賣權,成交量,交易時段\n'
                   '2024/07/26,TXO,202407W4,22000,買權,3,一般\n'
                   '2024/07/26,TXO,202407W4,22000,賣權,0,一般\n'
                   '2024/07/26,TXO,202407W4,22100,買權,9,盤後\n')
    keys, positive = _native_csv_keys(csv, day=date(2024, 7, 26), series='202407W4')
    assert keys == {('22000', 'C'), ('22000', 'P')}
    assert positive == {('22000', 'C')}
