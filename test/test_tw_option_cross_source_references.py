from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from scripts.build_tw_option_cross_source_references import merge_reference_oracles, reference_map, snapshot_receipt
from scripts.audit_tw_option_cross_source_references import audit_rows
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_event_index import compact_decision_events


KEY = ('TXO', '202610', '20000', 'C')
A, B = 'a' * 64, 'b' * 64


def record(kind, price, stamp, sha):
    return (kind, price, stamp, (sha,))


def row(day, clock, price, sha):
    return {'event_ts': datetime.fromisoformat(f'{day}T{clock}').replace(tzinfo=ZoneInfo('Asia/Taipei')),
            'product_raw': KEY[0], 'series_raw': KEY[1], 'strike_raw': KEY[2], 'right_raw': KEY[3],
            'price_raw': str(price), 'source_sha256': sha}


def test_first_observed_open_overrides_a_later_trade_or_other_source_open():
    left = {KEY: record('observed_session_open_candidate', '10', '20261021084600', A)}
    right = {KEY: record('observed_session_open_candidate', '11', '20261021084700', B)}
    assert merge_reference_oracles(left, right)[KEY] == left[KEY]
    later_last = {KEY: record('last_observed_trade_candidate', '12', '20261021084800', B)}
    assert merge_reference_oracles(left, later_last)[KEY] == left[KEY]


def test_same_second_disagreement_stays_ambiguous_and_source_bound():
    left = {KEY: record('observed_session_open_candidate', '10', '20261021084600', A)}
    right = {KEY: record('observed_session_open_candidate', '11', '20261021084600', B)}
    assert merge_reference_oracles(left, right)[KEY] == (
        'ambiguous_first_second', None, '20261021084600', (A, B))
    right[KEY] = record('observed_session_open_candidate', '10', '20261021084600', B)
    assert merge_reference_oracles(left, right)[KEY] == (
        'observed_session_open_candidate', '10', '20261021084600', (A, B))
    left[KEY] = record('ambiguous_first_second', None, '20261021084600', A)
    assert merge_reference_oracles(left, right)[KEY][0:2] == ('ambiguous_first_second', None)


def test_last_trade_and_missing_side_are_not_assumed_fills():
    left = {KEY: record('last_observed_trade_candidate', '9', '20261020130000', A)}
    right = {KEY: record('last_observed_trade_candidate', '10', '20261021084459', B)}
    assert merge_reference_oracles(left, right)[KEY] == right[KEY]
    assert merge_reference_oracles(left, {}) == left
    with pytest.raises(ValueError, match='unsupported reference'):
        merge_reference_oracles({KEY: record('invented', '1', '20261021080000', A)}, {})


def test_separate_event_indexes_preserve_open_gate_and_cross_source_ties():
    day = date(2026, 10, 21)
    windows = {'TXO': (525, 825)}
    official = pl.DataFrame([row('2026-10-21', '08:44:59', 9, A),
                             row('2026-10-21', '08:45:00', 10, A),
                             row('2026-10-21', '08:46:00', 12, A)])
    finmind = pl.DataFrame([row('2026-10-21', '08:44:58', 8, B),
                            row('2026-10-21', '08:45:00', 11, B),
                            row('2026-10-21', '09:00:00', 99, B)])
    compact = pl.concat([compact_decision_events(source, day=day, session_windows=windows)
                         for source in (official, finmind)])
    for clock in ('08:45', '09:00'):
        actual = reference_map(reference_candidates(compact, trading_date=day,
                                                     decision_time=clock, session_windows=windows))
        full = reference_map(reference_candidates(pl.concat([official, finmind]), trading_date=day,
                                                   decision_time=clock, session_windows=windows))
        assert actual == full
    early = reference_map(reference_candidates(compact, trading_date=day, decision_time='08:45',
                                                session_windows=windows))[KEY]
    assert early[:3] == ('last_observed_trade_candidate', '9', '20261021084459')
    late = reference_map(reference_candidates(compact, trading_date=day, decision_time='09:00',
                                               session_windows=windows))[KEY]
    assert late == ('ambiguous_first_second', None, '20261021084500', (A, B))


def test_independent_readback_rejects_future_unbound_and_duplicate_rows():
    day = date(2026, 10, 21)
    trade = datetime.fromisoformat('2026-10-21T08:44:59').replace(tzinfo=ZoneInfo('Asia/Taipei'))
    from datetime import timedelta
    valid = {'decision_time': '08:45', 'decision_date': day, 'product_raw': KEY[0],
             'series_raw': KEY[1], 'strike_decimal': KEY[2], 'right_raw': KEY[3],
             'trade_ts': trade, 'available_by': trade + timedelta(seconds=1),
             'reference_kind': 'last_observed_trade_candidate', 'reference_price_decimal': '9',
             'source_hashes': [A]}
    assert audit_rows([valid], day, {A}, {B})['08:45']['keys'] == 1
    with pytest.raises(ValueError, match='observable'):
        audit_rows([valid | {'available_by': valid['available_by'] + timedelta(seconds=1)}], day, {A}, {B})
    with pytest.raises(ValueError, match='provenance'):
        audit_rows([valid | {'source_hashes': ['c' * 64]}], day, {A}, {B})
    with pytest.raises(ValueError, match='duplicate'):
        audit_rows([valid, valid], day, {A}, {B})


def test_source_receipt_snapshot_preserves_original_bytes_and_rejects_collisions(tmp_path):
    from hashlib import sha256
    raw = b'{"date":"2026-10-21","rows":2}\n'
    native = tmp_path / 'native.json'
    native.write_bytes(raw)
    digest = sha256(raw).hexdigest()
    source = {'receipt_path': str(native), 'receipt_sha256': digest}
    snapshot = snapshot_receipt(source, tmp_path / 'snapshots')
    saved = tmp_path / 'snapshots' / f'{digest}.json'
    assert saved.read_bytes() == raw
    assert snapshot['native_receipt_path'] == str(native)
    assert snapshot_receipt(source, tmp_path / 'snapshots') == snapshot
    saved.write_bytes(b'wrong')
    with pytest.raises(ValueError, match='collision'):
        snapshot_receipt(source, tmp_path / 'snapshots')
    native.write_bytes(b'updated')
    with pytest.raises(ValueError, match='bytes differ'):
        snapshot_receipt(source, tmp_path / 'other')
