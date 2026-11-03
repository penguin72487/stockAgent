import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.build_tw_option_cross_source_references import reference_map
from scripts.run_tw_option_cross_source_recent import ensure_composite, select_days, single_window
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_event_index import CONTRACT


def pin(day):
    return {'date': day, 'planned': True, 'receipt_sha256': 'a' * 64, 'parquet_sha256': 'b' * 64}


def test_selection_keeps_missing_source_and_next_file_distinct():
    plan = {'windows': [single_window(pin(d)) for d in ('2026-10-05', '2026-10-07', '2026-10-08')]}
    dates = ['2026-10-05', '2026-10-06', '2026-10-07', '2026-10-08']
    rows, _ = select_days(dates, plan)
    assert [r['state'] for r in rows] == ['no_pinned_prior_finmind_source',
                                           'finmind_source_unavailable_or_unverified',
                                           'selected', 'next_official_source_unavailable']
    assert rows[2]['prior_finmind_day'] == '2026-10-05'
    assert rows[2]['next_official_day'] == '2026-10-08'
    with pytest.raises(ValueError, match='distinct'):
        select_days(dates + [dates[-1]], plan)
    with pytest.raises(ValueError, match='repeated'):
        select_days(dates, {'windows': plan['windows'] + [plan['windows'][0]]})


def write_parent(path, day, rows, plan_path, source_sha):
    path.mkdir()
    frame = pl.DataFrame(rows).with_columns(pl.lit(date.fromisoformat(day)).cast(pl.Date).alias('source_date'))
    payload = path / 'observed_events.parquet'
    frame.write_parquet(payload)
    doc = {'contract': CONTRACT, 'training_ready': False, 'source_pins': [{'day': day, 'sha256': source_sha}],
           'plan_sha256': sha256_file(plan_path), 'policy_sha256': 'c' * 64,
           'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': frame.height}
    manifest = path / 'manifest.json'
    manifest.write_text(json.dumps(doc))
    return manifest


def observation(day, clock, price, source):
    return {'event_ts': datetime.fromisoformat(f'{day}T{clock}').replace(tzinfo=ZoneInfo('Asia/Taipei')),
            'product_raw': 'TXO', 'series_raw': '202610', 'strike_raw': '20000', 'right_raw': 'C',
            'price_raw': price, 'source_sha256': source}


def test_composition_preserves_causal_prior_and_open_and_rejects_tampering(tmp_path, monkeypatch):
    monkeypatch.setattr('scripts.run_tw_option_cross_source_recent.capacity_check', lambda path: {})
    plan_path = tmp_path / 'plan.json'
    plan_path.write_text('{}')
    plan = {'policy_sha256': 'c' * 64, 'policy_path': str(tmp_path / 'policy.json'),
            'not_planned_days': [{'date': '2026-10-20', 'status': 'observed_empty'}]}
    a, b = 'a' * 64, 'b' * 64
    first = write_parent(tmp_path / 'one', '2026-10-19',
                         [observation('2026-10-19', '13:00:00', '9', a)], plan_path, a)
    second = write_parent(tmp_path / 'two', '2026-10-21',
                          [observation('2026-10-21', '08:45:00', '10', b),
                           observation('2026-10-21', '08:46:00', '12', b)], plan_path, b)
    day = date(2026, 10, 21)
    result = ensure_composite([first, second], day, plan_path, plan, tmp_path / 'out')
    doc = json.loads(result.read_bytes())
    assert doc['known_missing_source_days'] == plan['not_planned_days']
    frame = pl.read_parquet(doc['data_path'])
    early = reference_map(reference_candidates(frame, trading_date=day, decision_time='08:45',
                                               session_windows={'TXO': (525, 825)}))
    later = reference_map(reference_candidates(frame, trading_date=day, decision_time='09:00',
                                               session_windows={'TXO': (525, 825)}))
    key = ('TXO', '202610', '20000', 'C')
    assert early[key][:3] == ('last_observed_trade_candidate', '9', '20261019130000')
    assert later[key][:3] == ('observed_session_open_candidate', '10', '20261021084500')
    assert ensure_composite([first, second], day, plan_path, plan, tmp_path / 'out') == result
    with pytest.raises(ValueError, match='future day'):
        ensure_composite([first, second], date(2026, 10, 20), plan_path, plan, tmp_path / 'future')
    (tmp_path / 'out' / 'observed_events.parquet').write_bytes(b'tampered')
    with pytest.raises(ValueError, match='digest'):
        ensure_composite([first, second], day, plan_path, plan, tmp_path / 'out')
