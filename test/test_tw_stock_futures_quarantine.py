"""A missing physical contract cannot erase healthy peers or their session."""
from copy import deepcopy
from datetime import date
import json

import numpy as np
import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_stock_futures_history import HISTORY_DATASET, HISTORY_SOURCE, MINUTE_SCHEMA
from stockagent.data.tw_stock_futures_minute import load_futures_minute_tape, validate_futures_minute_data
from stockagent.data.tw_stock_futures_quarantine import (
    CONTRACT_DAY_QUARANTINE_POLICY, CONTRACT_DAY_QUARANTINE_VERSION, normalize_contract_days,
)


def fixture(tmp_path):
    days = [date(2021, 6, 21), date(2021, 6, 22)]
    contracts = [('LVF:202107', '5871', 0), ('LVM:202107', '5871', 1), ('CDF:202107', '2330', 0)]
    keys, coverage, bars = [], [], []
    for day in days:
        for physical, symbol, slot in contracts:
            missing = day == days[0] and physical == 'LVF:202107'
            keys.append(dict(date=day, physical_contract=physical, underlying_symbol=symbol,
                             candidate_slot=slot, contract_multiplier=100. if slot else 2000.))
            coverage.append(dict(date=day, physical_contract=physical,
                                 status='source_empty_unresolved' if missing else 'minute_verified',
                                 source_file_sha256='' if missing else 'a' * 64))
            if not missing:
                for minute, price in [(526, 100.), (810, 110.)]:
                    bars.append(dict(date=day, physical_contract=physical, minute=minute, vwap=price,
                                     high=price, low=price, close=price, volume=20., source_file_sha256='a' * 64))
    path = tmp_path/'minutes.parquet'
    atomic_write_parquet(path, pl.DataFrame(bars, schema=MINUTE_SCHEMA))
    atomic_write_parquet(tmp_path/'coverage.parquet', pl.DataFrame(coverage))
    q = [dict(date=str(days[0]), physical_contract='LVF:202107')]
    manifest = dict(dataset=HISTORY_DATASET, source_kind=HISTORY_SOURCE, contract_version=2,
                    status='complete_with_quarantine', source_daily_sha256='daily', daily_proxy_before='2020-01-01',
                    requested_dates=list(map(str, days)), covered_dates=[str(days[1])], usable_dates=list(map(str, days)),
                    quarantined_dates=[], quarantined_contract_days=q,
                    contract_day_quarantine_version=CONTRACT_DAY_QUARANTINE_VERSION,
                    contract_day_quarantine_policy=CONTRACT_DAY_QUARANTINE_POLICY,
                    outputs={k: dict(file=f'{k}.parquet', sha256=sha256_file(tmp_path/f'{k}.parquet'))
                             for k in ('minutes', 'coverage')})
    atomic_write_json(tmp_path/'manifest.json', manifest)
    return path, pl.DataFrame(keys), manifest


def test_exact_slot_exclusion_preserves_peers_calendar_and_next_day(tmp_path):
    path, keys, manifest = fixture(tmp_path)
    kwargs = dict(daily_sha256='daily', fee=40., participation=.5, daily_proxy_before='2020-01-01')
    days = np.array(manifest['requested_dates'], dtype='datetime64[D]')
    tape, _ = load_futures_minute_tape(path, keys, days, ('5871', '2330'),
                                     quarantine_contract_days=manifest['quarantined_contract_days'], **kwargs)
    assert tape.shape[:3] == (2, 2, 2)
    assert not tape[0, 0, 0].any()
    assert tape[0, 0, 1, 0] == 100.  # Healthy mini retains slot 1.
    assert tape[1, 0, 0, 0] == 2000.  # Same contract is not excluded the next day.
    np.testing.assert_array_equal(tape[0, 0, 1], tape[1, 0, 1])
    np.testing.assert_array_equal(tape[0, 1], tape[1, 1])
    # With no alternate for the affected stock, only that stock's action closes.
    single = keys.filter(pl.col('candidate_slot') == 0)
    x, _ = load_futures_minute_tape(path, single, days, ('5871', '2330'),
                                  quarantine_contract_days=manifest['quarantined_contract_days'], **kwargs)
    np.testing.assert_array_equal((x[..., 0] > 0).any(axis=-1), [[False, True], [True, True]])
    # Retrospective exclusion never rewrites the raw unresolved observation.
    assert pl.read_parquet(tmp_path/'coverage.parquet').filter(pl.col('status') == 'source_empty_unresolved').height == 1


@pytest.mark.parametrize('damage', ['unconfigured', 'wrong_key', 'unrelated_gap', 'false_usable', 'false_covered', 'wrong_version'])
def test_quarantine_remains_fail_closed(tmp_path, damage):
    path, _, manifest = fixture(tmp_path)
    q = deepcopy(manifest['quarantined_contract_days'])
    if damage == 'unconfigured':
        q = []
    elif damage == 'wrong_key':
        q[0]['physical_contract'] = 'CDF:202107'
        manifest['quarantined_contract_days'] = q
    elif damage == 'unrelated_gap':
        coverage = pl.read_parquet(tmp_path/'coverage.parquet').with_columns(
            pl.when(pl.col('physical_contract') == 'CDF:202107').then(pl.lit('missing_receipt'))
            .otherwise(pl.col('status')).alias('status'))
        atomic_write_parquet(tmp_path/'coverage.parquet', coverage)
        manifest['outputs']['coverage']['sha256'] = sha256_file(tmp_path/'coverage.parquet')
        manifest['covered_dates'] = []
        manifest['usable_dates'] = []
    elif damage == 'false_usable':
        manifest['usable_dates'] = []
    elif damage == 'false_covered':
        manifest['covered_dates'] = manifest['requested_dates']
    else:
        manifest['contract_day_quarantine_version'] = 99
    atomic_write_json(tmp_path/'manifest.json', manifest)
    with pytest.raises(ValueError):
        validate_futures_minute_data(path, daily_sha256='daily', daily_proxy_before='2020-01-01',
                                    quarantine_contract_days=q)


@pytest.mark.parametrize('entries', [None, 'LVF', ['2021-06-21'],
    [dict(date='2021-06-21', physical_contract='LVF:*')],
    [dict(date='2021-06-21', physical_contract='LVF:202113')],
    [dict(date='2021-06-21', physical_contract='LVF:202107', extra=True)],
    [dict(date='2021-06-21', physical_contract='LVF:202107')] * 2])
def test_quarantine_rejects_ambiguous_or_duplicate_scope(entries):
    with pytest.raises(ValueError):
        normalize_contract_days(entries)
