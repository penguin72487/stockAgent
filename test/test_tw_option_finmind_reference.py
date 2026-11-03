from datetime import date
import hashlib
import json

import polars as pl
import pytest

from stockagent.data.tw_option_finmind_reference import candidate_rows
from stockagent.data.tw_option_reference_candidates import reference_candidates


DAY = date(2026, 10, 21)
WINDOWS = {'TXO': (525, 825), 'TEO': (525, 825)}


def vendor(rows):
    return pl.DataFrame(rows, schema_overrides={
        'date': pl.String, 'option_id': pl.String, 'contract_date': pl.String,
        'ExercisePrice': pl.Float32, 'PutCall': pl.String, 'price': pl.Float32,
        'volume': pl.Int32,
    })


def row(timestamp, *, product='TXO', series='202610.0', strike=100.0,
        right='C', price=6.0, volume=1):
    return {'date': timestamp, 'option_id': product, 'contract_date': series,
            'ExercisePrice': strike, 'PutCall': right, 'price': price, 'volume': volume}


def test_adapter_filters_and_reports_source_quality_without_inventing_rows():
    source = vendor([
        row('2026-10-21 08:45:00'),
        row('2026-10-21 08:46:00', product='TEO', series='202610W1', right='P'),
        row('2026-10-20 13:00:00'),
        row('bad timestamp'),
        row('2026-10-21 08:47:00', product='ZZO'),
        row('2026-10-21 08:47:00', series='202610/202611'),
        row('2026-10-21 08:47:00', price=0.0),
        row('2026-10-21 08:47:00', strike=float('nan')),
        row('2026-10-21 08:47:00', right='X'),
    ])
    output, stats = candidate_rows(source, day=DAY, source_sha256='a' * 64,
                                   candidate_products=set(WINDOWS))
    assert output.height == 2
    assert output['series_raw'].to_list() == ['202610', '202610W1']
    assert output['source_sha256'].unique().to_list() == ['a' * 64]
    assert stats['source_rows'] == 9 and stats['excluded_rows'] == 7
    assert stats['invalid_or_other_day_timestamp_rows'] == 2
    assert stats['unsupported_contract_rows'] == 2
    assert stats['nonpositive_or_invalid_numeric_rows'] == 2
    assert stats['unclassified_products'] == ['ZZO']


def test_adapter_rejects_wrong_precision_schema_and_unpinned_source():
    source = vendor([row('2026-10-21 08:45:00')])
    with pytest.raises(ValueError, match='precision'):
        candidate_rows(source.with_columns(pl.col('price').cast(pl.Float64)), day=DAY,
                       source_sha256='a' * 64, candidate_products=set(WINDOWS))
    with pytest.raises(ValueError, match='identity'):
        candidate_rows(source, day=DAY, source_sha256='not-a-sha', candidate_products=set(WINDOWS))
    with pytest.raises(ValueError, match='candidate product'):
        candidate_rows(source, day=DAY, source_sha256='a' * 64, candidate_products=set())


def test_source_seconds_and_ambiguous_first_trade_flow_through_shared_reducer():
    prior, _ = candidate_rows(vendor([row('2026-10-20 13:00:00', price=5)]),
                              day=date(2026, 10, 20), source_sha256='a' * 64,
                              candidate_products=set(WINDOWS))
    current, _ = candidate_rows(vendor([
        row('2026-10-21 08:45:00', price=6),
        row('2026-10-21 08:45:00', price=7),
        row('2026-10-21 08:50:00', price=8),
    ]), day=DAY, source_sha256='b' * 64, candidate_products=set(WINDOWS))
    source = pl.concat([prior, current])
    early = reference_candidates(source, trading_date=DAY, decision_time='08:45', session_windows=WINDOWS)
    assert early.item(0, 'reference_price_decimal') == '5'
    assert early.item(0, 'reference_kind') == 'last_observed_trade_candidate'
    late = reference_candidates(source, trading_date=DAY, decision_time='09:00', session_windows=WINDOWS)
    assert late.item(0, 'reference_kind') == 'ambiguous_first_second'
    assert late.item(0, 'reference_price_decimal') is None
    assert late.item(0, 'trade_ts').isoformat() == '2026-10-21T08:45:00+08:00'


def test_two_day_materialization_requires_source_receipts_and_reports_unknown_prior(tmp_path, monkeypatch):
    from scripts import build_taifex_option_finmind_reference_window as builder

    monkeypatch.setattr(builder, 'priority_dispatch_status', lambda *args, **kwargs: {'state': 'admitted'})
    build, pin_source = builder.build, builder.pin_source

    root = tmp_path / 'complement'
    receipts = root / 'receipts' / 'TaiwanOptionTick' / 'all'
    receipts.mkdir(parents=True)
    for day, price in [('2026-10-20', 5.0), ('2026-10-21', 6.0)]:
        data = root / 'parquet' / f'{day}.parquet'
        data.parent.mkdir(parents=True, exist_ok=True)
        vendor([row(f'{day} 08:45:00', price=price)]).write_parquet(data)
        sha = hashlib.sha256(data.read_bytes()).hexdigest()
        receipt = {'dataset': 'TaiwanOptionTick', 'data_id': '', 'partition': day, 'kind': 'id_day',
                   'status': 'complete', 'storage_contract_version': 3, 'source_first_date': day,
                   'source_last_date': day, 'source_bytes_preserved': True, 'rows': 1, 'sha256': sha,
                   'parquet_path': str(data.relative_to(root)), 'parquet_size_bytes': data.stat().st_size}
        (receipts / f'{day}.json').write_text(json.dumps(receipt))
    evidence = tmp_path / 'policy_evidence.json'
    evidence.write_text('{}')
    sha = hashlib.sha256(evidence.read_bytes()).hexdigest()
    policy = tmp_path / 'policy.json'
    policy.write_text(json.dumps({
        'contract': 'taifex_option_session_policy_candidate_v1', 'research_candidate_only': True,
        'products': {'TXO': {'day_minutes': [525, 825]}},
        'inventory_comparison_pins': {'inventory_path': str(evidence), 'inventory_sha256': sha,
                                      'receipt_path': str(evidence), 'receipt_sha256': sha},
        'current_catalog_comparison_pins': {'raw_path': str(evidence), 'raw_sha256': sha,
                                            'receipt_path': str(evidence), 'receipt_sha256': sha},
    }))
    dates = [date(2026, 10, 20), DAY]
    output = tmp_path / 'result'
    manifest = build(root, policy, dates, output)
    assert manifest['training_ready'] is False
    frame = pl.read_parquet(output / 'observed_references.parquet')
    prior = frame.filter((pl.col('decision_date') == DAY) & (pl.col('decision_time') == '08:45'))
    assert prior.item(0, 'reference_price_decimal') == '5'
    assert prior.item(0, 'reference_kind') == 'last_observed_trade_candidate'
    opening = frame.filter((pl.col('decision_date') == DAY) & (pl.col('decision_time') == '09:00'))
    assert opening.item(0, 'reference_price_decimal') == '6'
    assert any('before the first' in text for text in manifest['limitations'])
    with pytest.raises(ValueError, match='consecutive'):
        build(root, policy, [date(2026, 10, 20), date(2026, 10, 22)], tmp_path / 'gap')
    damaged = root / 'parquet' / '2026-10-21.parquet'
    damaged.write_bytes(damaged.read_bytes() + b'x')
    with pytest.raises(ValueError, match='differs from receipt'):
        pin_source(root, DAY)


def test_unconfirmed_physical_capacity_defers_without_creating_output(tmp_path, monkeypatch):
    from scripts import build_taifex_option_finmind_reference_window as builder

    monkeypatch.setattr(builder, 'priority_dispatch_status',
                        lambda *args, **kwargs: {'state': 'deferred', 'reason': 'physical_backing_headroom_unconfirmed'})
    with pytest.raises(builder.CapacityNotAdmitted) as error:
        builder.capacity_check(tmp_path / 'does-not-exist')
    assert error.value.status['reason'] == 'physical_backing_headroom_unconfirmed'
    assert not (tmp_path / 'does-not-exist').exists()


def test_official_vendor_comparison_reports_differences_and_rejects_float_key_collisions():
    from scripts.audit_taifex_finmind_option_references import compare

    official, _ = candidate_rows(vendor([row('2026-10-21 08:45:00', price=6)]),
                                 day=DAY, source_sha256='a' * 64, candidate_products=set(WINDOWS))
    other, _ = candidate_rows(vendor([row('2026-10-21 08:45:00', price=7),
                                      row('2026-10-21 08:45:00', product='TEO')]),
                              day=DAY, source_sha256='b' * 64, candidate_products=set(WINDOWS))
    def at_nine(raw):
        return reference_candidates(raw, trading_date=DAY, decision_time='09:00',
                                    session_windows=WINDOWS).with_columns(
            pl.lit(DAY).cast(pl.Date).alias('decision_date'), pl.lit('09:00').alias('decision_time'))
    a, b = at_nine(official), at_nine(other)
    report = compare(a, b, [DAY])
    assert report['matched_keys'] == 1 and report['matched_value_differences'] == 1
    assert report['official_only_keys'] == 0 and report['vendor_only_keys'] == 1
    collision = pl.concat([a, a.with_columns(pl.lit('100.0000001').alias('strike_decimal'))])
    with pytest.raises(ValueError, match='collide'):
        compare(collision, b, [DAY])


def test_policy_extension_retains_existing_references_and_adds_historical_keys():
    from scripts.audit_finmind_option_reference_policy_delta import compare

    source = vendor([row('2026-10-21 08:45:00'),
                     row('2026-10-21 08:45:00', product='GTO', price=3)])
    old, extended = {'TXO': (525, 825)}, {'TXO': (525, 825), 'GTO': (525, 825)}
    result = compare(source, DAY, 'a' * 64, old, extended)
    assert result['previous_source_rows']['unclassified_products'] == ['GTO']
    assert result['extended_source_rows']['unclassified_products'] == []
    assert result['clocks']['08:45']['new_candidate_keys'] == 0
    assert result['clocks']['09:00']['new_candidate_keys_by_product'] == [{'product_raw': 'GTO', 'len': 1}]
    with pytest.raises(ValueError, match='preserve'):
        compare(source, DAY, 'a' * 64, old, {'TXO': (526, 825)})
