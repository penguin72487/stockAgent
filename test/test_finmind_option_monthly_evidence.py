import hashlib
import json

import pytest

from downloader.finmind_option_monthly_evidence import make_pins, validate_sources


def _source(tmp_path):
    source = tmp_path / 'data_tw_index_options_daily' / '2026-01-01_2026-01-31_all.csv'
    source.parent.mkdir(parents=True)
    source.write_bytes(b'official monthly bytes')
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    identity = {'contract': 'taifex_option_all_products_daily_raw_v1',
                'url': 'https://www.taifex.com.tw/cht/3/optDataDown',
                'request': {'down_type': '1', 'queryStartDate': '2026/01/01',
                            'queryEndDate': '2026/01/31', 'commodity_id': 'all', 'commodity_id2': ''},
                'range_start': '2026-01-01', 'range_end': '2026-01-31', 'point_in_time_snapshot': False}
    source.with_suffix('.intent.json').write_text(json.dumps(identity))
    receipt = {**identity, 'path': str(source), 'bytes': source.stat().st_size, 'sha256': digest}
    source.with_suffix('.receipt.json').write_text(json.dumps(receipt))
    coverage = {'contract': 'tw_option_all_product_monthly_range_readback_v1',
                'start': '2026-01-01', 'end': '2026-01-31', 'months_verified': 1,
                'sources': [{'start': '2026-01-01', 'end': '2026-01-31', 'path': str(source),
                             'receipt': str(source.with_suffix('.receipt.json')),
                             'sha256': digest, 'bytes': source.stat().st_size}]}
    return coverage, source


def test_monthly_rechecks_pin_receipt_intent_and_original_source(tmp_path):
    coverage, source = _source(tmp_path)
    dates = ['2026-01-28', '2026-01-30']
    pins = make_pins(coverage, dates, tmp_path)
    assert len(pins) == 1
    validate_sources(coverage, dates, tmp_path, pins)
    source.write_bytes(b'official altered bytes')
    with pytest.raises(ValueError):
        validate_sources(coverage, dates, tmp_path, pins)


@pytest.mark.parametrize('defect', ['receipt', 'intent', 'out_of_scope', 'ambiguous', 'bad_pin'])
def test_monthly_rechecks_reject_changed_or_ambiguous_evidence(tmp_path, defect):
    coverage, source = _source(tmp_path)
    dates = ['2026-01-28']
    pins = make_pins(coverage, dates, tmp_path)
    if defect in {'receipt', 'intent'}:
        path = source.with_suffix(f'.{defect}.json')
        path.write_bytes(path.read_bytes() + b' ')
    elif defect == 'out_of_scope':
        dates = ['2026-02-01']
    elif defect == 'ambiguous':
        coverage['sources'].append(dict(coverage['sources'][0]))
        coverage['months_verified'] = 2
    else:
        pins[0]['start'] = '2026-01-02'
    with pytest.raises(ValueError):
        validate_sources(coverage, dates, tmp_path, pins)


def test_monthly_rechecks_reject_oversized_receipts_before_parsing(tmp_path):
    coverage, source = _source(tmp_path)
    source.with_suffix('.receipt.json').write_bytes(b' ' * 65537)
    pins = make_pins(coverage, ['2026-01-28'], tmp_path)
    with pytest.raises(ValueError, match='oversized'):
        validate_sources(coverage, ['2026-01-28'], tmp_path, pins)
