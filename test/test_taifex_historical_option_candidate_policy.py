import csv
import hashlib
import json

import pytest

from scripts.build_taifex_historical_option_candidate_policy import ADDITIONS, assemble


def test_historical_policy_includes_observed_families_and_separates_cash_currencies(tmp_path):
    base = tmp_path / 'base.json'
    base.write_text(json.dumps({'contract': 'taifex_option_session_policy_candidate_v1',
                                'research_candidate_only': True, 'products': {'TXO': {'day_minutes': [525, 825]}},
                                'reference_urls': {}, 'limitations': 'Candidate only.'}))
    inventory = tmp_path / 'inventory.csv'
    with inventory.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=['product', 'provisional_class', 'positive_volume_days'])
        writer.writeheader()
        writer.writerow({'product': 'TXO', 'provisional_class': 'index_option', 'positive_volume_days': 100})
        for product, details in ADDITIONS.items():
            writer.writerow({'product': product, 'provisional_class': details['class'], 'positive_volume_days': 1})
    summary = tmp_path / 'summary.json'
    summary.write_text(json.dumps({'contract': 'taifex_option_archive_product_day_inventory_v1',
                                   'product_inventory': str(inventory),
                                   'product_inventory_sha256': hashlib.sha256(inventory.read_bytes()).hexdigest(),
                                   'distinct_observed_products': 6}))
    policy = assemble(base, summary)
    ext = policy['historical_extension']
    assert ext['uncovered_observed_products_after_extension'] == []
    assert set(policy['products']) == {'TXO', *ADDITIONS}
    assert ext['additions']['GTO']['cash_currency'] == 'TWD'
    assert ext['additions']['MSO']['cash_currency'] == 'USD'
    assert ext['additions']['RHO']['cash_currency'] == 'CNY'
    assert policy['research_candidate_only'] is True
    inventory.write_text(inventory.read_text() + '\n')
    with pytest.raises(ValueError, match='differs from its receipt'):
        assemble(base, summary)
