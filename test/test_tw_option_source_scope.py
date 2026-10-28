import hashlib
from pathlib import Path

import polars as pl
import pytest

from stockagent.data.tw_option_source_scope import annotate_option_source_scope, load_option_source_scope


def _inventory(tmp_path: Path):
    path = tmp_path / 'inventory.csv'
    path.write_text('product,provisional_class\nTXO,index_option\nCAO,security_option_candidate\n'
                    'NYO,etf_option_candidate\nTGO,commodity_option\nRHO,currency_option\n',
                    encoding='utf-8')
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_mixed_scope_keeps_unmatched_unknown_and_duplicate_original_rows(tmp_path):
    path, digest = _inventory(tmp_path)
    scope = load_option_source_scope(path, expected_sha256=digest, scopes=['etf_options', 'stock_options'])
    original = pl.DataFrame({'product': ['CAO', 'TXO', 'NYO', 'NEW', 'CAO'],
                             'raw': ['1.00', '2', '3', '4', '1.00']})
    annotated = annotate_option_source_scope(original, scope)
    assert annotated['product'].to_list() == original['product'].to_list()
    assert annotated['raw'].to_list() == original['raw'].to_list()
    assert annotated['source_scope_class'].to_list() == [
        'security_option_candidate', 'index_option', 'etf_option_candidate', 'unclassified',
        'security_option_candidate']
    assert annotated['source_scope_match'].to_list() == [True, False, True, False, True]
    assert not {'executable', 'available_at', 'currency'} & set(annotated.columns)


def test_all_options_preserves_new_codes_and_scope_fingerprint_is_canonical(tmp_path):
    path, digest = _inventory(tmp_path)
    a = load_option_source_scope(path, expected_sha256=digest, scopes=['all_options', 'stock_options'])
    b = load_option_source_scope(path, expected_sha256=digest, scopes=['stock_options', 'all_options', 'stock_options'])
    assert a.fingerprint == b.fingerprint
    annotated = annotate_option_source_scope(pl.DataFrame({'product': ['NEW', 'RHO']}), a)
    assert annotated['source_scope_class'].to_list() == ['unclassified', 'currency_option']
    assert annotated['source_scope_match'].to_list() == [True, True]


@pytest.mark.parametrize('scopes', [[], ['unknown'], 'index_options'])
def test_requires_explicit_known_scopes(tmp_path, scopes):
    path, digest = _inventory(tmp_path)
    with pytest.raises(ValueError):
        load_option_source_scope(path, expected_sha256=digest, scopes=scopes)


def test_rejects_inventory_mutation_and_duplicate_product(tmp_path):
    path, digest = _inventory(tmp_path)
    path.write_text(path.read_text() + 'TXO,index_option\n')
    with pytest.raises(ValueError, match='digest'):
        load_option_source_scope(path, expected_sha256=digest, scopes=['index_options'])
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match='duplicate'):
        load_option_source_scope(path, expected_sha256=digest, scopes=['index_options'])
