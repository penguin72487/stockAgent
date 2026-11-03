"""Clock reuse must reject every financial delta except the audited expiry field."""
import hashlib
import json

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.preflight_tw_futures_prior_exchange_clock import (
    validate_checkpoint_rebuild_policy, validate_rule_delta_for_clock_reuse,
)


def _setup(tmp_path, *, changed_open=False, duplicate=False):
    old = tmp_path / 'old.parquet'
    new = tmp_path / 'new.parquet'
    readback = tmp_path / 'readback.json'
    original = pl.DataFrame({'date': ['2020-01-02', '2020-01-03'],
                             'physical_contract': ['a', 'a' if not duplicate else 'b'],
                             'terminal_contract_value_twd': [101.5, 102.0],
                             'opening_contract_value_twd': [100.0, 101.0]})
    if duplicate:
        original = original.with_columns(pl.lit('2020-01-02').alias('date'),
                                          pl.lit('a').alias('physical_contract'))
    changed = original.with_columns(
        pl.when(pl.int_range(pl.len()) == 0).then(101.0)
        .otherwise(pl.col('terminal_contract_value_twd')).alias('terminal_contract_value_twd'))
    if changed_open:
        changed = changed.with_columns((pl.col('opening_contract_value_twd') + 1)
                                       .alias('opening_contract_value_twd'))
    original.write_parquet(old)
    changed.reverse().write_parquet(new)
    proof = {'contract': 'index_terminal_rounding_v22_selected_independent_readback_v1',
             'candidate': '/candidate', 'old_candidate_identity': {'rules': sha256_file(old)},
             'candidate_identity': {'rules': sha256_file(new)},
             'selected_daily_values_equal': True, 'selected_dependency_bytes_equal': True,
             'selected_rule_changed_columns': {'terminal_contract_value_twd': 1},
             'financial_release_accepted': False, 'training_admitted': False}
    readback.write_text(json.dumps(proof))
    publication = {'contract': 'terminal_rounding_v22_research_publication_v1',
                   'candidate': '/candidate', 'selected_readback_sha256': sha256_file(readback),
                   'research_only': True, 'training_started_for_candidate': False}
    return old, new, readback, publication


def test_matching_research_expiry_delta_rebinds_after_column_comparison(tmp_path):
    old, new, readback, publication = _setup(tmp_path)
    result = validate_rule_delta_for_clock_reuse(old_rules=old, new_rules=new,
                                                 publication=publication, readback=readback)
    assert result['contract'] == 'audited_expiry_only_rule_delta_v1'
    assert result['changed_columns'] == {'terminal_contract_value_twd': 1}


def test_changed_opening_value_fails_even_with_expiry_receipt(tmp_path):
    old, new, readback, publication = _setup(tmp_path, changed_open=True)
    with pytest.raises(ValueError, match='more than the audited expiry'):
        validate_rule_delta_for_clock_reuse(old_rules=old, new_rules=new,
                                            publication=publication, readback=readback)


def test_changed_rules_without_pinned_publication_are_not_admitted(tmp_path):
    old, new, readback, publication = _setup(tmp_path)
    publication['selected_readback_sha256'] = 'not the receipt'
    with pytest.raises(ValueError, match='matching audited'):
        validate_rule_delta_for_clock_reuse(old_rules=old, new_rules=new,
                                            publication=publication, readback=readback)


def test_duplicate_contract_day_is_not_admitted(tmp_path):
    old, new, readback, publication = _setup(tmp_path, duplicate=True)
    with pytest.raises(ValueError, match='ambiguous'):
        validate_rule_delta_for_clock_reuse(old_rules=old, new_rules=new,
                                            publication=publication, readback=readback)


def test_identical_rules_keep_the_existing_strict_path(tmp_path):
    old, _, _, _ = _setup(tmp_path)
    result = validate_rule_delta_for_clock_reuse(old_rules=old, new_rules=old,
                                                 publication={}, readback=None)
    assert result == {'contract': 'identical_financial_rule_bytes_v1', 'changed_columns': {}}


def test_changed_clock_inputs_never_reuse_prior_checkpoint(tmp_path):
    identity = {'source_frame_sha256': 'old', 'asset_scope': 'all_twd', 'entry_windows': [1, 300]}
    checkpoint = tmp_path / hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    document = {'contract': 'verified_bulk_window_checkpoint_v1', 'identity': identity}
    kwargs = dict(checkpoint=checkpoint, document=document,
                  old_source_hash='new', new_source_hash='new')
    with pytest.raises(ValueError, match='cannot be reused'):
        validate_checkpoint_rebuild_policy(**kwargs, rebuild_from_originals=False)
    assert validate_checkpoint_rebuild_policy(**kwargs, rebuild_from_originals=True) is False
    with pytest.raises(ValueError, match='scope is invalid'):
        validate_checkpoint_rebuild_policy(**{**kwargs, 'new_source_hash': 'different'},
                                             rebuild_from_originals=True)
