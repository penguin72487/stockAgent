"""Finite halt-source additions must preserve the accounting and source parent."""
from copy import deepcopy
import json
from pathlib import Path

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import prepare_tw_futures_margin_training as builder
from scripts.repair_tw_futures_margin_source_intervals import stage_loss_reduction_halt_repair
from stockagent.data import tw_futures_margin_halt_sources as sources
from test_tw_futures_halt_valuation import source_review_case


def bundle_fixture(tmp_path):
    bundle = tmp_path / 'bundle'; episode = bundle / 'episode'; episode.mkdir(parents=True)
    path, review = source_review_case(episode, wording='減資彌補虧損')
    cf_path = episode / 'corporate.parquet'
    corporate = pl.read_parquet(cf_path).with_columns(pl.lit(False).alias('has_equity_credit_fields'),
        pl.lit(0.).alias('equity_credit_long_per_contract'), pl.lit(0.).alias('equity_debit_short_per_contract'))
    corporate.write_parquet(cf_path)
    atomic_write_json(episode / 'manifest.json', dict(outputs={cf_path.name: dict(sha256=sha256_file(cf_path))}))
    review['corporate_input']['sha256'] = sha256_file(cf_path); atomic_write_json(path, review)
    source = tmp_path / 'source'; source.mkdir(); atomic_write_json(source / 'source_manifest.json', dict(identity='original'))
    pending = tmp_path / 'pending'; pending.mkdir(); effective = pending / 'corporate_event_candidates.parquet'
    corporate.write_parquet(effective)
    atomic_write_json(pending / 'manifest.json', dict(status='pending_source_bound_rule_delta',
        parent_source_manifest_sha256=sha256_file(source / 'source_manifest.json'), changed_products=['ES1'],
        sources=[], outputs={effective.name: dict(sha256=sha256_file(effective))},
        terminal_source_delta=dict(path='terminal'), information_halt_source_delta=dict(path='information_halt'),
        opening_margin_boundary_reviews=[dict(path='cpf.json')]))
    baseline = bundle / 'baseline.py'; baseline.write_text('# frozen original\n')
    atomic_write_json(bundle / 'manifest.json', dict(status='source_bound_loss_reduction_halt_extension',
        parent_source_manifest_sha256=sha256_file(source / 'source_manifest.json'),
        implementation_sha256=sha256_file(Path(sources.__file__)),
        reviews=[dict(path=str(path.relative_to(bundle)), sha256=sha256_file(path))],
        sources=[dict(path=str(p.relative_to(bundle)), sha256=sha256_file(p))
                 for p in episode.iterdir() if p.is_file() and p != path],
        accounting_library_baseline=dict(path=baseline.name, sha256=sha256_file(baseline))))
    return source, pending, bundle, corporate


def test_original_reviews_and_terminal_inputs_remain_visible(tmp_path):
    source, pending, bundle, _ = bundle_fixture(tmp_path)
    paths, episodes, identity, _ = sources.load_loss_reduction_bundle(
        source, pending / 'corporate_event_candidates.parquet', bundle)
    original = source / 'halt/old/review.json'; original.parent.mkdir(parents=True); original.write_text('{}')
    composed = sources.LossReductionInputSource(source, paths)
    assert set((composed / 'halt').glob('*/review.json')) == {original, *paths}
    assert composed / 'terminal/manifest.json' == source / 'terminal/manifest.json'
    assert episodes[0]['contract_months'] == ['201509', '201512']
    assert identity['corporate_input_sha256'] == sha256_file(pending / 'corporate_event_candidates.parquet')
    with pytest.raises(ValueError): composed / '../outside'
    with pytest.raises(ValueError): (composed / 'halt').glob('**/*')


def test_unrelated_repair_is_admitted_while_own_event_change_is_rejected(tmp_path):
    source, pending, bundle, corporate = bundle_fixture(tmp_path)
    unrelated = corporate.with_columns(pl.lit('ZZF').alias('from_product'))
    path = pending / 'corporate_event_candidates.parquet'
    pl.concat([corporate, unrelated]).write_parquet(path)
    atomic_write_json(pending / 'manifest.json', dict(outputs={path.name: dict(sha256=sha256_file(path))}))
    assert len(sources.load_loss_reduction_bundle(source, path, bundle)[1]) == 1
    for column, value in [('equity_credit_long_per_contract', 100.), ('contract_multiplier', 1601.)]:
        changed = corporate.with_columns(pl.lit(value).alias(column))
        with pytest.raises(ValueError):
            sources.load_loss_reduction_bundle(source, path, bundle, corporate_frame=changed)


def test_missing_reviewed_event_and_mutated_original_fail_closed(tmp_path):
    source, pending, bundle, corporate = bundle_fixture(tmp_path)
    path = pending / 'corporate_event_candidates.parquet'
    with pytest.raises(ValueError):
        sources.load_loss_reduction_bundle(source, path, bundle, corporate_frame=corporate.head(0))
    (bundle / 'episode/candidate.txt').write_text('unbound text')
    with pytest.raises(ValueError, match='original SHA'):
        sources.load_loss_reduction_bundle(source, path, bundle)


def test_present_but_omitted_nested_original_cannot_be_staged(tmp_path):
    source, pending, bundle, _ = bundle_fixture(tmp_path)
    file = bundle / 'manifest.json'; manifest = json.loads(file.read_text())
    manifest['sources'] = [s for s in manifest['sources'] if not s['path'].endswith('commencement.json')]
    atomic_write_json(file, manifest)
    with pytest.raises(ValueError, match='omits a referenced original'):
        stage_loss_reduction_halt_repair(source, pending, bundle, tmp_path / 'staging.json')
    assert not (pending / 'loss_reduction_halt').exists()


def test_refresh_accepts_additional_evidence_and_preserves_original_reviews(tmp_path):
    source, pending, bundle, _ = bundle_fixture(tmp_path)
    stage_loss_reduction_halt_repair(source, pending, bundle, tmp_path / 'first.json')
    original_sha = sha256_file(pending / 'loss_reduction_halt/episode/review.json')
    file = bundle / 'manifest.json'; manifest = json.loads(file.read_text())
    additional = bundle / 'extra.json'; additional.write_text('{"source_evidence":"additional"}')
    manifest['sources'].append(dict(path=additional.name, sha256=sha256_file(additional)))
    atomic_write_json(file, manifest)
    stage_loss_reduction_halt_repair(source, pending, bundle, tmp_path / 'second.json', refresh_pending=True)
    assert sha256_file(pending / 'loss_reduction_halt/episode/review.json') == original_sha
    assert (pending / 'loss_reduction_halt/extra.json').read_bytes() == additional.read_bytes()
    # A valid new baseline is still not the original accepted financial code.
    baseline = bundle / 'baseline.py'; baseline.write_text('# replaced original\n')
    manifest['accounting_library_baseline']['sha256'] = sha256_file(baseline); atomic_write_json(file, manifest)
    with pytest.raises(ValueError, match='previous review and baseline'):
        stage_loss_reduction_halt_repair(source, pending, bundle, tmp_path / 'third.json', refresh_pending=True)


def test_stage_preserves_other_inputs_and_every_financial_row(tmp_path):
    source, pending, bundle, _ = bundle_fixture(tmp_path)
    parent = json.loads((pending / 'manifest.json').read_text())
    cf_sha = sha256_file(pending / 'corporate_event_candidates.parquet')
    receipt = tmp_path / 'staging.json'
    stage_loss_reduction_halt_repair(source, pending, bundle, receipt)
    current = json.loads((pending / 'manifest.json').read_text())
    for key in ['terminal_source_delta', 'information_halt_source_delta', 'opening_margin_boundary_reviews', 'outputs']:
        assert current[key] == parent[key]
    assert sha256_file(pending / 'corporate_event_candidates.parquet') == cf_sha
    assert current['loss_reduction_halt_source_delta']['products'] == ['AAF']
    assert current['changed_products'] == ['AAF', 'ES1']
    with pytest.raises(ValueError): stage_loss_reduction_halt_repair(source, pending, bundle, receipt)


def test_wording_migration_proves_the_entire_remaining_financial_code_unchanged(tmp_path):
    module = 'stockagent/data/tw_futures_margin_preparation.py'
    current = builder._calculation_identity()
    original = (builder.ROOT / module).read_text().replace(
        "if not any(phrase in text for phrase in ('減資以彌補虧損', '減資彌補虧損')):",
        "if '減資以彌補虧損' not in text:")
    baseline = tmp_path / 'baseline.py'; baseline.write_text(original)
    previous = deepcopy(current); previous['files'][module] = sha256_file(baseline)
    proof = builder._verify_loss_reduction_loader_extension(previous, current, (baseline, sha256_file(baseline)))
    assert proof['accounting_functions_unchanged']
    for kind in ['fee_file', 'accounting_function', 'incorrect_baseline']:
        changed = deepcopy(previous)
        if kind == 'fee_file': changed['files']['stockagent/data/tw_futures_execution_terms.py'] = 'f' * 64
        elif kind == 'accounting_function': changed['functions']['_compile_accounting'] = 'f' * 64
        else: baseline.write_text(original.replace('before!=2000', 'before!=1000'))
        with pytest.raises(ValueError):
            builder._verify_loss_reduction_loader_extension(changed, current, (baseline, sha256_file(baseline)))
