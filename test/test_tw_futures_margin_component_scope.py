from datetime import date

import polars as pl
import pytest


@pytest.mark.parametrize('changed', ['source_manifest_sha256', 'end', 'slots', 'products', 'calculation'])
def test_parent_accounting_reselection_refuses_financial_input_changes(tmp_path, changed):
    import json
    from scripts.prepare_tw_futures_margin_training import reuse_parent_accounting
    parent = tmp_path / 'parent'
    parent.mkdir()
    inputs = dict(source_manifest_sha256='a' * 64, end='2026-09-24', slots=2816,
                  products=['DR1'], calculation={'identity': 'verified'})
    (parent / 'accounting_checkpoint.json').write_text(json.dumps(dict(inputs=inputs, outputs={})))
    requested = dict(inputs, context_only_prefix={'contract': 'new_order_policy'})
    requested[changed] = 'incompatible'
    with pytest.raises(ValueError, match='source, calculation'):
        reuse_parent_accounting(parent, tmp_path / 'new', requested, {})


def test_parent_accounting_reuses_original_rows_and_rejects_corrupt_payload(tmp_path):
    import json
    from downloader.artifact_io import sha256_file
    from scripts.prepare_tw_futures_margin_training import reuse_parent_accounting
    parent, output = tmp_path / 'parent', tmp_path / 'new'
    parent.mkdir()
    original = pl.DataFrame(dict(product=['DR1', 'HF1'], previous_volume=[None, 0.]))
    original.write_parquet(parent / 'frame.parquet')
    digest = sha256_file(parent / 'frame.parquet')
    inputs = dict(source_manifest_sha256='a' * 64, end='2026-09-24', slots=2816,
        products=['DR1', 'HF1'], calculation={'identity': 'verified'},
        context_only_prefix={'contract': 'old_whole_contract_bound'})
    (parent / 'accounting_checkpoint.json').write_text(json.dumps(dict(
        inputs=inputs, outputs={'frame.parquet': digest})))
    requested = dict(inputs, context_only_prefix={'contract': 'broader_generic_entry_proof'})
    frames = reuse_parent_accounting(parent, output, requested, {'frame': 'frame.parquet'})
    assert frames['frame'].equals(original)
    assert sha256_file(output / 'frame.parquet') == digest
    assert json.loads((parent / 'accounting_checkpoint.json').read_text())['inputs'] == inputs
    assert json.loads((output / 'accounting_checkpoint.json').read_text())['inputs'] == requested
    # Source and immutable local evidence have separate inodes: later producer
    # corruption cannot mutate the already bound original calculation.
    (parent / 'frame.parquet').write_bytes(b'corrupt')
    assert sha256_file(output / 'frame.parquet') == digest
    with pytest.raises(ValueError, match='cache SHA/path'):
        reuse_parent_accounting(parent, tmp_path / 'another', requested, {'frame': 'frame.parquet'})


def test_valuation_scope_closes_families_and_multi_hop_corporate_transfers(monkeypatch):
    import scripts.prepare_tw_futures_margin_training as builder
    import stockagent.data.tw_futures_margin_preparation as preparation
    monkeypatch.setattr(builder, 'equity_contract_families', lambda _: pl.DataFrame(dict(
        product=['A', 'A1', 'B', 'B1', 'C', 'Z'], standard_product=['A', 'A', 'B', 'B', 'C', 'Z'])))
    edges = pl.DataFrame(dict(product=['A1', 'B1'], corporate_transfer_target=['B', 'C']))
    monkeypatch.setattr(preparation, 'corporate_identity_boundaries', lambda _: (None, edges))
    universe = pl.DataFrame(dict(product=['A', 'A1', 'B', 'B1', 'C', 'Z']))
    assert builder._valuation_affected_scope(universe, None, {'products': ['A1']}) == ['A', 'A1', 'B', 'B1', 'C']
    with pytest.raises(ValueError, match='exact parent universe'):
        builder._valuation_affected_scope(universe, None, {'products': ['missing']})


def test_same_valuation_parent_reselects_capacity_without_changing_values(tmp_path):
    import json
    from downloader.artifact_io import sha256_file
    from scripts.prepare_tw_futures_margin_training import reuse_parent_with_valuation
    parent, output = tmp_path / 'parent', tmp_path / 'new'
    parent.mkdir()
    original = pl.DataFrame(dict(product=['DR1', 'HF1'], value=[37.5, None],
        valuation_research_applied=[True, False]))
    rules = original.head(1)
    original.write_parquet(parent / 'frame.parquet')
    rules.write_parquet(parent / 'rules.parquet')
    names = {'frame': 'frame.parquet', 'rules': 'rules.parquet'}
    hashes = {name: sha256_file(parent / name) for name in names.values()}
    inputs = dict(source_manifest_sha256='a' * 64, end='2026-09-24', slots=2816,
        products=['DR1', 'HF1'], calculation={'identity': 'verified'},
        integrated_source_inputs=dict(rule_delta_manifest_sha256='b' * 64,
            valuation_research_policy_sha256='c' * 64,
            valuation_research_implementation_sha256='d' * 64),
        context_only_prefix={'contract': 'conservative'})
    (parent / 'accounting_checkpoint.json').write_text(json.dumps(dict(inputs=inputs, outputs=hashes)))
    requested = dict(inputs, context_only_prefix={'contract': 'whole_contract_bound',
        'maximum_volume_participation': .5})
    logged = []
    def run(destination, requested_inputs):
        return reuse_parent_with_valuation(parent, destination, requested_inputs, names,
            source=None, universe=None, rule_delta=None, information_reviews=(), policy={},
            log=lambda stage, **values: logged.append((stage, values)))
    result = run(output, requested)
    assert result['frame'].equals(original) and result['rules'].equals(rules)
    assert all(sha256_file(output / name) == digest for name, digest in hashes.items())
    assert not (output / 'valuation_recompile').exists()
    assert logged == [('verified_unchanged_valuation_parent_reselected',
        {'rows': 1, 'financial_recompilations': 0})]
    assert json.loads((parent / 'accounting_checkpoint.json').read_text())['inputs'] == inputs
    with pytest.raises(ValueError, match='source, calculation'):
        run(tmp_path / 'changed-finance', dict(requested, end='2026-09-25'))
    (parent / 'frame.parquet').write_bytes(b'corrupt original')
    with pytest.raises(ValueError, match='cache SHA/path'):
        run(tmp_path / 'corrupt', requested)


def test_strict_parent_rows_gain_explicit_false_research_flags_without_changing_finances():
    from scripts.prepare_tw_futures_margin_training import _strict_parent_valuation_disclosure
    from stockagent.data.tw_futures_valuation_research import VALUATION_RESEARCH_COLUMNS
    original = pl.DataFrame(dict(product=['AA', 'BB'], price=[12.5, None], fee=[3., 7.]))
    own = original.with_columns(*[pl.lit(False if dtype == pl.Boolean else None, dtype=dtype).alias(name)
        for name, dtype in VALUATION_RESEARCH_COLUMNS.items()])
    result = _strict_parent_valuation_disclosure(original, own)
    assert result.select(original.columns).equals(original)
    for name,dtype in VALUATION_RESEARCH_COLUMNS.items():
        assert result.schema[name] == dtype
        assert result[name].to_list() == ([False,False] if dtype == pl.Boolean else [None,None])
    with pytest.raises(ValueError, match='strict parent'):
        _strict_parent_valuation_disclosure(own, own)
    with pytest.raises(ValueError, match='complete new schema'):
        _strict_parent_valuation_disclosure(original, own.select('valuation_research_applied'))


@pytest.mark.parametrize('direction', ['none','incoming','outgoing'])
def test_publication_scope_keeps_whole_financial_components(direction):
    from scripts.prepare_tw_futures_margin_training import _publication_asset_scope
    day = date(2020,1,2)
    frame = pl.DataFrame(dict(date=[day,day], product=['AA','BB'],
        physical_contract=['AA:202001','BB:202001'], asset_class=['stock_future','interest_rate_future']))
    rules = frame.select('date','product','physical_contract').with_columns(
        pl.Series('carry_from_physical_contract',
            ['BB:202001',''] if direction == 'incoming' else ['', 'AA:202001'] if direction == 'outgoing' else ['','']),
        pl.lit(day).alias('carry_from_date'), pl.Series('value',[20.,30.]))
    flags = frame.select('date','product','physical_contract')
    if direction != 'none':
        with pytest.raises(ValueError,match='cuts an inventory transfer'):
            _publication_asset_scope(frame,rules,flags,'twd_equity')
        return
    projected = _publication_asset_scope(frame,rules,flags,'twd_equity')
    assert all(data['product'].to_list() == ['AA'] for data in projected)
    assert projected[1].equals(rules.head(1))
    assert all(a is b for a,b in zip(_publication_asset_scope(frame,rules,flags,None),(frame,rules,flags)))


def test_all_twd_publication_keeps_every_asset_class_and_cross_class_carry():
    from scripts.prepare_tw_futures_margin_training import _publication_asset_scope
    classes=['stock_future','etf_future','index_future','commodity_future','interest_rate_future']
    day=date(2020,1,2)
    frame=pl.DataFrame(dict(date=[day]*5,product=list('ABCDE'),
        physical_contract=[x+':202001' for x in 'ABCDE'],asset_class=classes))
    rules=frame.select('date','product','physical_contract').with_columns(
        pl.Series('carry_from_physical_contract',['E:202001','','','','']),
        pl.lit(day).alias('carry_from_date'),pl.Series('value',[1.,2.,3.,4.,5.]))
    flags=frame.select('date','product','physical_contract')
    projected=_publication_asset_scope(frame,rules,flags,'all_twd')
    assert all(a.equals(b) for a,b in zip(projected,(frame,rules,flags)))


def test_affected_valuation_composes_unchanged_finances_and_reassigns_slots(tmp_path, monkeypatch):
    import json
    from downloader.artifact_io import sha256_file
    import scripts.prepare_tw_futures_margin_training as builder
    parent, output = tmp_path / 'parent', tmp_path / 'new'
    parent.mkdir()
    output.mkdir()
    days = [date(2020, 1, 2), date(2020, 1, 3)]
    frame = pl.DataFrame([dict(date=day, product=product, contract='202001',
        physical_instance='202001#generation=1', physical_contract=product + ':202001',
        symbol='TAIFEX_SLOT_000' + str(i), portfolio_slot=i,
        valuation_price=100. if product == 'A' else None)
        for i, product in enumerate(['A', 'B'], 1) for day in days])
    rules = frame.select('date', 'product', 'physical_contract', 'valuation_price')
    flags = frame.select('date', 'product', 'physical_contract').with_columns(pl.lit(False).alias('has_blocker'))
    dependencies = frame.select('date', 'product', 'physical_contract', 'symbol')
    names = dict(frame='frame.parquet', rules='compiled_rules.parquet',
                 flags='contract_day_blockers.parquet', dependencies='execution_dependencies.parquet')
    originals = dict(frame=frame, rules=rules, flags=flags, dependencies=dependencies)
    for key, name in names.items():
        originals[key].write_parquet(parent / name)
    inputs = dict(source_manifest_sha256='a' * 64, end='2020-01-03', slots=4,
        products=['A', 'B'], calculation={'identity': 'verified'},
        integrated_source_inputs={'rule_delta_manifest_sha256': 'b' * 64})
    original_hashes = {name: sha256_file(parent / name) for name in names.values()}
    (parent / 'accounting_checkpoint.json').write_text(json.dumps(dict(inputs=inputs, outputs=original_hashes)))
    requested = dict(inputs, context_only_prefix={'contract': 'broader_generic'},
        integrated_source_inputs=dict(inputs['integrated_source_inputs'],
            valuation_research_policy_sha256='c' * 64,
            valuation_research_implementation_sha256='d' * 64))
    monkeypatch.setattr(builder, 'read_bound_output', lambda _: (pl.DataFrame(), None))
    monkeypatch.setattr(builder, '_valuation_affected_scope', lambda *args: ['A'])
    calls = []

    def compile_affected(source, universe, **options):
        calls.append(options)
        own = {key: data.filter(pl.col('product') == 'A') for key, data in originals.items()}
        own['frame'] = own['frame'].with_columns(pl.lit(123.).alias('valuation_price'))
        own['rules'] = own['rules'].with_columns(pl.lit(123.).alias('valuation_price'))
        return tuple(own[key] for key in names)

    monkeypatch.setattr(builder, '_compile_accounting', compile_affected)
    result = builder.reuse_parent_with_valuation(parent, output, requested, names,
        source=tmp_path / 'source', universe=pl.DataFrame(dict(product=['A', 'B'])),
        rule_delta=tmp_path / 'rules', information_reviews=[], policy={'products': ['A']},
        log=lambda *args, **kwargs: None)
    assert calls[0]['scope_products'] == ['A']
    assert set(calls[0]['portfolio_lifetimes']['product']) == {'A', 'B'}
    for key, data in result.items():
        retained = data.filter(pl.col('product') == 'B')
        columns = [name for name in retained.columns if name not in ('symbol', 'portfolio_slot')]
        assert retained.select(columns).equals(originals[key].filter(pl.col('product') == 'B').select(columns))
    assert result['rules'].filter(pl.col('product') == 'A')['valuation_price'].to_list() == [123., 123.]
    assert not result['frame'].select('date', 'symbol').is_duplicated().any()
    assert {name: sha256_file(parent / name) for name in names.values()} == original_hashes
    checkpoint = json.loads((output / 'accounting_checkpoint.json').read_text())
    assert checkpoint['inputs'] == requested
    assert all(checkpoint['outputs'][name] == sha256_file(output / name) for name in names.values())
    receipt = json.loads((output / 'parent_valuation_reuse.json').read_text())
    assert receipt['full_financial_rebuilds'] == 0
    assert receipt['unaffected_reused_rows']['rules'] == 2


@pytest.mark.parametrize('change', ['rule_delta_manifest_sha256', 'valuation_research_policy_sha256'])
def test_valuation_reuse_rejects_changed_rules_and_research_parent(tmp_path, change):
    import json
    from scripts.prepare_tw_futures_margin_training import _verified_parent_accounting
    parent = tmp_path / 'parent'
    parent.mkdir()
    base = dict(source_manifest_sha256='a' * 64, products=['A'], slots=4, end='2020-01-03',
        calculation={'identity': 'verified'}, integrated_source_inputs={'rule_delta_manifest_sha256': 'b' * 64})
    requested = dict(base, integrated_source_inputs=dict(base['integrated_source_inputs'],
        valuation_research_policy_sha256='c' * 64, valuation_research_implementation_sha256='d' * 64))
    if change == 'rule_delta_manifest_sha256':
        requested['integrated_source_inputs'][change] = 'changed'
        message = 'changes source'
    else:
        base['integrated_source_inputs'][change] = 'old_research'
        message = 'strict parent'
    (parent / 'accounting_checkpoint.json').write_text(json.dumps(dict(inputs=base, outputs={})))
    with pytest.raises(ValueError, match=message):
        _verified_parent_accounting(parent, requested, {}, adding_valuation=True)

from stockagent.data.tw_futures_margin_release import select_complete_margin_components


def fixture():
    dates = [date(2020, 1, d) for d in (2, 3, 6, 7)]
    rows, rules, flags = [], [], []
    for identity, offset, count in [("A", 0, 2), ("B", 2, 2), ("C", 0, 4)]:
        for j in range(count):
            i = offset + j
            final = identity != "A" and i == 3
            rows.append(dict(date=dates[i], physical_contract=identity, product=identity,
                next_market_date=dates[i+1] if i < 3 else None, cash_settlement=final,
                lifetime_status="corporate_transfer_candidate" if identity == "A" else "official_final"))
            origin = ("A" if identity == "B" and j == 0 else identity) if (j or identity == "B") else ""
            rules.append(dict(date=dates[i], physical_contract=identity,
                terminal_event="cash_settlement" if final else "mark_only",
                carry_from_date=dates[i-1] if origin else None,
                carry_from_physical_contract=origin))
            flags.append(dict(date=dates[i], physical_contract=identity,
                is_warmup=False, has_blocker=False))
    return pl.DataFrame(rows), pl.DataFrame(rules), pl.DataFrame(flags)


@pytest.mark.parametrize("failed", ["A", "B"])
def test_one_bad_day_removes_predecessor_and_successor_not_only_the_bad_row(failed):
    frame, rules, flags = fixture()
    flags = flags.with_columns((pl.col("physical_contract") == failed).alias("has_blocker"))
    selected, chosen, scope = select_complete_margin_components(frame, rules, flags,
        start=date(2020, 1, 1), end=date(2020, 1, 7))
    assert set(selected["physical_contract"]) == {"C"}
    assert set(chosen["physical_contract"]) == {"C"}
    assert scope.filter(pl.col("physical_contract").is_in(["A", "B"]))["selected"].to_list() == [False, False]


def test_complete_conversion_keeps_both_owners_and_exact_source_rows():
    frame, rules, flags = fixture()
    selected, chosen, scope = select_complete_margin_components(frame, rules, flags,
        start=date(2020, 1, 1), end=date(2020, 1, 7))
    assert selected.sort("date", "physical_contract").equals(frame.sort("date", "physical_contract"))
    assert chosen.sort("date", "physical_contract").equals(rules.sort("date", "physical_contract"))
    assert scope["selected"].all()


def test_endpoint_is_mark_only_without_synthetic_cash_event():
    frame, rules, flags = fixture()
    selected, chosen, _ = select_complete_margin_components(frame, rules, flags,
        start=date(2020, 1, 1), end=date(2020, 1, 6))
    tail = selected.filter(pl.col("date") == date(2020, 1, 6))
    assert tail["next_market_date"].null_count() == tail.height
    assert not tail["cash_settlement"].any()
    assert chosen.filter(pl.col("date") == date(2020, 1, 6))["terminal_event"].eq("mark_only").all()


def test_missing_diagnostic_is_not_silently_treated_as_complete():
    frame, rules, flags = fixture()
    with pytest.raises(ValueError, match="diagnostics"):
        select_complete_margin_components(frame, rules, flags.head(1),
            start=date(2020, 1, 1), end=date(2020, 1, 7))


def test_context_only_product_is_not_advertised_as_a_trainable_account():
    frame, rules, flags = fixture()
    flags = flags.with_columns((pl.col("physical_contract") == "C").alias("is_warmup"))
    rules = rules.filter(pl.col("physical_contract") != "C")
    selected, _, scope = select_complete_margin_components(frame, rules, flags,
        start=date(2020, 1, 1), end=date(2020, 1, 7))
    assert set(selected["product"]) == {"A", "B"}
    assert not scope.filter(pl.col("product") == "C")["selected"].any()


def test_early_cpf_gap_excludes_whole_lifetime_but_keeps_verified_later_cpf():
    frame, rules, flags = fixture()
    frame = frame.with_columns(pl.lit('CPF').alias('product'))
    flags = flags.with_columns((pl.col('physical_contract') == 'A').alias('has_blocker'))
    selected, chosen, scope = select_complete_margin_components(frame,rules,flags,
        start=date(2020,1,1),end=date(2020,1,7))
    # The failed early owner and its transferred successor stay together;
    # a separate complete later month of the SAME product remains admitted.
    assert set(selected['physical_contract']) == {'C'}
    assert set(chosen['physical_contract']) == {'C'}
    assert set(selected['product']) == {'CPF'}
    assert not scope.filter(pl.col('physical_contract').is_in(['A','B']))['selected'].any()


def test_empty_context_keeps_all_predecessors_without_admitting_unrelated_early_cpf():
    from scripts.prepare_tw_futures_margin_training import _selected_empty_context_keys
    day=date(2020,1,2)
    selected=pl.DataFrame([dict(date=day,physical_contract='CURRENT')])
    suppressed=pl.DataFrame([dict(date=day,physical_contract='CURRENT',carry_from_physical_contract='PREVIOUS')])
    empty=pl.DataFrame([dict(date=day,physical_contract=p,carry_from_physical_contract=origin)
        for p,origin in [('PREVIOUS','GRANDPARENT'),('GRANDPARENT','ROOT'),('ROOT',''),('EARLY_CPF','')]])
    keys=_selected_empty_context_keys(selected,empty,suppressed)
    assert set(keys['physical_contract'])=={'PREVIOUS','GRANDPARENT','ROOT'}
