from pathlib import Path

from scripts.build_tw_futures_partial_clock_experiment_spec import paired_spec


def test_partial_variants_pair_each_parent_without_changing_training_settings():
    names = [f'clock{clock}__{window}__{policy}' for clock in ('0845', '0900')
             for window, policy in (('1m', 'previous_session'), ('300m', 'previous_session'),
                                    ('300m', 'single_contract_probe'), ('300m', 'single_contract_probe_p100'))]
    spec = {'expected_fold_count': 14, 'runtime': {'parallel_jobs': 1}, 'matrix': {
        'fixed_overrides': {'training': {'epochs': 10000, 'early_stopping_no_improve_ratio': .1}},
        'dimensions': [{'name': 'all_twd', 'variants': [
            {'name': name, 'overrides': {'trading': {'max_volume_participation': 1.}}}
            for name in names]}]}}
    result = paired_spec(spec, partial_clock_root=Path('/data/partial'), output_root=Path('/out'))
    original = spec['matrix']['dimensions'][0]['variants']
    pairs = result['matrix']['dimensions'][0]['variants']
    assert len(pairs) == 16
    assert result['matrix']['fixed_overrides'] == spec['matrix']['fixed_overrides']
    assert result['runtime'] == spec['runtime']
    assert result['expected_fold_count'] == 14
    for variant, strict, partial in zip(original, pairs[::2], pairs[1::2]):
        assert strict['name'] == 'strict__' + variant['name']
        assert strict['overrides'] == variant['overrides']
        assert partial['name'] == 'partial__' + variant['name']
        tr = partial['overrides']['trading']
        clock = variant['name'].split('__')[0][5:]
        assert tr['tw_futures_portfolio_entry_clock_path'].endswith(f'/{clock}/entries.parquet')
        assert tr['tw_futures_portfolio_opening_reference_path'].endswith(f'/{clock}/opening_reference.parquet')
        expected = dict(variant['overrides'].get('trading', {}))
        expected.pop('tw_futures_portfolio_entry_clock_path', None)
        expected.pop('tw_futures_portfolio_opening_reference_path', None)
        assert {k: v for k, v in tr.items() if k not in {
            'tw_futures_portfolio_entry_clock_path', 'tw_futures_portfolio_opening_reference_path'}} == expected
