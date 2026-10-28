"""Node-local measurements must honor actual budgets and immutable proof."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from stockagent.remote_build import (candidate_threads, choose_measured, cpu_list,
                                    effective_limits, local_affinities, observe_node, validate_selection)


def test_container_quota_and_ancestor_memory_override_visible_host():
    groups = [{'cpu_max': 'max 100000', 'memory_max': 'max'},
              {'cpu_max': '5375999 100000', 'memory_max': str(242*1024**3), 'memory_current': str(111*1024**3)},
              {'cpu_max': '8000000 100000', 'memory_max': str(300*1024**3), 'memory_current': str(280*1024**3)}]
    limits = effective_limits(list(range(224)), 490*1024**3, groups)
    assert limits['cpu_capacity'] == pytest.approx(53.75999)
    assert limits['cpu_worker_budget'] == 53
    assert limits['memory_headroom_bytes'] == 20*1024**3
    assert limits['cpu_thread_trial_cap'] == 108
    assert candidate_threads({'limits': limits}) == [2, 4, 8, 16, 32, 53, 64, 108]


def test_affinity_and_fractional_quota_are_both_limits():
    assert effective_limits([2, 3], 100, [{'cpu_max': '9000000 100000'}])['cpu_worker_budget'] == 2
    assert effective_limits([2, 3], 100, [{'cpu_max': '150000 100000'}])['cpu_worker_budget'] == 1
    assert effective_limits([2], 100, [{'memory_max': '10', 'memory_current': '20'}])['memory_headroom_bytes'] == 0


def memory_stats(**changes):
    values = dict(file=800, shmem=200, active_file=300, inactive_file=250,
                  file_dirty=20, file_writeback=10, unevictable=40,
                  anon=5000, slab_reclaimable=9000, swapcached=8000)
    values.update(changes)
    return '\n'.join(f'{key} {value}' for key, value in values.items())


def test_opt_in_clean_file_capacity_keeps_default_and_does_not_count_shmem_or_slab():
    group = {'path': '/cg/job', 'memory_max': '1000', 'memory_current': '950',
             'memory_stat': memory_stats()}
    assert effective_limits([0], 2000, [group])['memory_headroom_bytes'] == 50
    measured = effective_limits([0], 2000, [group], memory_policy='clean_file_reclaim_v1')
    assert measured['memory_unreclaimed_headroom_bytes'] == 50
    assert measured['memory_headroom_bytes'] == 530  # 50 + min(600, 550) - 70.
    assert measured['memory_reclaim_bounds'] == [{'path': '/cg/job', 'clean_file_bytes': 480,
                                                 'unreclaimed_headroom_bytes': 50}]


@pytest.mark.parametrize('changes,expected', [
    ({'file': 250, 'shmem': 200}, 0),
    ({'file_dirty': 600}, 0),
    ({'unevictable': 600}, 0),
    ({'active_file': 0, 'inactive_file': 0}, 0),
    ({'file': 200, 'shmem': 250}, 0),
    ({'file': 350, 'shmem': 200, 'anon': 99999999, 'slab_reclaimable': 99999999}, 80),
])
def test_only_clean_filesystem_lru_is_credited(changes, expected):
    group = {'memory_max': '1000', 'memory_current': '1000', 'memory_stat': memory_stats(**changes)}
    assert effective_limits([0], 2000, [group], memory_policy='clean_file_reclaim_v1')['memory_headroom_bytes'] == expected


def test_clean_file_capacity_obeys_host_and_every_ancestor_instead_of_adding_credits():
    groups = [{'memory_max': '1000', 'memory_current': '950', 'memory_stat': memory_stats()},
              {'memory_max': '2000', 'memory_current': '1980', 'memory_stat': memory_stats(file=300, shmem=200)}]
    assert effective_limits([0], 2000, groups, memory_policy='clean_file_reclaim_v1')['memory_headroom_bytes'] == 50
    assert effective_limits([0], 25, groups, memory_policy='clean_file_reclaim_v1')['memory_headroom_bytes'] == 25
    # Counters can be sampled across a reclaim; never exceed the limit itself.
    groups[0]['memory_current'] = '10'
    assert effective_limits([0], 2000, groups[:1], memory_policy='clean_file_reclaim_v1')['memory_headroom_bytes'] == 1000


@pytest.mark.parametrize('stats', [None, '', 'file 1\nfile 2', 'file -1', 'file 1 extra',
                                   memory_stats().replace('unevictable 40', ''),
                                   memory_stats().replace('file_dirty 20', 'file_dirty NaN')])
def test_reclaim_admission_rejects_missing_invalid_or_duplicate_stats(stats):
    group = {'memory_max': '1000', 'memory_current': '950', 'memory_stat': stats}
    with pytest.raises(ValueError):
        effective_limits([0], 2000, [group], memory_policy='clean_file_reclaim_v1')
    assert effective_limits([0], 2000, [group])['memory_headroom_bytes'] == 50


def test_unknown_memory_admission_policy_is_rejected():
    with pytest.raises(ValueError, match='unknown'):
        effective_limits([0], 1000, [], memory_policy='guess_all_cache_is_free')


@pytest.mark.parametrize('group', [{'cpu_max': '100 0'}, {'cpu_max': '0 100'},
                                 {'memory_max': '10'}, {'memory_max': '10', 'memory_current': '-1'}])
def test_unknown_or_invalid_limits_cannot_be_admitted(group):
    with pytest.raises(ValueError):
        effective_limits([0], 100, [group])


@pytest.mark.parametrize('value', ['3-1', '../0', '1;touch bad', '0,,2', '-1'])
def test_affinity_parser_rejects_invalid_cpu_lists(value):
    with pytest.raises(ValueError):
        cpu_list(value)


def test_numa_candidates_use_one_thread_per_core_in_observed_affinity():
    profile = {'identity': {'topology': [
        {'cpu': 0, 'package': 0, 'core': 0}, {'cpu': 2, 'package': 0, 'core': 0},
        {'cpu': 1, 'package': 0, 'core': 1}, {'cpu': 3, 'package': 1, 'core': 0}],
        'numa_cpus': {'node0': [0, 1, 2], 'node1': [3]}}}
    assert local_affinities(profile) == [{'name': 'node0-physical', 'cpus': [0, 1]},
                                       {'name': 'node0-logical', 'cpus': [0, 1, 2]},
                                       {'name': 'node1-physical', 'cpus': [3]},
                                       {'name': 'all-physical', 'cpus': [0, 1, 3]}]


def test_nps4_socket_candidates_fit_quota_when_single_numa_domain_does_not():
    topology = [{'cpu': cpu, 'package': (cpu % 112) // 56, 'core': cpu % 56}
                for cpu in range(224)]
    nodes = {f'node{node}': [cpu for cpu in range(224) if (cpu % 112) // 14 == node]
             for node in range(8)}
    candidates = local_affinities({'identity': {'topology': topology, 'numa_cpus': nodes}})
    admitted = {row['name']: row['cpus'] for row in candidates if len(row['cpus']) >= 53}
    assert {name: len(cpus) for name, cpus in admitted.items()} == {
        'socket0-physical': 56, 'socket0-logical': 112,
        'socket1-physical': 56, 'socket1-logical': 112, 'all-physical': 112}
    assert all(len({(topology[cpu]['package'], topology[cpu]['core']) for cpu in cpus}) == len(cpus)
               for name, cpus in admitted.items() if name.endswith('physical'))
    assert len({tuple(row['cpus']) for row in candidates}) == len(candidates)


def test_one_numa_per_socket_does_not_duplicate_the_same_cpu_candidate():
    profile = {'identity': {'topology': [
        {'cpu': 0, 'package': 0, 'core': 0}, {'cpu': 1, 'package': 0, 'core': 1},
        {'cpu': 2, 'package': 0, 'core': 0}, {'cpu': 3, 'package': 0, 'core': 1}],
        'numa_cpus': {'node0': [0, 1, 2, 3]}}}
    assert local_affinities(profile) == [{'name': 'node0-physical', 'cpus': [0, 1]},
                                       {'name': 'node0-logical', 'cpus': [0, 1, 2, 3]}]


def sample(threads, times, *, state='accepted', parity=True):
    return [{'state': state, 'parameters': {'polars_threads': threads, 'arrow_threads': threads},
             'complete_wall_seconds': elapsed, 'peak_rss_bytes': 48*1024**3,
             'exact_output_parity': parity, 'resource_budget_met': True} for elapsed in times]


def test_choice_uses_full_repeated_mean_and_keeps_rejections_out():
    runs = sample(8, [120, 121, 119]) + sample(16, [110, 111, 112]) + sample(32, [1], state='rejected')
    winner = choose_measured(runs, minimum_samples=3)
    assert winner['selected']['parameters']['polars_threads'] == 16
    assert winner['selected']['mean_complete_wall_seconds'] == 111
    assert len(winner['ranked']) == 2
    with pytest.raises(ValueError, match='output/resource proof'):
        choose_measured(sample(32, [1], parity=False))
    with pytest.raises(ValueError, match='invalid measured'):
        choose_measured(sample(32, [float('nan')]))


def selection_fixture(tmp_path):
    context = {'code': 'fixed-code', 'source': 'fixed-source', 'runtime': 'fixed-runtime'}
    profile = {'machine_sha256': 'node-identity', 'limits': {'cpu_worker_budget': 16, 'cpu_thread_trial_cap': 32, 'memory_headroom_bytes': 70*1024**3},
               'identity': {'affinity': list(range(32))}}
    parameters = {'polars_threads': 8, 'arrow_threads': 8, 'affinity': list(range(8)), 'memory_budget_bytes': 64*1024**3}
    basis = []
    for index in range(3):
        p = tmp_path / f'measured-{index}.json'
        p.write_text(json.dumps({'state': 'accepted', 'measurement_context': context,
                                'source_and_code_unchanged': True, 'peak_memory_budget_met': True,
                                'memory_budget_bytes': parameters['memory_budget_bytes'],
                                'node_profile_after': {'identity': {'affinity': parameters['affinity']}},
                                'effective_polars_threads': 8, 'effective_arrow_threads': 8,
                                'builds': [{'output_receipt': {'sha256': 'exact-output'}}]}))
        basis.append({'receipt': str(p), 'receipt_sha256': hashlib.sha256(p.read_bytes()).hexdigest(),
                      'complete_wall_seconds': 100+index})
    receipt = {'state': 'accepted', 'machine_sha256': profile['machine_sha256'], 'context': context,
               'selected': {'parameters': parameters, 'samples': 3, 'mean_complete_wall_seconds': 101},
               'expected_output_sha256': 'exact-output', 'selected_measurements': basis}
    return receipt, profile, context


def test_bound_measurement_can_be_applied_and_changed_evidence_cannot(tmp_path):
    receipt, profile, context = selection_fixture(tmp_path)
    assert validate_selection(receipt, profile, context)['polars_threads'] == 8
    Path(receipt['selected_measurements'][0]['receipt']).write_text('changed')
    with pytest.raises(ValueError, match='evidence changed'):
        validate_selection(receipt, profile, context)


@pytest.mark.parametrize('changed', ['affinity', 'memory_budget_bytes'])
def test_selection_requires_actual_affinity_and_memory_evidence(tmp_path, changed):
    receipt, profile, context = selection_fixture(tmp_path)
    selected = receipt['selected']['parameters']
    selected[changed] = list(range(16)) if changed == 'affinity' else 63 * 1024**3
    with pytest.raises(ValueError, match='does not establish'):
        validate_selection(receipt, profile, context)


@pytest.mark.parametrize('change', ['machine', 'source', 'quota', 'affinity', 'memory', 'samples', 'mean'])
def test_old_tuning_cannot_transfer_to_changed_conditions(tmp_path, change):
    receipt, profile, context = selection_fixture(tmp_path)
    if change == 'machine': profile['machine_sha256'] = 'another-node'
    if change == 'source': context = {**context, 'source': 'new-source'}
    if change == 'quota': profile['limits']['cpu_thread_trial_cap'] = 4
    if change == 'affinity': profile['identity']['affinity'] = [0, 1, 2]
    if change == 'memory': profile['limits']['memory_headroom_bytes'] = 63*1024**3
    if change == 'samples': receipt['selected_measurements'] = receipt['selected_measurements'][:1]
    if change == 'mean': receipt['selected']['mean_complete_wall_seconds'] = 1
    with pytest.raises(ValueError):
        validate_selection(receipt, profile, context)


def test_probe_resolves_nested_cgroups_and_fingerprints_capacity_not_current_usage(tmp_path, monkeypatch):
    proc, sys = tmp_path/'proc', tmp_path/'sys'
    group = tmp_path/'cgroup'; child = group/'parent/job'
    child.mkdir(parents=True); (proc/'self').mkdir(parents=True)
    (proc/'self/mountinfo').write_text(f'1 0 0:1 / / rw - ext4 /dev/example rw\n2 1 0:2 / {group} rw - cgroup2 cgroup rw\n')
    (proc/'self/cgroup').write_text('0::/parent/job\n')
    (proc/'meminfo').write_text('MemTotal: 100000000 kB\nMemAvailable: 90000000 kB\n')
    (proc/'cpuinfo').write_text('model name : Engineering CPU\n')
    (group/'parent/cpu.max').write_text('150000 100000')
    (child/'memory.max').write_text('1000000000')
    (child/'memory.current').write_text('100')
    monkeypatch.setattr('stockagent.remote_build.os.sched_getaffinity', lambda _: {0, 1, 2, 3})
    first = observe_node(tmp_path, tmp_path, proc=proc, sys=sys)
    assert first['limits']['cpu_worker_budget'] == 1
    assert first['limits']['memory_headroom_bytes'] == 999999900
    (child/'memory.current').write_text('200')
    second = observe_node(tmp_path, tmp_path, proc=proc, sys=sys)
    assert second['machine_sha256'] == first['machine_sha256']
    assert second['limits']['memory_headroom_bytes'] == 999999800
    (group/'parent/cpu.max').write_text('350000 100000')
    assert observe_node(tmp_path, tmp_path, proc=proc, sys=sys)['machine_sha256'] != first['machine_sha256']


def test_probe_records_explicit_reclaim_policy_and_observed_counters(tmp_path, monkeypatch):
    proc, sys = tmp_path / 'proc', tmp_path / 'sys'
    group = tmp_path / 'cgroup'; group.mkdir(); (proc / 'self').mkdir(parents=True)
    (proc / 'self/mountinfo').write_text(f'1 0 0:1 / / rw - ext4 /dev/fixture rw\n2 1 0:2 / {group} rw - cgroup2 cgroup rw\n')
    (proc / 'self/cgroup').write_text('0::/\n')
    (proc / 'meminfo').write_text('MemTotal: 10000 kB\nMemAvailable: 9000 kB\n')
    (proc / 'cpuinfo').write_text('model name : CPU fixture\n')
    (group / 'memory.max').write_text('1000');(group / 'memory.current').write_text('950')
    (group / 'memory.stat').write_text(memory_stats())
    monkeypatch.setattr('stockagent.remote_build.os.sched_getaffinity', lambda _: {0})
    original = observe_node(tmp_path, tmp_path, proc=proc, sys=sys)
    opt = observe_node(tmp_path, tmp_path, proc=proc, sys=sys, memory_policy='clean_file_reclaim_v1')
    assert opt['limits']['memory_headroom_bytes'] == 530
    assert opt['limits']['memory_unreclaimed_headroom_bytes'] == 50
    assert opt['identity']['memory_admission_policy'] == 'clean_file_reclaim_v1'
    assert opt['machine_sha256'] != original['machine_sha256']
    assert opt['cgroups'][0]['memory_stat'] == memory_stats()
