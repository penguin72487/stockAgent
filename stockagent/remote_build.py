"""Observed Linux node limits and version-bound full-build measurements.

This module describes admission and engineering evidence. Source releases,
builders, GPU ownership and financial semantics retain their existing owners.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import socket


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def cpu_list(value: str) -> list[int]:
    result = set()
    for token in value.strip().split(','):
        if not re.fullmatch(r'\d+(?:-\d+)?', token):
            raise ValueError('invalid CPU list')
        bounds = [int(part) for part in token.split('-')]
        first, last = bounds[0], bounds[-1]
        if last < first or last > 1048576:
            raise ValueError('invalid CPU range')
        result.update(range(first, last + 1))
    return sorted(result)


def _read(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except FileNotFoundError:
        return None


def _unescape_mount(value: str) -> str:
    return re.sub(r'\\([0-7]{3})', lambda match: chr(int(match[1], 8)), value)


def _mounts(proc: Path) -> list[dict]:
    records = []
    for line in (proc / 'self/mountinfo').read_text().splitlines():
        before, after = line.split(' - ', 1)
        left, right = before.split(), after.split()
        records.append({'root': _unescape_mount(left[3]), 'mount': _unescape_mount(left[4]),
                        'device': left[2], 'filesystem': right[0], 'source': _unescape_mount(right[1])})
    return records


def _storage(path: Path, mounts: list[dict]) -> dict:
    matches = [record for record in mounts if path.is_relative_to(record['mount'])]
    if not matches:
        raise ValueError('storage mount could not be observed')
    mount = max(matches, key=lambda record: len(record['mount']))
    return {'mount': mount['mount'], 'device': mount['device'],
            'filesystem': mount['filesystem'], 'source': mount['source']}


def effective_limits(affinity: list[int], memory_available: int, groups: list[dict]) -> dict:
    if not affinity or memory_available < 0:
        raise ValueError('invalid observed capacity')
    cpu_capacity = float(len(affinity))
    memory_headroom = memory_available
    for group in groups:
        quota = group.get('cpu_max')
        if quota is not None:
            amount, period = quota.split()
            if int(period) <= 0:
                raise ValueError('invalid cgroup CPU period')
            if amount != 'max':
                if int(amount) <= 0:
                    raise ValueError('invalid cgroup CPU quota')
                cpu_capacity = min(cpu_capacity, int(amount) / int(period))
        limit, used = group.get('memory_max'), group.get('memory_current')
        if limit is not None and limit != 'max':
            if used is None or int(limit) < 0 or int(used) < 0:
                raise ValueError('unverified cgroup memory headroom')
            memory_headroom = min(memory_headroom, max(0, int(limit) - int(used)))
    return {'cpu_capacity': cpu_capacity, 'cpu_worker_budget': max(1, math.floor(cpu_capacity)),
            'cpu_thread_trial_cap': min(len(affinity), 2 * max(1, math.ceil(cpu_capacity))),
            'memory_headroom_bytes': memory_headroom,
            'memory_policy': 'conservative MemAvailable and visible ancestor max-minus-current; no reclaim assumed'}


def observe_node(source: Path, scratch: Path, *, proc: Path = Path('/proc'),
                 sys: Path = Path('/sys')) -> dict:
    """Read actual affinity, accessible cgroup-v2 ancestors, topology and mounts."""
    source, scratch = source.resolve(strict=True), scratch.resolve(strict=True)
    mounts = _mounts(proc)
    cgroup_mounts = [record for record in mounts if record['filesystem'] == 'cgroup2']
    memberships = [line[3:] for line in (proc / 'self/cgroup').read_text().splitlines()
                   if line.startswith('0::')]
    if len(cgroup_mounts) != 1 or len(memberships) != 1 or '..' in Path(memberships[0]).parts:
        raise ValueError('remote build tuning requires an observed cgroup-v2 hierarchy')
    mount = cgroup_mounts[0]
    base = Path(mount['mount'])
    membership = Path(memberships[0])
    root = Path(mount['root'])
    relative = membership.relative_to(root) if membership.is_relative_to(root) else Path(str(membership).lstrip('/'))
    current = base / relative
    if not current.is_dir() or not current.is_relative_to(base):
        raise ValueError('cgroup membership does not map to its visible mount')
    groups = []
    while True:
        groups.append({'path': str(current), **{key: _read(current / key) for key in
                        ('cpu.max', 'memory.max', 'memory.current', 'cpu.stat', 'memory.events')}})
        groups[-1] = {key.replace('.', '_'): value for key, value in groups[-1].items()}
        if current == base:
            break
        current = current.parent
    memory = {line.split(':')[0]: int(line.split()[1]) * 1024
              for line in (proc / 'meminfo').read_text().splitlines()
              if line.startswith(('MemAvailable:', 'MemTotal:'))}
    affinity = sorted(os.sched_getaffinity(0))
    limits = effective_limits(affinity, memory['MemAvailable'], groups)
    topology, node_cpus = [], {}
    for cpu in affinity:
        directory = sys / f'devices/system/cpu/cpu{cpu}/topology'
        package, core = _read(directory / 'physical_package_id'), _read(directory / 'core_id')
        if package is not None and core is not None:
            topology.append({'cpu': cpu, 'package': int(package), 'core': int(core)})
    for path in sorted((sys / 'devices/system/node').glob('node[0-9]*/cpulist')):
        node_cpus[path.parent.name] = sorted(set(cpu_list(path.read_text())) & set(affinity))
    models = sorted({line.split(':', 1)[1].strip() for line in (proc / 'cpuinfo').read_text().splitlines()
                     if line.startswith('model name')})
    identity = {'node': socket.gethostname(), 'boot_id': _read(proc / 'sys/kernel/random/boot_id'),
                'architecture': platform.machine(), 'kernel': platform.release(), 'cpu_models': models,
                'affinity': affinity, 'topology': topology, 'numa_cpus': node_cpus,
                'cgroup_limits': [{key: group[key] for key in ('path', 'cpu_max', 'memory_max')}
                                  for group in groups], 'memory_total_bytes': memory['MemTotal'],
                'source_storage': _storage(source, mounts), 'scratch_storage': _storage(scratch, mounts)}
    return {'schema_version': 1, 'identity': identity, 'machine_sha256': digest(identity),
            'limits': limits, 'cgroups': groups,
            'observations': {'mem_available_bytes': memory['MemAvailable'],
                             'scratch_free_bytes': shutil.disk_usage(scratch).free,
                             'load_average': list(os.getloadavg()),
                             'cpu_pressure': _read(proc / 'pressure/cpu'),
                             'io_pressure': _read(proc / 'pressure/io')},
            'scope': 'visible Linux/cgroup-v2 capacity and mount identity; no exclusive-host or disk-cold claim'}


def candidate_threads(profile: dict) -> list[int]:
    budget = profile['limits']['cpu_worker_budget']
    cap = profile['limits']['cpu_thread_trial_cap']
    return sorted({value for value in (2, 4, 8, 16, 32, 64, 128, budget, cap) if value <= cap})


def local_affinities(profile: dict) -> list[dict]:
    """Observed NUMA, socket and physical-core sets, without assuming NPS1."""
    topology = {row['cpu']: (row['package'], row['core']) for row in profile['identity']['topology']}
    result, seen_sets = [], set()

    def add_scope(name, cpus, *, logical=True):
        seen, physical = set(), []
        cpus = sorted(cpus)
        for cpu in cpus:
            key = topology.get(cpu)
            if key is not None and key not in seen:
                seen.add(key)
                physical.append(cpu)
        for kind, selected in [('physical', physical)] + ([('logical', cpus)] if logical else []):
            key = tuple(selected)
            if selected and key not in seen_sets:
                result.append({'name': name + '-' + kind, 'cpus': selected})
                seen_sets.add(key)

    for name, cpus in profile['identity']['numa_cpus'].items():
        add_scope(name, cpus)
    for package in sorted({key[0] for key in topology.values()}):
        add_scope('socket' + str(package), [cpu for cpu, key in topology.items() if key[0] == package])
    add_scope('all', list(topology), logical=False)
    return result


def choose_measured(runs: list[dict], *, minimum_samples: int = 1) -> dict:
    groups = {}
    for run in runs:
        if run['state'] == 'accepted':
            if not run.get('exact_output_parity') or not run.get('resource_budget_met'):
                raise ValueError('accepted measurement lacks output/resource proof')
            if not math.isfinite(run['complete_wall_seconds']) or run['complete_wall_seconds'] <= 0:
                raise ValueError('invalid measured wall time')
            groups.setdefault(digest(run['parameters']), []).append(run)
    ranked = []
    for key, values in groups.items():
        if len(values) >= minimum_samples:
            times = [value['complete_wall_seconds'] for value in values]
            ranked.append({'parameters': values[0]['parameters'], 'parameter_sha256': key,
                           'samples': len(times), 'mean_complete_wall_seconds': sum(times) / len(times),
                           'min_complete_wall_seconds': min(times), 'max_complete_wall_seconds': max(times),
                           'max_peak_rss_bytes': max(value['peak_rss_bytes'] for value in values)})
    if not ranked:
        raise ValueError('no compatible measured candidate')
    ranked.sort(key=lambda row: (row['mean_complete_wall_seconds'], row['max_complete_wall_seconds'],
                                 row['parameters']['polars_threads'], row['parameters']['arrow_threads']))
    return {'selected': ranked[0], 'ranked': ranked,
            'claim': 'fastest mean complete wall among the eligible measured candidates on this bound node/workload'}


def validate_selection(receipt: dict, profile: dict, context: dict) -> dict:
    if receipt.get('state') != 'accepted' or receipt.get('machine_sha256') != profile['machine_sha256']:
        raise ValueError('measured selection does not match the current node')
    if receipt.get('context') != context:
        raise ValueError('measured selection does not match code/source/runtime/workload')
    selected = receipt['selected']['parameters']
    budget = profile['limits']['cpu_thread_trial_cap']
    if any(type(selected[key]) is not int or not 1 <= selected[key] <= budget
           for key in ('polars_threads', 'arrow_threads')):
        raise ValueError('measured pools exceed the current CPU budget')
    cpus = selected['affinity']
    if cpus is not None and (not cpus or not set(cpus) <= set(profile['identity']['affinity'])
                             or len(cpus) < max(selected['polars_threads'], selected['arrow_threads'])):
        raise ValueError('measured CPU affinity is no longer available')
    if selected['memory_budget_bytes'] > profile['limits']['memory_headroom_bytes']:
        raise ValueError('measured selection has insufficient current memory headroom')
    basis = receipt.get('selected_measurements', [])
    if len(basis) < 3 or receipt['selected']['samples'] != len(basis):
        raise ValueError('measured selection requires repeated complete-build evidence')
    times = []
    for sample in basis:
        body = Path(sample['receipt']).read_bytes()
        if hashlib.sha256(body).hexdigest() != sample['receipt_sha256']:
            raise ValueError('measured build evidence changed')
        actual = json.loads(body)
        if (actual.get('state') != 'accepted' or actual.get('measurement_context') != context
                or not actual.get('source_and_code_unchanged') or not actual.get('peak_memory_budget_met')
                or actual.get('memory_budget_bytes') != selected['memory_budget_bytes']
                or actual['builds'][0]['output_receipt']['sha256'] != receipt['expected_output_sha256']
                or actual['effective_polars_threads'] != selected['polars_threads']
                or actual['effective_arrow_threads'] != selected['arrow_threads']
                or (cpus is not None and actual.get('node_profile_after', {}).get('identity', {}).get('affinity') != cpus)):
            raise ValueError('measured build evidence does not establish the selected settings')
        elapsed = sample['complete_wall_seconds']
        if not math.isfinite(elapsed) or elapsed <= 0:
            raise ValueError('invalid complete-build sample time')
        times.append(elapsed)
    if not math.isclose(sum(times)/len(times), receipt['selected']['mean_complete_wall_seconds'], rel_tol=1e-12):
        raise ValueError('measured selection mean differs from its evidence')
    return selected
