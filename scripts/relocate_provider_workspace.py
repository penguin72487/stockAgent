#!/usr/bin/env python3
"""Copy and verify enrolled acquisition roots on E, then preserve their aliases.

Run through the existing agent workflow. Related collectors are fenced while
their original state is copied; the old C roots are retained for recovery.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from concurrent.futures import ProcessPoolExecutor, as_completed
import fcntl
import hashlib
import json
import os
import multiprocessing
from pathlib import Path
import signal
import subprocess
import sys
import time

from downloader.artifact_io import atomic_write_json
from downloader.dataset_lock import exclusive_dataset_lock
from stockagent.data_sync import provider_workspace as placement
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from scripts.reload_finmind_at_idle import _pidfd_open, _pidfd_signal

ROOT = placement.ROOT
STATE = volume.ENROLLMENT.parent / placement.GENERATION
FENCE = Path('/run/stockagent-provider-migration.active')
SYSTEMD_RUNTIME_ROOT = Path('/run/systemd/system')
TIMERS = ('stockagent-finlab-daytrade-priority.timer', 'stockagent-finlab-daytrade-projection.timer',
          'stockagent-finlab-quota-snapshot.timer', 'stockagent-finmind-quota-snapshot.timer',
          'stockagent-tej-api-trial.timer')
SERVICES = tuple(name.replace('.timer', '.service') for name in TIMERS) + (
    'stockagent-tej-history.service', 'stockagent-finmind-free.service',
    'stockagent-finmind-sponsor.service', 'stockagent-finmind-complement.service')
FINITE_SCRIPTS = ('scripts/run_finlab_finite_priority_handoff.py',
                  'scripts/run_finmind_option_corrected_product_backlog.py',
                  'scripts/run_finmind_option_conflict_wait.py')
DROPIN = '90-temporary-provider-e-migration.conf'


def command(argv, **kwargs):
    try:
        return subprocess.run([str(a) for a in argv], check=True, capture_output=True, text=True, **kwargs).stdout.strip()
    except subprocess.CalledProcessError as error:
        raise RuntimeError('provider placement child failed: '+error.stderr[-2400:]) from error


def save(value):
    placement.REGISTRY.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    atomic_write_json(placement.REGISTRY, value)


def save_entry(name, entry):
    """Independent provider lanes share one serialized canonical registry."""
    with (volume.ENROLLMENT.parent/'provider-placement.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        value = placement.load()
        value['providers'][name] = entry
        save(value)


def process_rows():
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            argv = (proc/'cmdline').read_bytes().split(b'\0')
            yield proc, [os.fsdecode(arg) for arg in argv if arg]
        except FileNotFoundError:
            continue


def pause():
    """Keep disabled general history lanes disabled; record exact finite owners."""
    record = STATE / 'pause.json'
    previous = None
    if record.exists():
        if not FENCE.exists():
            raise RuntimeError('existing migration lost its writer fence')
        previous = json.loads(record.read_bytes())
    STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    FENCE.write_text(placement.GENERATION + '\n')
    states = dict(previous['unit_states']) if previous else {}
    for unit in (*TIMERS, *SERVICES):
        if unit not in states:
            states[unit] = command(['systemctl', 'show', unit, '--property=ActiveState', '--value'])
        d = SYSTEMD_RUNTIME_ROOT / (unit + '.d')
        d.mkdir(parents=True, exist_ok=True)
        (d/DROPIN).write_text('[Unit]\nConditionPathExists=!' + str(FENCE) + '\n')
    finite = set(previous['finite_units_stopped']) if previous else set()
    recovery = {row['pid']: row for row in previous['interactive_recovery']} if previous else {}
    for proc, argv in process_rows():
        if any(script in argv for script in FINITE_SCRIPTS):
            cgroup = (proc/'cgroup').read_text().strip().split('/')[-1]
            if not cgroup.startswith('stockagent-agent-') or not cgroup.endswith('.service'):
                raise RuntimeError('finite provider worker has an unknown owner')
            finite.add(cgroup)
        if 'scripts/recover_tej_startup.py' in argv:
            recovery.setdefault(int(proc.name), {'pid': int(proc.name),
                'start_ticks': (proc/'stat').read_text().split(') ', 1)[1].split()[19],
                'argv': argv[1:], 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()})
    recovery = list(recovery.values())
    value = {'unit_states': states, 'finite_units_stopped': sorted(finite), 'interactive_recovery': recovery,
             'observed_at_epoch': time.time(), 'state': 'fenced'}
    atomic_write_json(record, value)
    command(['systemctl', 'daemon-reload'])
    command(['systemctl', 'stop', *TIMERS, *SERVICES, *sorted(finite)], timeout=90)
    for row in recovery:
        proc = Path('/proc')/str(row['pid'])
        try:
            fd = _pidfd_open(row['pid'])
        except ProcessLookupError:
            continue
        try:
            if not recovery_alive(row):
                continue  # The old owner exited; never signal a reused PID.
            _pidfd_signal(fd, signal.SIGTERM)
        finally:
            os.close(fd)
    deadline = time.monotonic()+65
    while any(recovery_alive(row) for row in recovery):
        if time.monotonic() > deadline:
            raise RuntimeError('original TEJ interactive owner has not drained; retained fence')
        time.sleep(.5)
    value.update(state='paused', paused_at_epoch=time.time())
    atomic_write_json(record, value)
    return value


def recovery_alive(row):
    try:
        return (Path('/proc', str(row['pid']), 'stat').read_text().split(') ', 1)[1].split()[19] == row['start_ticks']
                and Path('/proc/sys/kernel/random/boot_id').read_text().strip() == row['boot_id'])
    except FileNotFoundError:
        return False


def lock_paths(name):
    alias = ROOT / ('data_' + name)
    paths = {alias/'.priority_queries.lock', alias/'.gap_priority.lock'}
    if name == 'finlab':
        paths.update((alias/'.sync.lock', alias/'intraday/.sync.lock'))
    elif name == 'finmind':
        for part in ('', 'complement', 'sponsor'):
            paths.update((alias/part/'worker.lock', alias/part/'.priority_queries.lock'))
        paths.add(alias/'account_status.lock')
    else:
        paths.update(alias/p for p in ('.download.lock', '.scheduler.lock', '.desktop-startup.lock'))
        paths.add(alias/'api_trial_v1/.api.lock')
    return sorted(p for p in paths if p.parent.exists())


def namespaces(*, with_roots=False):
    """Pin each relevant namespace before changing any aliases; no PID reuse."""
    result = {}
    identity = ROOT.stat()
    try:
        for proc, _ in process_rows():
            try:
                s = (proc/'root'/str(ROOT).lstrip('/')).stat()
                if (s.st_dev, s.st_ino) != (identity.st_dev, identity.st_ino):
                    continue
                fd = os.open(proc/'ns/mnt', os.O_RDONLY)
                inode = os.fstat(fd).st_ino
                if inode in result:
                    os.close(fd)
                else:
                    if with_roots:
                        root_fd = None
                        try:
                            root_fd = os.open(proc/'root', os.O_RDONLY | os.O_DIRECTORY)
                            if os.stat(proc/'ns/mnt').st_ino != inode:
                                os.close(root_fd)
                                os.close(fd)
                                continue
                        except BaseException:
                            if root_fd is not None:
                                os.close(root_fd)
                            os.close(fd)
                            raise
                        result[inode] = (fd, root_fd)
                    else:
                        result[inode] = fd
            except FileNotFoundError:
                continue
        return result
    except BaseException:
        for value in result.values():
            for fd in (value if with_roots else (value,)):
                os.close(fd)
        raise


def bind_here(name):
    """Executed inside an already pinned namespace, using the fixed registry."""
    entry = placement.load()['providers'][name]
    if entry['state'] not in {'cutover', 'active'}:
        raise RuntimeError('provider has no completed copy and cutover intent')
    try:
        volume.check()
    except Exception:
        # Reuse the original enrolled workspace mounter, never create a C copy.
        from scripts.manage_workspace_volume import mount
        mount(volume.configuration())
    alias, destination = Path(entry['alias']), Path(entry['destination'])
    if not placement.exact_mount(alias, destination):
        if alias.is_symlink() or alias.is_mount() or any(alias.iterdir()):
            raise RuntimeError('refuse to hide an occupied or different provider root')
        if relocation.signature(alias)[:2] != entry['placeholder_identity']:
            raise RuntimeError('provider placeholder inode changed')
        command(['mount', '--bind', destination, alias])
    for child in entry['external_subtrees']:
        target, source = alias/child['relative'], Path(child['source'])
        if not placement.exact_mount(target, source):
            if target.is_symlink() or target.is_mount() or any(target.iterdir()):
                raise RuntimeError('refuse to hide an occupied external subtree')
            command(['mount', '--bind', source, target])
        if child['readonly']:
            command(['mount', '-o', 'remount,bind,ro', target])
    placement.check_entry(name, entry, allow_cutover=True)
    return {'namespace_inode': os.stat('/proc/self/ns/mnt').st_ino, 'provider': name, 'verified': True}


def vanished_namespace_after_missing_attach(inode, error):
    """Skip only a vanished namespace after an identified ENOENT attachment."""
    detail = str(error)
    if 'FileNotFoundError: [Errno 2]' not in detail or 'provider alias attach failed:' not in detail:
        return False
    current = namespaces()
    try:
        return inode not in current
    finally:
        for fd in current.values():
            os.close(fd)


def mount_everywhere(name):
    entry = placement.load()['providers'][name]
    if entry['state'] not in {'cutover', 'active'}:
        raise RuntimeError('provider namespace handoff lacks its completed copy')
    volume.check(physical=True)
    if relocation.digest(Path(entry['evidence'])/'copy-result.json') != entry['copy_result_sha256']:
        raise RuntimeError('provider namespace handoff copy proof changed')
    program = namespace_program()
    entry['namespace_helper_sha256'] = hashlib.sha256(program.encode()).hexdigest()
    save_entry(name, entry)
    descriptors = []
    def pinned(path, **fields):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(fd)
        info = os.fstat(fd)
        return {'fd': fd, 'identity': [info.st_dev, info.st_ino], **fields}
    root = pinned(entry['destination'])
    proc_fd = os.open('/proc', os.O_RDONLY | os.O_DIRECTORY)
    descriptors.append(proc_fd)
    if root['identity'][0] != Path(volume.configuration()['mount_point']).stat().st_dev:
        raise RuntimeError('provider destination is not the enrolled E filesystem')
    children = [pinned(child['source'], relative=child['relative'], readonly=child['readonly'])
                for child in entry['external_subtrees']]
    request = {'provider': name, 'repository_root': str(ROOT), 'repository_identity': relocation.signature(ROOT)[:2],
               'root': root, 'children': children, 'placeholder_identity': entry['placeholder_identity'],
               'proc_fd': proc_fd}
    seen = set()
    summaries = []
    try:
        for _ in range(5):
            batch = namespaces(with_roots=True)
            try:
                pending = {inode: fd for inode, fd in batch.items() if inode not in seen}
                if not pending:
                    volume.check()
                    atomic_write_json(Path(entry['evidence'])/'namespace-readback.json', {
                        'provider': name, 'namespace_helper_sha256': entry['namespace_helper_sha256'],
                        'copy_result_sha256': entry['copy_result_sha256'], 'namespace_readbacks': summaries,
                        'observed_at_epoch': time.time(), 'service_code_mounts_changed': 0})
                    return sorted(seen)
                for inode, (fd, root_fd) in pending.items():
                    from stockagent.data_sync.transport_namespace_handoff import prepared_tree
                    with ExitStack() as trees:
                        specifications = [(entry['destination'], root, False),
                            *[(child['source'], spec, child['readonly'])
                              for child, spec in zip(entry['external_subtrees'], children)]]
                        requests = []
                        tree_fds = []
                        for path, spec, readonly in specifications:
                            tree = trees.enter_context(prepared_tree(Path(path), ['ro' if readonly else 'rw', 'relatime']))
                            tree_fds.append(tree)
                            info = os.fstat(tree)
                            if [info.st_dev, info.st_ino] != spec['identity']:
                                raise RuntimeError('detached mount source differs from pinned directory')
                            requests.append({**spec, 'fd': tree})
                        request.update(namespace_inode=inode, namespace_fd=fd, namespace_root_fd=root_fd,
                                       root=requests[0], children=requests[1:])
                        try:
                            output = command([sys.executable, '-c', program],
                                pass_fds=(fd, root_fd, proc_fd, *tree_fds), input=json.dumps(request), timeout=90)
                        except RuntimeError as error:
                            if vanished_namespace_after_missing_attach(inode, error):
                                summaries.append({'namespace_inode': inode, 'verified': False,
                                                  'state': 'vanished_during_mount_handoff'})
                                continue
                            raise RuntimeError(f'provider {name} namespace {inode}: {error}') from error
                    value = json.loads(output)
                    if not value['verified'] or value['namespace_inode'] != inode:
                        raise RuntimeError('namespace child readback differs')
                    targets = [value['target'] for value in value['readbacks']]
                    probe = ('import os,ctypes,json,sys\nq=json.load(sys.stdin);c=ctypes.CDLL(None,use_errno=True)\n'
                             'assert c.setns(q["namespace_fd"],0)==0\nos.fchdir(q["namespace_root_fd"]);os.chroot(".");os.chdir("/")\n'
                             'for p in q["paths"]:\n s=os.stat(p);print(str(s.st_dev)+":"+str(s.st_ino))\n')
                    observed = command([sys.executable, '-c', probe], pass_fds=(fd, root_fd),
                        input=json.dumps({'namespace_fd': fd, 'namespace_root_fd': root_fd, 'paths': targets}), timeout=10).splitlines()
                    expected = [':'.join(map(str, spec['identity'])) for spec in [root, *children]]
                    if observed != expected:
                        raise RuntimeError('independent namespace stat readback differs')
                    summaries.append({**value, 'independent_stat': observed})
                    seen.add(inode)
            finally:
                for fds in batch.values():
                    for fd in fds:
                        os.close(fd)
        raise RuntimeError('mount namespace census did not converge; cutover remains fenced')
    finally:
        for fd in descriptors:
            os.close(fd)


def namespace_program():
    """Use the existing detached-mount API without importing frozen code there."""
    import inspect
    from stockagent.data_sync.transport_namespace_handoff import MountAttributes, mount_api, attach_tree
    return ('import os,ctypes\n'+inspect.getsource(MountAttributes)+'\n'+inspect.getsource(mount_api)+'\n'+
            inspect.getsource(attach_tree)+'\n'+(ROOT/'scripts/provider_workspace_namespace.py').read_text())


def collect_provider_jobs(jobs):
    """Expose a failed lane immediately while other owners finish safely."""
    failures = []
    for job in as_completed(jobs):
        name = jobs[job]
        try:
            job.result()
        except Exception as error:
            failures.append(name)
            print(json.dumps({'event': 'provider_worker_failed', 'provider': name,
                              'error_type': type(error).__name__, 'error': str(error)[-2400:]},
                             ensure_ascii=False), flush=True)
    if failures:
        raise RuntimeError('provider placement failed; retained original and fence: ' + ', '.join(sorted(failures)))


def move(name, workers, verify_workers=1):
    with ExitStack() as locks:
        return move_locked(name, workers, locks, verify_workers=verify_workers)


def move_locked(name, workers, locks, verify_workers=1):
    value = placement.load()
    if name not in value['providers']:
        c = volume.configuration()
        base = Path(c['mount_point'])
        external = []
        if name == 'finlab':
            external = [{'relative': 'intraday/objects/futures-workspace-v1',
                         'source': str(base/volume.FINLAB_FUTURES_ROLE), 'readonly': False}]
            volume.check_finlab_futures()
        if name == 'finmind':
            for directory in ('parquet', 'receipts', 'receipt_history'):
                rel = 'complement/'+directory+'/USStockPriceMinute'
                if (ROOT/'data_finmind'/rel).is_dir():
                    external.append({'relative': rel, 'source': str(ROOT/'.data_finmind.pre-e-20261103'/rel),
                                     'readonly': True})
        entry = {'alias': str(ROOT/('data_'+name)), 'destination': str(base/'downloads'/placement.GENERATION/name/'data'),
                 'backup': str(ROOT/('.data_'+name+'.pre-e-20261103')),
                 'evidence': str(base/'migration'/placement.GENERATION/name),
                 'filesystem_uuid': json.loads(volume.ENROLLMENT.read_bytes())['filesystem_uuid'],
                 'external_subtrees': external, 'state': 'copying'}
        if Path(entry['backup']).exists():
            raise RuntimeError('retain unregistered previous provider backup')
        value['providers'][name] = entry
        save_entry(name, entry)
    entry = value['providers'][name]
    if entry['state'] == 'active':
        return placement.check_entry(name, entry)
    alias, destination, evidence = (Path(entry[k]) for k in ('alias', 'destination', 'evidence'))
    if entry['state'] == 'copying':
        for path in lock_paths(name):
            locks.enter_context(exclusive_dataset_lock(path, provider='provider_e_relocation', timeout_seconds=0))
        # Keep these original lock descriptions through rename and alias readback.
        references = process_references_many([alias], limit=100)
        if references:
            atomic_write_json(STATE/(name+'-references.json'), references)
            raise RuntimeError('provider has live process references; retained original and fence')
        exclusions = tuple(child['relative'] for child in entry['external_subtrees'])
        db = relocation.index(alias, destination, evidence, excluded_subtrees=exclusions, preserve_symlinks=True)
        intent = json.loads((evidence/'intent.json').read_bytes())
        db.close()
        budget = intent['logical_bytes'] + intent['entries']*16384 + 1024**2
        controls = evidence.parent/'capacity-leases'
        controls.mkdir(mode=0o700, exist_ok=True)
        lease = locks.enter_context(reserve_capacity(controls, key=hashlib.sha256(name.encode()).hexdigest(),
            maximum_growth_bytes=budget, capacity_bytes=2**63-1,
            minimum_free_bytes=volume.configuration()['minimum_free_bytes']))
        capacity = volume.admit(budget + lease['other_reserved_growth_bytes'])
        atomic_write_json(evidence/'capacity-admission.json', capacity)
        print(json.dumps({'provider': name, 'phase': 'copying', 'files': intent['files'],
                          'logical_bytes': intent['logical_bytes']}), flush=True)
        relocation.copy(alias, destination, evidence, workers=workers, excluded_subtrees=exclusions,
                        preserve_symlinks=True, sync_batch_files=512, verify_workers=verify_workers)
        entry.update(state='verified_copy', copy_result_sha256=relocation.digest(evidence/'copy-result.json'),
                     source_identity=relocation.signature(alias))
        save_entry(name, entry)
    if entry['state'] == 'verified_copy':
        if relocation.digest(evidence/'copy-result.json') != entry['copy_result_sha256']:
            raise RuntimeError('full-copy receipt changed before cutover')
        backup = Path(entry['backup'])
        if not backup.exists():
            if process_references_many([alias], limit=100):
                raise RuntimeError('provider acquired live references before cutover')
            if relocation.signature(alias) != entry['source_identity']:
                raise RuntimeError('provider root changed before cutover')
            alias.rename(backup)
        elif relocation.signature(backup)[:2] != entry['source_identity'][:2]:
            raise RuntimeError('retained provider root identity differs')
        if not alias.exists():
            alias.mkdir(mode=0o700)
        if alias.is_symlink() or alias.is_mount() or any(alias.iterdir()):
            raise RuntimeError('provider cutover placeholder is occupied')
        entry.update(state='cutover', placeholder_identity=relocation.signature(alias)[:2])
        save_entry(name, entry)
    inodes = mount_everywhere(name)
    entry = placement.load()['providers'][name]
    entry['namespace_inodes_verified'] = inodes
    entry['namespace_readback_sha256'] = relocation.digest(evidence/'namespace-readback.json')
    entry.update(state='active', activated_at_epoch=time.time(), original_files_deleted=0)
    save_entry(name, entry)
    result = placement.check_entry(name, entry)
    atomic_write_json(evidence/'placement-result.json', {**entry, **result})
    print(json.dumps(result), flush=True)
    return result


def _provider_parent_mount_unit(entry, child_units):
    """Re-enqueue retained children when a delayed parent mount recovers."""
    children = 'Wants=' + ' '.join(child_units) + '\n' if child_units else ''
    return ('[Unit]\nDescription=Verified temporary E provider state\n'
            'DefaultDependencies=no\nBefore=umount.target\nConflicts=umount.target\n'
            'Requires=stockagent-e-workspace-mount.service\nAfter=stockagent-e-workspace-mount.service\n'
            + children + '[Mount]\nWhat=' + entry['destination'] + '\nWhere=' + entry['alias'] +
            '\nType=none\nOptions=bind\n[Install]\nWantedBy=multi-user.target\n')


def _provider_child_mount_unit(child, target, parent_unit):
    """A post-local-fs retained bind must not implicitly precede local-fs."""
    return ('[Unit]\nDescription=Provider enrolled retained subtree\n'
            'DefaultDependencies=no\nBefore=umount.target\nConflicts=umount.target\nRequires='+parent_unit+
            '\nAfter='+parent_unit+'\n[Mount]\nWhat='+child['source']+'\nWhere='+str(target)+
            '\nType=none\nOptions=bind'+(',ro' if child['readonly'] else '')+
            '\n[Install]\nWantedBy=multi-user.target\n')


def _provider_child_upgrade(child, target, parent_unit, existing):
    if ('What='+child['source']+'\n') not in existing:
        raise RuntimeError('existing child mount has a different source')
    legacy = ('[Unit]\nDescription=Provider enrolled retained subtree\nRequires='+parent_unit+
              '\nAfter='+parent_unit+'\n[Mount]\nWhat='+child['source']+'\nWhere='+str(target)+
              '\nType=none\nOptions=bind'+(',ro' if child['readonly'] else '')+
              '\n[Install]\nWantedBy=multi-user.target\n')
    # Do not replace another owner's pre-existing child unit, even if its
    # source path matches. Only our exact previous generated text is upgraded.
    return _provider_child_mount_unit(child, target, parent_unit) if existing == legacy else None


def install_mounts():
    value = placement.load()
    units = []
    for entry in value['providers'].values():
        alias = Path(entry['alias'])
        unit = command(['systemd-escape', '--path', '--suffix=mount', alias])
        children = [(child, alias / child['relative'],
                     command(['systemd-escape', '--path', '--suffix=mount', alias / child['relative']]))
                    for child in entry['external_subtrees']]
        Path('/etc/systemd/system', unit).write_text(_provider_parent_mount_unit(
            entry, [child_unit for _, _, child_unit in children]))
        units.append(unit)
        for child, target, subunit in children:
            targetfile = Path('/etc/systemd/system', subunit)
            if targetfile.exists():
                # The existing FinLab child keeps its canonical owner/unit.
                replacement = _provider_child_upgrade(child, target, unit, targetfile.read_text())
                if replacement is not None:
                    targetfile.write_text(replacement)
                    units.append(subunit)
                continue
            targetfile.write_text(_provider_child_mount_unit(child, target, unit))
            units.append(subunit)
    for service in (*SERVICES, 'stockagent-finlab-futures-workspace.service'):
        directory = Path('/etc/systemd/system')/(service+'.d')
        directory.mkdir(parents=True, exist_ok=True)
        (directory/'provider-e-placement.conf').write_text('[Unit]\nRequiresMountsFor='+
            ' '.join(entry['alias'] for entry in value['providers'].values())+'\n')
    command(['systemctl', 'daemon-reload'])
    command(['systemctl', 'enable', *units])
    return units


def resume_finite():
    """Optional node-local handoff plan; original scripts, tasks and requests.

    Stable names plus exact argv readback close the launch/receipt crash gap.
    This schedules canonical owners only after all E placement gates passed.
    """
    plan_path = STATE/'continuation.json'
    if not plan_path.exists():
        return {}
    plan = json.loads(plan_path.read_bytes())
    if plan.get('contract') != 'provider_e_finite_continuation_v1' or not 0 < len(plan.get('jobs', [])) <= 3:
        raise RuntimeError('invalid finite provider continuation plan')
    import re
    runs = ROOT/'artifacts/operations/agent-workflow/runs'
    launched = {}
    for job in plan['jobs']:
        if (not re.fullmatch(r'[a-z0-9_-]{1,80}', job['name'])
                or job['role'] in launched or job['argv'][:1] != ['run_fintech_python']
                or job['argv'][1] not in FINITE_SCRIPTS):
            raise RuntimeError('continuation must use the existing finite owner scripts')
        argv = [launched[value[5:]] if value.startswith('@run:') else value for value in job['argv']]
        previous = []
        for record in runs.glob(job['name']+'-*/run.json'):
            row = json.loads(record.read_bytes())
            if row.get('task_id') != job['task'] or row.get('argv') != argv:
                raise RuntimeError('stable continuation name has incompatible previous argv')
            previous.append(row)
        if len(previous) > 1:
            raise RuntimeError('continuation has multiple previous owners; no duplicate launch')
        if previous:
            row = previous[0]
        else:
            row = json.loads(command(['stockagent-agent', 'run', '--backend', 'systemd', '--name', job['name'],
                                      job['task'], '--', *argv], timeout=60))
        launched[job['role']] = row['run_id']
        atomic_write_json(STATE/'continuation-results.json', {'plan_sha256': relocation.digest(plan_path),
            'runs': launched, 'observed_at_epoch': time.time(), 'training_ready_claim': False})
    return launched


def replan(name, reason):
    """Preserve an interrupted proof/payload, explicitly re-inventory new state."""
    if not reason:
        raise RuntimeError('an explicit replan requires its source-change reason')
    value = placement.load()
    entry = value['providers'][name]
    alias, destination, evidence = (Path(entry[k]) for k in ('alias', 'destination', 'evidence'))
    if entry['state'] != 'copying' or Path(entry['backup']).exists() or alias.is_mount():
        raise RuntimeError('only an unswitched, interrupted provider copy may be replanned')
    with ExitStack() as locks:
        for path in lock_paths(name):
            locks.enter_context(exclusive_dataset_lock(path, provider='provider_e_replan', timeout_seconds=0))
        if process_references_many([alias, destination, evidence], limit=100):
            raise RuntimeError('interrupted provider copy still has live references')
        volume.admit()
        retained = evidence.parent/'replans'/(name+'-'+str(time.time_ns()))
        retained.mkdir(mode=0o700, parents=True)
        journal = {'state': 'planned', 'reason': reason, 'entry': entry,
                   'source_current_signature': relocation.signature(alias),
                   'old_intent_sha256': relocation.digest(evidence/'intent.json'),
                   'old_index_sha256': relocation.digest(evidence/'files.sqlite'),
                   'old_payload_identity': relocation.signature(destination),
                   'retained_at': str(retained), 'original_files_deleted': 0}
        atomic_write_json(retained/'replan.json', journal)
        evidence.rename(retained/'evidence')
        destination.rename(retained/'payload')
        journal['state'] = 'interrupted_copy_and_proof_retained'
        atomic_write_json(retained/'replan.json', journal)
        entry['replans'] = [*entry.get('replans', []), str(retained/'replan.json')]
        save(value)


def resume(saved):
    for name, entry in placement.load()['providers'].items():
        placement.check_entry(name, entry)
    for unit in (*TIMERS, *SERVICES):
        (SYSTEMD_RUNTIME_ROOT/(unit+'.d')/DROPIN).unlink()
    FENCE.unlink()
    command(['systemctl', 'daemon-reload'])
    # Timers restart their oneshots; the original TEJ history resumes directly.
    restore = [unit for unit in TIMERS if saved['unit_states'][unit] in {'active', 'activating'}]
    if saved['unit_states']['stockagent-tej-history.service'] == 'active':
        restore.append('stockagent-tej-history.service')
    if restore:
        command(['systemctl', 'start', *restore], timeout=45)
    atomic_write_json(STATE/'resumed.json', {'state': 'original_scheduled_owners_resumed', 'units': restore,
        'finite_runs_require_new_receipts': saved['finite_units_stopped'],
        'interactive_recovery_requires_original_owner': saved['interactive_recovery'],
        'observed_at_epoch': time.time(), 'source_files_deleted': 0})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--namespace-bind', choices=placement.PROVIDERS, help=argparse.SUPPRESS)
    parser.add_argument('--workers', type=int, default=12)
    parser.add_argument('--verify-workers', type=int, default=1)
    parser.add_argument('--provider-workers', type=int, default=3)
    parser.add_argument('--replan-provider', choices=placement.PROVIDERS)
    parser.add_argument('--replan-reason')
    parser.add_argument('--wait-owner-seconds', type=int, default=0)
    args = parser.parse_args()
    if (not 1 <= args.workers <= 128 or not 1 <= args.verify_workers <= 128 or not 1 <= args.provider_workers <= 3
            or not 0 <= args.wait_owner_seconds <= 86400):
        parser.error('workers and verify-workers must be 1..128, provider-workers 1..3, owner wait 0..86400 seconds')
    if args.namespace_bind:
        print(json.dumps(bind_here(args.namespace_bind)))
        return
    if os.geteuid() != 0:
        raise RuntimeError('enrolled provider alias operation requires root')
    STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (STATE/'owner.lock').open('a') as lock:
        deadline = time.monotonic()+args.wait_owner_seconds
        last_report = 0
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise
                if time.monotonic()-last_report >= 60:
                    atomic_write_json(STATE/'owner-wait.json', {'state': 'waiting_for_original_relocation_owner',
                        'pid': os.getpid(), 'observed_at_epoch': time.time(), 'takes_over_no_live_owner': True})
                    last_report = time.monotonic()
                time.sleep(5)
        volume.admit()
        if (STATE/'resumed.json').exists():
            for name, entry in placement.load()['providers'].items():
                placement.check_entry(name, entry)
            units = []
        else:
            saved = pause()
            if args.replan_provider:
                replan(args.replan_provider, args.replan_reason)
            if args.provider_workers == 1:
                for name in placement.PROVIDERS:
                    move(name, args.workers, args.verify_workers)
            else:
                with ProcessPoolExecutor(max_workers=args.provider_workers,
                                         mp_context=multiprocessing.get_context('spawn')) as pool:
                    jobs = {pool.submit(move, name, args.workers, args.verify_workers): name
                            for name in placement.PROVIDERS}
                    collect_provider_jobs(jobs)
            units = install_mounts()
            resume(saved)
        continuation = resume_finite()
        atomic_write_json(STATE/'result.json', {'state': 'verified_e_provider_placement',
            'providers': list(placement.PROVIDERS), 'mount_units': units, 'source_files_deleted': 0,
            'continuation_runs': continuation, 'observed_at_epoch': time.time()})


if __name__ == '__main__':
    main()
