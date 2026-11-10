import json
import os
import time
from types import SimpleNamespace

import pytest

from scripts import reclaim_unused_vscode_servers as gc


def server(root, digit, age_hours=100):
    path = root/('Stable-'+digit*40)
    (path/'server').mkdir(parents=True)
    (path/'server/binary').write_bytes(b'regenerable distribution')
    when = time.time()-age_hours*3600
    os.utime(path, (when, when))
    return path


def empty_refs(_root, names):
    return {name:[] for name in names}


def test_dry_run_and_apply_preserve_new_live_and_non_server_data(tmp_path):
    old, live, recent = server(tmp_path,'a'),server(tmp_path,'b'),server(tmp_path,'c',1)
    (tmp_path/'user-settings').write_text('must survive')
    def refs(_root, names):
        return {name:([123] if 'b'*40 in name else []) for name in names}
    dry = gc.reclaim(tmp_path, process_references=refs)
    assert [row['state'] for row in dry['results']] == ['eligible','in_use']
    assert old.exists() and live.exists() and recent.exists()
    result = gc.reclaim(tmp_path, apply=True, process_references=refs)
    assert [row['state'] for row in result['results']] == ['removed','in_use']
    assert not old.exists() and live.exists() and recent.exists()
    assert (tmp_path/'user-settings').read_text() == 'must survive'


def test_connection_at_rename_boundary_restores_original_name(tmp_path):
    old = server(tmp_path,'a')
    calls = 0
    def refs(_root, names):
        nonlocal calls
        calls += 1
        return {name:([321] if calls == 2 else []) for name in names}
    result = gc.reclaim(tmp_path, apply=True, process_references=refs)
    assert result['results'][0]['state'] == 'in_use_after_rename' and old.exists()


def test_interrupted_retirement_retries_only_the_journalled_inode(tmp_path, monkeypatch):
    old = server(tmp_path,'a')
    real = gc.shutil.rmtree
    monkeypatch.setattr(gc.shutil, 'rmtree', lambda *_a: (_ for _ in ()).throw(OSError('interrupted')))
    with pytest.raises(OSError, match='interrupted'):
        gc.reclaim(tmp_path, apply=True, process_references=empty_refs)
    tomb = tmp_path/('.stockagent-retired-'+'a'*40)
    marker = tmp_path/('.stockagent-retired-'+'a'*40+'.json')
    assert tomb.exists() and marker.exists() and not old.exists()
    changed = json.loads(marker.read_text()); changed['inode'] += 1; marker.write_text(json.dumps(changed))
    with pytest.raises(RuntimeError, match='another generation'):
        gc.reclaim(tmp_path, apply=True, process_references=empty_refs)
    changed['inode'] -= 1; marker.write_text(json.dumps(changed))
    monkeypatch.setattr(gc.shutil, 'rmtree', real)
    result = gc.reclaim(tmp_path, apply=True, process_references=empty_refs)
    assert result['results'][0]['state'] == 'removed' and not tomb.exists() and not marker.exists()


def test_uninspectable_process_and_symlink_candidate_are_not_removed(tmp_path):
    old = server(tmp_path,'a')
    alias = tmp_path/('Stable-'+'b'*40); alias.symlink_to(old, target_is_directory=True)
    def denied(_root, _names):
        raise PermissionError('cannot inspect')
    with pytest.raises(PermissionError):
        gc.reclaim(tmp_path, apply=True, process_references=denied)
    assert old.exists() and alias.is_symlink()


def test_extension_cache_only_removes_old_unreferenced_package_files(tmp_path):
    old = tmp_path/'openai.chatgpt-26.1021.22027-linux-x64'
    busy = tmp_path/'openai.codex-audio-26.1021.22027'
    recent = tmp_path/'github.vscode-pull-request-github-0.176.1'
    outside = tmp_path/'user-settings'
    for path in (old, busy, recent, outside):
        path.write_bytes(b'regenerable' if path != outside else b'preserve')
    when = time.time()-100*3600
    for path in (old, busy):
        os.utime(path, (when, when))
    def refs(_root, names):
        return {name:([123] if name == busy.name else []) for name in names}
    dry = gc.reclaim_extension_cache(tmp_path, process_references=refs)
    assert {row['state'] for row in dry['results']} == {'eligible', 'in_use'} and old.exists()
    result = gc.reclaim_extension_cache(tmp_path, apply=True, process_references=refs)
    assert {row['state'] for row in result['results']} == {'removed', 'in_use'}
    assert not old.exists() and busy.exists() and recent.exists() and outside.read_bytes() == b'preserve'


def test_extension_cache_skips_links_and_rejects_changed_generation(tmp_path):
    target = tmp_path/'outside'; target.write_bytes(b'keep')
    link = tmp_path/'openai.chatgpt-1.0'; link.symlink_to(target)
    hard = tmp_path/'openai.other-1.0'; os.link(target, hard)
    old = tmp_path/'openai.codex-1.0'; old.write_bytes(b'old')
    when = time.time()-100*3600
    os.utime(target, (when, when)); os.utime(old, (when, when))
    def changed(_root, names):
        old.write_bytes(b'new generation')
        return {name:[] for name in names}
    with pytest.raises(RuntimeError, match='changed before retirement'):
        gc.reclaim_extension_cache(tmp_path, apply=True, process_references=changed)
    assert link.is_symlink() and hard.exists() and target.read_bytes() == b'keep' and old.exists()


def test_pip_http_cache_removes_only_old_unreferenced_pairs(tmp_path):
    directory = tmp_path/'a'/'b'; directory.mkdir(parents=True)
    names = ['a'*56, 'b'*56, 'c'*56]
    for name in names:
        for suffix in ('', '.body'):
            (directory/(name+suffix)).write_bytes(b'cached download')
    old = time.time()-100*3600
    for path in directory.iterdir(): os.utime(path, (old, old))
    os.utime(directory/(names[1]+'.body'), None)  # Do not break a newer pair.
    def refs(_root, paths):
        return {path:([123] if names[2] in path else []) for path in paths}
    options = {'process_references':refs, 'running_pip':lambda:[]}
    dry = gc.reclaim_pip_http_cache(tmp_path, **options)
    assert dry['eligible_files'] == 2 and dry['removed_files'] == 0
    done = gc.reclaim_pip_http_cache(tmp_path, apply=True, **options)
    assert done['removed_files'] == 2 and not (directory/names[0]).exists()
    assert all((directory/(name+suffix)).exists() for name in names[1:] for suffix in ('', '.body'))


def test_pip_http_cache_stops_for_active_pip_and_skips_redirected_pair(tmp_path):
    key = 'd'*56
    outside = tmp_path/'outside'; outside.write_bytes(b'keep')
    original = tmp_path/key; original.write_bytes(b'cache')
    (tmp_path/(key+'.body')).symlink_to(outside)
    old = time.time()-100*3600; os.utime(original, (old, old))
    assert gc.reclaim_pip_http_cache(tmp_path, apply=True, running_pip=lambda:[1])['state'] == 'in_use'
    done = gc.reclaim_pip_http_cache(tmp_path, apply=True, running_pip=lambda:[], process_references=empty_refs)
    assert done['removed_files'] == 0 and original.exists() and outside.read_bytes() == b'keep'


def test_pressure_cleanup_is_bounded_and_never_changes_disk_reserve(tmp_path, monkeypatch):
    extension, pip = tmp_path/'extension', tmp_path/'pip'; extension.mkdir(); pip.mkdir()
    monkeypatch.setattr(gc, 'EXTENSION_CACHE_ROOT', extension)
    monkeypatch.setattr(gc, 'PIP_HTTP_CACHE_ROOT', pip)
    calls = []
    def first(root, *, apply):
        calls.append((root,apply)); return {'results':[{'state':'removed','allocated_bytes':5}]}
    def second(root, *, apply):
        calls.append((root,apply)); return {'state':'checked','removed_allocated_bytes':7}
    healthy = lambda _disk:SimpleNamespace(f_blocks=8000, f_frsize=1, f_bavail=200)
    assert gc.reclaim_under_pressure(probe=healthy, extension_cleanup=first, pip_cleanup=second)['state'] == 'not_needed'
    assert not calls
    observed = iter([SimpleNamespace(f_blocks=8000, f_frsize=1, f_bavail=80),
                     SimpleNamespace(f_blocks=8000, f_frsize=1, f_bavail=92)])
    result = gc.reclaim_under_pressure(probe=lambda _disk:next(observed), extension_cleanup=first, pip_cleanup=second)
    assert calls == [(extension,True),(pip,True)]
    assert result['threshold_bytes'] == 100 and result['removed_allocated_bytes'] == 12
    assert result['available_after_bytes'] == 92
    def failed(*_a, **_k):
        raise PermissionError('private diagnostic')
    result = gc.reclaim_under_pressure(probe=lambda _disk:SimpleNamespace(f_blocks=8000, f_frsize=1, f_bavail=80),
                                       extension_cleanup=failed, pip_cleanup=second)
    assert result['state'] == 'partial' and result['profile_issues'] == 1
    assert result['removed_allocated_bytes'] == 7 and 'private diagnostic' not in json.dumps(result)
