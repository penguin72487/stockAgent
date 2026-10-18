"""Atomic path probe never relaxes canonical immutable verification."""
import ctypes
import ast
import errno
import os
from pathlib import Path

import pytest

from scripts import benchmark_immutable_path_validation as bench
from stockagent.data_sync import immutable_replication as immutable


@pytest.fixture
def source():
    return Path(immutable.__file__).read_text()


def test_production_helpers_are_the_measured_prototype_not_a_parallel_variant():
    names = {'OpenHow': '_OpenHow', 'native_syscall': '_native_openat2_syscall',
             'OPENAT2': '_OPENAT2', 'checked_no_symlink': '_checked_no_symlink'}
    class Rename(ast.NodeTransformer):
        def visit_Name(self, node):
            node.id = names.get(node.id, node.id)
            return node
        def visit_Global(self, node):
            node.names = [names.get(name, name) for name in node.names]
            return node
        def visit_FunctionDef(self, node):
            node.name = names.get(node.name, node.name)
            if node.body and isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant):
                node.body.pop(0)  # Mechanism equality, not prose equality.
            return self.generic_visit(node)
        def visit_ClassDef(self, node):
            node.name = names.get(node.name, node.name)
            return self.generic_visit(node)
    original = ast.parse(Path(bench.__file__).read_text())
    adopted = ast.parse(Path(immutable.__file__).read_text())
    for original_name, adopted_name in names.items():
        if original_name == 'OPENAT2':
            continue
        left = next(node for node in original.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
                    and node.name == original_name)
        right = next(node for node in adopted.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
                     and node.name == adopted_name)
        assert ast.dump(Rename().visit(left), include_attributes=False) == ast.dump(
            Rename().visit(right), include_attributes=False)


def test_actual_production_guard_fallback_and_lexical_rejection(tmp_path, monkeypatch):
    observed = []
    def fallback(path):
        observed.append(path)
        return False
    monkeypatch.setattr(immutable, '_checked_no_symlink', fallback)
    assert immutable.safe(tmp_path, 'missing/member') == tmp_path/'missing/member'
    assert observed == [tmp_path/'missing/member']
    with pytest.raises(ValueError, match='unsafe immutable member path'):
        immutable.safe(tmp_path, '../outside')
    link = tmp_path/'redirect'
    link.symlink_to(tmp_path/'missing')
    with pytest.raises(ValueError, match='redirected'):
        immutable.safe(tmp_path, 'redirect/member')


def test_actual_production_guard_rejects_kernel_symlink_error(tmp_path, monkeypatch):
    def failed(*args):
        ctypes.set_errno(errno.ELOOP)
        return -1
    monkeypatch.setattr(immutable, '_OPENAT2', failed)
    with pytest.raises(ValueError, match='redirected'):
        immutable.safe(tmp_path, 'member')


@pytest.mark.parametrize('relative', ['', '../outside', '/absolute', 'a/../b',
                                      'a//b', './a', 'a\\b', 'a\nb', 'a\0b'])
def test_lexical_rejection_precedes_any_probe(tmp_path, source, relative):
    def forbidden(path):
        raise AssertionError('unsafe relative path must not reach kernel probe')
    selected = bench.candidate_safe(source, forbidden)
    with pytest.raises(ValueError, match='unsafe immutable member path'):
        selected(tmp_path, relative)


def test_successful_probe_does_not_repeat_parent_stats(tmp_path, source, monkeypatch):
    selected = bench.candidate_safe(source, lambda path: True)
    def forbidden(path):
        raise AssertionError('successful complete resolution must not repeat ancestor stats')
    monkeypatch.setattr(Path, 'is_symlink', forbidden)
    assert selected(tmp_path, 'member') == tmp_path/'member'


def test_missing_payload_keeps_full_old_ancestor_guard(tmp_path, source, monkeypatch):
    selected = bench.candidate_safe(source, lambda path: False)
    observed = []
    original = Path.is_symlink
    def counted(path):
        observed.append(path)
        return original(path)
    monkeypatch.setattr(Path, 'is_symlink', counted)
    path = tmp_path/'missing'/'member'
    assert selected(tmp_path, 'missing/member') == path
    assert observed == [path, *path.parents]


@pytest.mark.parametrize('kind', ['leaf', 'ancestor', 'dangling_leaf', 'dangling_ancestor'])
def test_real_kernel_rejects_all_symlink_positions(tmp_path, source, kind):
    if bench.OPENAT2 is None:
        pytest.skip('native openat2 ABI unavailable')
    target = tmp_path/'target'
    target.mkdir()
    (target/'member').write_text('original')
    link = tmp_path/'redirect'
    link.symlink_to(target if 'dangling' not in kind else tmp_path/'missing')
    relative = 'redirect/member' if 'ancestor' in kind else 'redirect'
    selected = bench.candidate_safe(source)
    with pytest.raises(ValueError, match='redirected'):
        selected(tmp_path, relative)


def test_real_kernel_success_and_missing_path(tmp_path):
    if bench.OPENAT2 is None:
        pytest.skip('native openat2 ABI unavailable')
    path = tmp_path/'file'
    path.write_text('原始值')
    assert bench.checked_no_symlink(path) is True
    assert bench.checked_no_symlink(tmp_path/'missing') is False


def test_path_only_resolution_does_not_block_on_fifo(tmp_path):
    if bench.OPENAT2 is None or not hasattr(os, 'mkfifo'):
        pytest.skip('native Linux FIFO probe unavailable')
    fifo = tmp_path/'fifo'
    os.mkfifo(fifo)
    assert bench.checked_no_symlink(fifo) is True
    with pytest.raises(ValueError, match='not a regular file'):
        immutable.digest(fifo)


def test_exact_flags_and_fd_closure(tmp_path, monkeypatch):
    closed = []
    def syscall(number, dfd, path, how, size):
        assert number.value == 437 and dfd.value == -100
        assert path.value == os.fsencode(tmp_path)
        assert how._obj.flags == os.O_PATH | os.O_CLOEXEC
        assert how._obj.flags & os.O_NOFOLLOW == 0
        assert how._obj.mode == 0 and how._obj.resolve == 4
        assert size.value == 24
        return 197
    monkeypatch.setattr(bench, 'OPENAT2', syscall)
    monkeypatch.setattr(bench.os, 'close', closed.append)
    assert bench.checked_no_symlink(tmp_path) is True
    assert closed == [197]


@pytest.mark.parametrize('module,check_name,capability_name,how_name', [
    (bench, 'checked_no_symlink', 'OPENAT2', 'OpenHow'),
    (immutable, '_checked_no_symlink', '_OPENAT2', '_OpenHow'),
])
def test_inflight_probe_survives_another_threads_capability_fallback(
        tmp_path, monkeypatch, module, check_name, capability_name, how_name):
    closed, calls = [], []
    original_how = getattr(module, how_name)
    def disable_before_dispatch(*args):
        # Deterministic interleaving: another call receives ENOSYS/EPERM
        # between this call's capability check and its kernel dispatch.
        setattr(module, capability_name, None)
        return original_how(*args)
    def syscall(*args):
        calls.append(args)
        return 197
    monkeypatch.setattr(module, capability_name, syscall)
    monkeypatch.setattr(module, how_name, disable_before_dispatch)
    monkeypatch.setattr(module.os, 'close', closed.append)
    check = getattr(module, check_name)
    assert check(tmp_path) is True
    assert len(calls) == 1 and closed == [197]
    assert check(tmp_path) is False  # Later calls retain the full old guard.


@pytest.mark.parametrize('error', [errno.ENOSYS, errno.EINVAL, errno.EPERM,
                                  errno.ENOENT, errno.ENOTDIR, errno.EACCES, errno.EIO])
def test_every_non_symlink_error_falls_back_without_accepting(tmp_path, monkeypatch, error):
    def failed(*args):
        ctypes.set_errno(error)
        return -1
    monkeypatch.setattr(bench, 'OPENAT2', failed)
    assert bench.checked_no_symlink(tmp_path) is False
    assert (bench.OPENAT2 is None) == (error in {errno.ENOSYS, errno.EINVAL, errno.EPERM})


def test_syscall_symlink_error_is_never_a_success(tmp_path, monkeypatch):
    def failed(*args):
        ctypes.set_errno(errno.ELOOP)
        return -1
    monkeypatch.setattr(bench, 'OPENAT2', failed)
    with pytest.raises(ValueError, match='redirected'):
        bench.checked_no_symlink(tmp_path)


@pytest.mark.parametrize('system,machine', [('Darwin','x86_64'), ('Linux','alpha'), ('Linux','mips64')])
def test_unknown_syscall_abi_uses_original_guard(monkeypatch, system, machine):
    monkeypatch.setattr(bench.platform, 'system', lambda: system)
    monkeypatch.setattr(bench.platform, 'machine', lambda: machine)
    assert bench.native_syscall() is None


@pytest.mark.parametrize('damage', ['none', 'payload', 'extra', 'missing_ready', 'symlink'])
def test_complete_original_verification_and_rejection_unchanged(tmp_path, source, monkeypatch, damage):
    release = tmp_path/'release'
    release.mkdir()
    payload = release/'member'
    payload.write_text('原值 -3.125')
    expected = immutable.seal(release, {'purpose': 'test-only'})
    selected = bench.candidate_safe(source)
    if damage == 'payload':
        payload.write_text('different')
    elif damage == 'extra':
        (release/'unexpected').write_bytes(b'new')
    elif damage == 'missing_ready':
        (release/'READY').unlink()
    elif damage == 'symlink':
        other = tmp_path/'other'
        payload.rename(other)
        payload.symlink_to(other)
    monkeypatch.setattr(immutable, 'safe', selected)
    if damage == 'none':
        assert immutable.verify(release) == expected
    else:
        with pytest.raises((ValueError, FileNotFoundError)):
            immutable.verify(release)


def test_metadata_only_manifest_does_not_require_payload(tmp_path, source, monkeypatch):
    (tmp_path/'member').write_bytes(b'original')
    expected = immutable.seal(tmp_path, {'purpose': 'metadata-only'})
    (tmp_path/'member').unlink()
    monkeypatch.setattr(immutable, 'safe', bench.candidate_safe(source))
    assert immutable.read_manifest(tmp_path) == expected
