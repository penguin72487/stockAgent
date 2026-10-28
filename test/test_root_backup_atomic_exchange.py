"""Isolated tests for the exact, non-destructive root-backup relocation."""
import importlib.util
import errno
import os
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/relocate_root_backup_alias.py'
SPEC = importlib.util.spec_from_file_location('root_backup_exchange_under_test', SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def parents(tmp_path):
    # Reject production paths before the test constructs any candidate.
    assert tmp_path.is_relative_to(Path('/var/tmp'))
    assert not tmp_path.is_relative_to(Path('/root/stockAgent'))
    src, dst = tmp_path / 'old', tmp_path / 'new'
    src.mkdir()
    dst.mkdir()
    return src, dst


def test_exchange_preserves_old_name_inode_bytes_and_open_fd(tmp_path):
    old, new = parents(tmp_path)
    source, target = old / 'backup', new / 'backup'
    source.mkdir()
    (source / 'value').write_bytes(b'fixed backup')
    info = source.lstat()
    before = MODULE.inventory(source, with_sha=True)
    with (source / 'value').open('rb') as already_open:
        result = MODULE.exchange(source, target, source_parent=old, target_parent=new,
                                 expected_directory_id=(info.st_dev, info.st_ino))
        assert already_open.read() == b'fixed backup'
    assert source.is_symlink()
    assert os.readlink(source) == str(target)
    assert (source / 'value').read_bytes() == b'fixed backup'
    assert target.lstat().st_ino == info.st_ino == result['directory_inode']
    after = MODULE.inventory(target, with_sha=True)
    before['root_signature'][5] = after['root_signature'][5]
    assert before == after


def test_exchange_refuses_production_path_collision_and_changed_identity(tmp_path):
    old, new = parents(tmp_path)
    source, target = old / 'backup', new / 'backup'
    source.mkdir()
    info = source.lstat()
    with pytest.raises(ValueError, match='exact, distinct'):
        MODULE.exchange(Path('/root/stockAgent'), target, source_parent=old, target_parent=new,
                        expected_directory_id=(info.st_dev, info.st_ino))
    with pytest.raises(RuntimeError, match='identity changed'):
        MODULE.exchange(source, target, source_parent=old, target_parent=new,
                        expected_directory_id=(info.st_dev, info.st_ino + 1))
    target.mkdir()
    with pytest.raises(FileExistsError):
        MODULE.exchange(source, target, source_parent=old, target_parent=new,
                        expected_directory_id=(info.st_dev, info.st_ino))
    assert source.is_dir() and not source.is_symlink()
    assert target.is_dir() and not target.is_symlink()


def test_inventory_hashes_files_and_refuses_symlinks(tmp_path):
    old, _ = parents(tmp_path)
    source = old / 'backup'
    source.mkdir()
    (source / 'value').write_bytes(b'original')
    first = MODULE.inventory(source, with_sha=True)
    (source / 'value').write_bytes(b'different')
    second = MODULE.inventory(source, with_sha=True)
    assert first['rows'][0]['sha256'] != second['rows'][0]['sha256']
    (source / 'redirect').symlink_to('value')
    with pytest.raises(ValueError, match='contains a symlink'):
        MODULE.inventory(source, with_sha=True)


def test_failed_atomic_exchange_removes_only_its_placeholder(tmp_path, monkeypatch):
    old, new = parents(tmp_path)
    source, target = old / 'backup', new / 'backup'
    source.mkdir()
    (source / 'value').write_bytes(b'preserve original')
    sentinel = new / 'unrelated'
    sentinel.write_bytes(b'preserve unrelated')
    info = source.lstat()

    class FailedRename:
        def __call__(self, *args):
            MODULE.ctypes.set_errno(errno.EIO)
            return -1

    class FakeLibc:
        def __init__(self):
            self.renameat2 = FailedRename()

    monkeypatch.setattr(MODULE.ctypes, 'CDLL', lambda *args, **kwargs: FakeLibc())
    with pytest.raises(OSError, match='Input/output error'):
        MODULE.exchange(source, target, source_parent=old, target_parent=new,
                        expected_directory_id=(info.st_dev, info.st_ino))
    assert not source.is_symlink()
    assert (source / 'value').read_bytes() == b'preserve original'
    assert not target.exists() and not target.is_symlink()
    assert sentinel.read_bytes() == b'preserve unrelated'
