"""Reject invalid ownership/resources before importing or rebuilding any data."""
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import verify_tw_public_remote_derivation as module


def arguments(tmp_path):
    code = tmp_path/'code'
    source = tmp_path/'tw-public/engineering-snapshot'
    code.mkdir(); source.mkdir(parents=True)
    return Namespace(code_root=code, source_root=source, output=tmp_path/'new-output',
                     code_receipt=tmp_path/'not-read.json', packed_root=tmp_path/'packed',
                     snapshot_id=source.name, end_date='2020-01-01', build_count=1,
                     polars_threads=2, memory_budget_gib=64)


def test_source_host_cannot_be_used_as_training_derivation_node(tmp_path, monkeypatch):
    args = arguments(tmp_path)
    monkeypatch.setattr(module.socket, 'gethostname', lambda: 'penguin')
    with pytest.raises(ValueError, match='remote research node'):
        module.verify(args)
    assert not args.output.exists()


def test_output_cannot_replace_the_fixed_source_tree(tmp_path, monkeypatch):
    args = arguments(tmp_path)
    args.output = args.source_root/'features/new-derived.parquet'
    monkeypatch.setattr(module.socket, 'gethostname', lambda: 'engineering-remote')
    with pytest.raises(ValueError, match='outside the immutable source'):
        module.verify(args)
    assert not args.output.exists()


@pytest.mark.parametrize('memory_gib,scratch_gib,affinity', [(63, 20, 8), (128, 9, 8), (128, 20, 1)])
def test_declared_resources_are_required_before_source_read_or_output_creation(tmp_path, monkeypatch,
                                                                            memory_gib, scratch_gib, affinity):
    args = arguments(tmp_path)
    monkeypatch.setattr(module.socket, 'gethostname', lambda: 'engineering-remote')
    original = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda path, *a, **kw:
                        f'MemAvailable: {memory_gib*1024**2} kB\nMemTotal: {memory_gib*1024**2} kB\n' if str(path)=='/proc/meminfo'
                        else original(path, *a, **kw))
    monkeypatch.setattr(module.shutil, 'disk_usage', lambda _: SimpleNamespace(free=scratch_gib*1024**3))
    monkeypatch.setattr(module.os, 'sched_getaffinity', lambda _: set(range(affinity)))
    with pytest.raises(ValueError, match='declared available RAM|CPU affinity'):
        module.verify(args)
    assert not args.output.exists()
    assert not args.code_receipt.exists()
